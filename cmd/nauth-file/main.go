// nauth-file runs nauth's reconcilers without nauth's CRDs: NatsClusters,
// Accounts, Users and the rest are read from a directory (a mounted
// ConfigMap) and kept in memory. Only Secrets go to the Kubernetes API, in the
// controller's own namespace: the account keys nauth creates and the user
// creds it writes. A namespaced Role on Secrets is all it needs.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/WirelessCar/nauth/api/v1alpha1"
	"github.com/WirelessCar/nauth/internal/adapter/inbound/controller"
	"github.com/WirelessCar/nauth/internal/adapter/outbound/k8s"
	"github.com/WirelessCar/nauth/internal/adapter/outbound/nats"
	"github.com/WirelessCar/nauth/internal/configsource"
	"github.com/WirelessCar/nauth/internal/core"
	"github.com/WirelessCar/nauth/internal/domain"
	"github.com/WirelessCar/nauth/internal/domain/nauth"
)

func main() {
	var dir, namespace, clusterName, probeAddr string
	var poll, resync, retry, accountInterval time.Duration
	flag.StringVar(&dir, "config-dir", "/etc/nats-config", "Directory with the resources (a mounted ConfigMap).")
	flag.StringVar(&namespace, "namespace", os.Getenv("POD_NAMESPACE"), "The namespace: resources land in it and Secrets are written to it.")
	flag.StringVar(&clusterName, "nats-cluster", "", "The NatsCluster that Accounts without a natsClusterRef belong to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.DurationVar(&poll, "poll-interval", 10*time.Second, "How often the directory is checked for changes.")
	flag.DurationVar(&resync, "resync-interval", 5*time.Minute, "How often every resource is reconciled.")
	flag.DurationVar(&retry, "retry-interval", 15*time.Second, "The wait after a failed reconcile.")
	flag.DurationVar(&accountInterval, "account-reconciliation-interval", 5*time.Minute,
		"How long successful NATS Account state validation remains fresh.")
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	logger := zap.New(zap.UseFlagOptions(&opts))
	ctrl.SetLogger(logger)
	log := logger.WithName("nauth-file")
	if err := run(log, dir, namespace, clusterName, probeAddr, poll, resync, retry, accountInterval); err != nil {
		log.Error(err, "stopped")
		os.Exit(1)
	}
}

func run(log logr.Logger, dir, namespace, clusterName, probeAddr string,
	poll, resync, retry, accountInterval time.Duration) error {
	if namespace == "" {
		return fmt.Errorf("--namespace (or POD_NAMESPACE) is required")
	}
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))

	kinds := []client.Object{
		&v1alpha1.NatsCluster{},
		&v1alpha1.AccountSigningKey{},
		&v1alpha1.Account{},
		&v1alpha1.AccountExport{},
		&v1alpha1.AccountImport{},
		&v1alpha1.User{},
	}
	memBuilder := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(kinds...)
	for _, idx := range controller.FieldIndexes() {
		memBuilder = memBuilder.WithIndex(idx.Object, idx.Field, idx.Extract)
	}
	cluster, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("kubernetes client: %w", err)
	}
	// Every Secret nauth touches is in this namespace; refuse the rest.
	c := configsource.NewSplitClient(memBuilder.Build(), client.NewNamespacedClient(cluster, namespace))

	var operatorCluster *core.OperatorNatsCluster
	if clusterName != "" {
		operatorCluster, err = core.NewOperatorNatsCluster(nauth.ClusterRef(namespace+"/"+clusterName), false)
		if err != nil {
			return err
		}
	}
	config, err := core.NewConfig(operatorCluster, domain.Namespace(namespace))
	if err != nil {
		return err
	}

	secretClient := k8s.NewSecretClient(c)
	accountClient := k8s.NewAccountClient(c)
	clusterClient := k8s.NewClusterClient(c, secretClient, k8s.NewConfigMapClient(c))
	natsSysClient := nats.NewSysClient()
	natsAccClient := nats.NewAccountClient()

	clusterManager, err := core.NewClusterManager(clusterClient, natsSysClient, config)
	if err != nil {
		return err
	}
	accountManager, err := core.NewAccountManager(natsSysClient, natsAccClient, accountClient, secretClient, accountInterval)
	if err != nil {
		return err
	}
	recorder := logRecorder{log: log.WithName("event")}

	runner := &configsource.Runner{
		Dir:       dir,
		Namespace: namespace,
		Scheme:    scheme,
		Mem:       c,
		Poll:      poll,
		Resync:    resync,
		Retry:     retry,
		Log:       log,
		Kinds: []configsource.Kind{
			{Object: &v1alpha1.NatsCluster{}, Reconciler: controller.NewNatsClusterReconciler(c, scheme, clusterManager, clusterClient, recorder)},
			{Object: &v1alpha1.AccountSigningKey{}, Reconciler: controller.NewAccountSigningKeyReconciler(c, scheme, core.NewAccountSigningKeyManager(secretClient))},
			{Object: &v1alpha1.Account{}, Reconciler: controller.NewAccountReconciler(c, scheme, accountManager, clusterManager, accountClient, recorder, accountInterval, false)},
			{Object: &v1alpha1.AccountExport{}, Reconciler: controller.NewAccountExportReconciler(c, scheme, core.NewAccountExportManager())},
			{Object: &v1alpha1.AccountImport{}, Reconciler: controller.NewAccountImportReconciler(c, scheme, core.NewAccountImportManager())},
			{Object: &v1alpha1.User{}, Reconciler: controller.NewUserReconciler(c, scheme, core.NewUserManager(accountManager, secretClient), recorder)},
		},
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go serveProbes(log, probeAddr, runner)
	log.Info("starting", "configDir", dir, "namespace", namespace, "natsCluster", clusterName)
	return runner.Run(ctx)
}

func serveProbes(log logr.Logger, addr string, runner *configsource.Runner) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := runner.Ready(r); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Error(err, "probe endpoint stopped")
	}
}

// logRecorder writes events to the log: there is no API object to attach them to.
type logRecorder struct{ log logr.Logger }

var _ events.EventRecorder = logRecorder{}

func (r logRecorder) Eventf(regarding runtime.Object, _ runtime.Object, eventtype, reason, _ string, note string, args ...interface{}) {
	name := ""
	if obj, ok := regarding.(client.Object); ok {
		name = obj.GetName()
	}
	msg := fmt.Sprintf(note, args...)
	if strings.EqualFold(eventtype, "Warning") {
		r.log.Info("warning", "object", name, "reason", reason, "message", msg)
		return
	}
	r.log.V(1).Info(msg, "object", name, "reason", reason)
}
