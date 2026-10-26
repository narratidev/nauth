package configsource

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// Kind pairs a resource kind with the reconciler that manages it.
type Kind struct {
	Object     client.Object
	Reconciler reconcile.Reconciler
}

// Runner reconciles the resources in Dir. Kinds are reconciled in the order
// given, so a kind should come after the kinds it depends on.
type Runner struct {
	Dir       string
	Namespace string
	Scheme    *runtime.Scheme
	// Mem holds the resources; only the Runner creates and deletes them.
	Mem   client.Client
	Kinds []Kind
	// Poll is how often Dir is checked for changes.
	Poll time.Duration
	// Resync is how often a resource is reconciled when its reconciler asks
	// for nothing sooner.
	Resync time.Duration
	// Retry is the wait after a failed reconcile.
	Retry time.Duration
	Log   logr.Logger

	gvks  []schema.GroupVersionKind
	due   map[string]time.Time
	hash  string
	ready atomic.Bool
}

// Run loads and reconciles until ctx ends.
func (r *Runner) Run(ctx context.Context) error {
	r.due = map[string]time.Time{}
	for _, k := range r.Kinds {
		gvk, err := apiutil.GVKForObject(k.Object, r.Scheme)
		if err != nil {
			return err
		}
		r.gvks = append(r.gvks, gvk)
	}
	ticker := time.NewTicker(r.Poll)
	defer ticker.Stop()
	for {
		r.tick(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// Ready reports whether the directory has been loaded once.
func (r *Runner) Ready(_ *http.Request) error {
	if !r.ready.Load() {
		return fmt.Errorf("configuration not loaded yet")
	}
	return nil
}

func (r *Runner) tick(ctx context.Context) {
	snap, err := Load(r.Dir, r.Namespace, r.Scheme, r.gvks)
	if err != nil {
		// Keep what was loaded last: a broken file must not delete anything.
		r.Log.Info("configuration not loaded; keeping the previous one", "error", err.Error())
	} else if snap.Hash != r.hash {
		if err := r.sync(ctx, snap); err != nil {
			r.Log.Error(err, "configuration not applied; retrying")
		} else {
			r.Log.Info("configuration loaded", "objects", len(snap.Objects), "skippedOtherKinds", snap.Skipped)
			r.hash = snap.Hash
			r.ready.Store(true)
			// Without watches, a change to one resource may matter to any
			// other (a User follows its Account), so all of them run now.
			for key := range r.due {
				r.due[key] = time.Time{}
			}
		}
	}
	r.reconcileDue(ctx)
	if r.ready.Load() {
		r.collectSecrets(ctx)
	}
}

// collectSecrets deletes the Secrets whose owner is gone, as the cluster's
// garbage collector would if the owner references were real (see
// OwnerRefsAnnotation). Only owners of the kinds this Runner manages count,
// so nack never touches what nauth wrote.
func (r *Runner) collectSecrets(ctx context.Context) {
	managed := map[schema.GroupKind]bool{}
	for _, gvk := range r.gvks {
		managed[gvk.GroupKind()] = true
	}
	live := map[types.UID]bool{}
	for _, gvk := range r.gvks {
		objs, err := r.list(ctx, gvk)
		if err != nil {
			r.Log.Error(err, "list", "kind", gvk.Kind)
			return
		}
		for _, obj := range objs {
			live[obj.GetUID()] = true
		}
	}
	secrets := &corev1.SecretList{}
	if err := r.Mem.List(ctx, secrets, client.InNamespace(r.Namespace)); err != nil {
		r.Log.Error(err, "list secrets")
		return
	}
	for i := range secrets.Items {
		secret := &secrets.Items[i]
		owner := metav1.GetControllerOf(secret)
		if owner == nil || live[owner.UID] {
			continue
		}
		gv, err := schema.ParseGroupVersion(owner.APIVersion)
		if err != nil || !managed[gv.WithKind(owner.Kind).GroupKind()] {
			continue
		}
		if err := r.Mem.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
			r.Log.Error(err, "delete secret of removed owner", "secret", secret.Name)
			continue
		}
		r.Log.Info("secret of removed owner deleted", "secret", secret.Name, "owner", owner.Kind+"/"+owner.Name)
	}
}

// sync makes Mem match the snapshot: it creates new resources, updates the
// spec, labels and annotations of changed ones, and deletes the ones that are
// gone. A deleted resource with finalizers stays until its reconciler has
// cleaned up, as in the cluster.
func (r *Runner) sync(ctx context.Context, snap *Snapshot) error {
	desired := map[string]client.Object{}
	for _, obj := range snap.Objects {
		gvk, err := apiutil.GVKForObject(obj, r.Scheme)
		if err != nil {
			return err
		}
		desired[objectKey(gvk, obj.GetName())] = obj
	}

	existing := map[string]client.Object{}
	for _, gvk := range r.gvks {
		list, err := r.list(ctx, gvk)
		if err != nil {
			return err
		}
		for _, obj := range list {
			existing[objectKey(gvk, obj.GetName())] = obj
		}
	}
	if len(snap.Objects) == 0 && len(existing) > 0 {
		return fmt.Errorf("the configuration is empty but %d resources are loaded; refusing to delete them all", len(existing))
	}

	for key, want := range desired {
		have, ok := existing[key]
		if !ok {
			want.SetGeneration(1)
			want.SetCreationTimestamp(metav1.Now())
			if err := r.Mem.Create(ctx, want); err != nil {
				return fmt.Errorf("create %s: %w", key, err)
			}
			r.Log.Info("resource added", "resource", key)
			r.due[key] = time.Time{}
			continue
		}
		changed, err := r.merge(have, want)
		if err != nil {
			return fmt.Errorf("update %s: %w", key, err)
		}
		if changed {
			if err := r.Mem.Update(ctx, have); err != nil {
				return fmt.Errorf("update %s: %w", key, err)
			}
			r.Log.Info("resource changed", "resource", key, "generation", have.GetGeneration())
		}
	}

	for key, have := range existing {
		if _, ok := desired[key]; ok || have.GetDeletionTimestamp() != nil {
			continue
		}
		if err := r.Mem.Delete(ctx, have); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete %s: %w", key, err)
		}
		r.Log.Info("resource removed", "resource", key)
		r.due[key] = time.Time{}
	}
	return nil
}

// merge copies the spec, labels and annotations of want into have. Labels and
// annotations are merged, not replaced: the controllers keep state in them.
// The generation goes up when the spec changes, as the API server does.
func (r *Runner) merge(have, want client.Object) (bool, error) {
	haveU, err := runtime.DefaultUnstructuredConverter.ToUnstructured(have)
	if err != nil {
		return false, err
	}
	wantU, err := runtime.DefaultUnstructuredConverter.ToUnstructured(want)
	if err != nil {
		return false, err
	}
	specChanged := !reflect.DeepEqual(haveU["spec"], wantU["spec"])
	if specChanged {
		if spec, ok := wantU["spec"]; ok {
			haveU["spec"] = spec
		} else {
			delete(haveU, "spec")
		}
	}
	changed := specChanged
	u := &unstructured.Unstructured{Object: haveU}
	u.SetLabels(mergeMaps(u.GetLabels(), want.GetLabels(), &changed))
	u.SetAnnotations(mergeMaps(u.GetAnnotations(), want.GetAnnotations(), &changed))
	if !changed {
		return false, nil
	}
	if specChanged {
		u.SetGeneration(have.GetGeneration() + 1)
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, have); err != nil {
		return false, err
	}
	return true, nil
}

func mergeMaps(have, want map[string]string, changed *bool) map[string]string {
	if len(want) == 0 {
		return have
	}
	if have == nil {
		have = map[string]string{}
	}
	for k, v := range want {
		if have[k] != v {
			have[k] = v
			*changed = true
		}
	}
	return have
}

func (r *Runner) list(ctx context.Context, gvk schema.GroupVersionKind) ([]client.Object, error) {
	listGVK := gvk.GroupVersion().WithKind(gvk.Kind + "List")
	raw, err := r.Scheme.New(listGVK)
	if err != nil {
		return nil, err
	}
	list := raw.(client.ObjectList)
	if err := r.Mem.List(ctx, list, client.InNamespace(r.Namespace)); err != nil {
		return nil, err
	}
	items, err := meta.ExtractList(list)
	if err != nil {
		return nil, err
	}
	out := make([]client.Object, 0, len(items))
	for _, item := range items {
		out = append(out, item.(client.Object))
	}
	return out, nil
}

// maxPasses bounds how often one resource is reconciled in a row.
const maxPasses = 10

func (r *Runner) reconcileDue(ctx context.Context) {
	now := time.Now()
	// A change to a resource matters to the kinds after it (a User waits for
	// its Account), so they run in the same tick, as their watches would.
	upstreamChanged := false
	for i, k := range r.Kinds {
		gvk := r.gvks[i]
		objs, err := r.list(ctx, gvk)
		if err != nil {
			r.Log.Error(err, "list", "kind", gvk.Kind)
			continue
		}
		live := map[string]bool{}
		kindChanged := false
		for _, obj := range objs {
			key := objectKey(gvk, obj.GetName())
			live[key] = true
			if due, ok := r.due[key]; ok && due.After(now) && !upstreamChanged {
				continue
			}
			next, changed := r.reconcileOne(ctx, k, gvk, obj, key)
			r.due[key] = next
			kindChanged = kindChanged || changed
		}
		upstreamChanged = upstreamChanged || kindChanged
		for key := range r.due {
			if !live[key] && hasKindPrefix(key, gvk) {
				delete(r.due, key)
			}
		}
	}
}

// reconcileOne reconciles a resource until it stops changing it, as the
// watch on its own kind would, and returns when it is due again.
func (r *Runner) reconcileOne(ctx context.Context, k Kind, gvk schema.GroupVersionKind, obj client.Object, key string) (time.Time, bool) {
	log := r.Log.WithValues("resource", key)
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}}
	version := obj.GetResourceVersion()
	changed := false
	for pass := 1; ; pass++ {
		res, err := k.Reconciler.Reconcile(logr.NewContext(ctx, log), req)
		now := time.Now()
		if err != nil {
			// Usually a dependency that is not ready yet; it is retried.
			log.Info("reconcile failed; retrying", "error", err.Error(), "retryIn", r.Retry.String())
			return now.Add(r.Retry), changed
		}
		current, ok := r.get(ctx, gvk, req.NamespacedName)
		if !ok {
			return now, true // deleted once its finalizers were done
		}
		selfChanged := current.GetResourceVersion() != version
		version = current.GetResourceVersion()
		changed = changed || selfChanged
		requeueNow := res.Requeue && res.RequeueAfter == 0 //nolint:staticcheck // reconcilers still return it
		if (selfChanged || requeueNow) && pass < maxPasses {
			continue
		}
		switch {
		case res.RequeueAfter > 0:
			return now.Add(min(res.RequeueAfter, r.Resync)), changed
		case requeueNow:
			return now.Add(r.Retry), changed
		default:
			return now.Add(r.Resync), changed
		}
	}
}

func (r *Runner) get(ctx context.Context, gvk schema.GroupVersionKind, key types.NamespacedName) (client.Object, bool) {
	raw, err := r.Scheme.New(gvk)
	if err != nil {
		return nil, false
	}
	obj := raw.(client.Object)
	if err := r.Mem.Get(ctx, key, obj); err != nil {
		return nil, false
	}
	return obj, true
}

func hasKindPrefix(key string, gvk schema.GroupVersionKind) bool {
	prefix := gvk.GroupKind().String() + "/"
	return len(key) > len(prefix) && key[:len(prefix)] == prefix
}
