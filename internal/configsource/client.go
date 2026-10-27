// Package configsource runs controller-runtime reconcilers against resources
// read from files (a mounted ConfigMap) instead of the Kubernetes API.
//
// The resources live in an in-memory client. Secrets are the exception: they
// go to the cluster, in the controller's own namespace, because that is where
// workloads read their credentials and where keys survive a restart.
//
// The code is shared verbatim between the nauth and nack forks.
package configsource

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// OwnerRefsAnnotation keeps a Secret's owner references while it is stored in
// the cluster. The owners only exist in memory, so real owner references would
// point the cluster's garbage collector at kinds it does not know.
const OwnerRefsAnnotation = "configsource.vidd.no/owner-references"

// splitClient keeps every object in memory, except Secrets.
type splitClient struct {
	client.Client
	cluster client.Client
}

// NewSplitClient returns a client that sends Secrets to cluster and everything
// else to mem.
func NewSplitClient(mem client.Client, cluster client.Client) client.Client {
	return &splitClient{Client: mem, cluster: cluster}
}

func isSecret(obj runtime.Object) bool {
	switch obj.(type) {
	case *corev1.Secret, *corev1.SecretList:
		return true
	}
	return false
}

func (c *splitClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if !isSecret(obj) {
		return c.Client.Get(ctx, key, obj, opts...)
	}
	if err := c.cluster.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	return restoreOwnerRefs(obj)
}

func (c *splitClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if !isSecret(list) {
		return c.Client.List(ctx, list, opts...)
	}
	if err := c.cluster.List(ctx, list, opts...); err != nil {
		return err
	}
	secrets := list.(*corev1.SecretList)
	for i := range secrets.Items {
		if err := restoreOwnerRefs(&secrets.Items[i]); err != nil {
			return err
		}
	}
	return nil
}

func (c *splitClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if !isSecret(obj) {
		return c.Client.Create(ctx, obj, opts...)
	}
	return c.write(obj, func() error { return c.cluster.Create(ctx, obj, opts...) })
}

func (c *splitClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if !isSecret(obj) {
		return c.Client.Update(ctx, obj, opts...)
	}
	return c.write(obj, func() error { return c.cluster.Update(ctx, obj, opts...) })
}

func (c *splitClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if !isSecret(obj) {
		return c.Client.Patch(ctx, obj, patch, opts...)
	}
	return c.write(obj, func() error { return c.cluster.Patch(ctx, obj, patch, opts...) })
}

func (c *splitClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if !isSecret(obj) {
		return c.Client.Delete(ctx, obj, opts...)
	}
	return c.cluster.Delete(ctx, obj, opts...)
}

func (c *splitClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	if !isSecret(obj) {
		return c.Client.DeleteAllOf(ctx, obj, opts...)
	}
	return c.cluster.DeleteAllOf(ctx, obj, opts...)
}

// write moves the owner references into the annotation for the call and puts
// them back afterwards, so the caller sees the object it wrote.
func (c *splitClient) write(obj client.Object, call func() error) error {
	if err := stashOwnerRefs(obj); err != nil {
		return err
	}
	if err := call(); err != nil {
		return err
	}
	return restoreOwnerRefs(obj)
}

func stashOwnerRefs(obj client.Object) error {
	refs := obj.GetOwnerReferences()
	if len(refs) == 0 {
		return nil
	}
	raw, err := json.Marshal(refs)
	if err != nil {
		return fmt.Errorf("encode owner references: %w", err)
	}
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[OwnerRefsAnnotation] = string(raw)
	obj.SetAnnotations(annotations)
	obj.SetOwnerReferences(nil)
	return nil
}

func restoreOwnerRefs(obj client.Object) error {
	raw, ok := obj.GetAnnotations()[OwnerRefsAnnotation]
	if !ok {
		return nil
	}
	var refs []metav1.OwnerReference
	if err := json.Unmarshal([]byte(raw), &refs); err != nil {
		return fmt.Errorf("decode owner references of %s: %w", obj.GetName(), err)
	}
	obj.SetOwnerReferences(refs)
	return nil
}
