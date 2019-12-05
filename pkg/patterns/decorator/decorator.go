package decorator

import (
	"context"
	"fmt"

	"admiralty.io/multicluster-controller/pkg/cluster"
	"admiralty.io/multicluster-controller/pkg/controller"
	"admiralty.io/multicluster-controller/pkg/patterns"
	"admiralty.io/multicluster-controller/pkg/reconcile"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func NewController(c *cluster.Cluster, prototype runtime.Object, a Applier, o controller.WatchOptions) (*controller.Controller, error) {
	client, err := c.GetDelegatingClient()
	if err != nil {
		return nil, fmt.Errorf("getting delegating client: %v", err)
	}

	gvks, _, err := c.GetScheme().ObjectKinds(prototype)
	if err != nil {
		return nil, fmt.Errorf("getting GVKs for prototype: %v", err)
	}
	if len(gvks) != 1 {
		return nil, fmt.Errorf("scheme has %d GVK(s) for prototype when 1 is expected")
	}
	gvk := gvks[0]

	r := &reconciler{
		client:    client,
		prototype: prototype,
		gvk:       gvk,
		applier:   a,
	}

	co := controller.New(r, controller.Options{})

	if err := co.WatchResourceReconcileObject(c, prototype, o); err != nil {
		return nil, fmt.Errorf("setting up proxy pod observation watch: %v", err)
	}

	return co, nil
}

type reconciler struct {
	client    client.Client
	prototype runtime.Object
	gvk       schema.GroupVersionKind
	applier   Applier
}

func (r *reconciler) Reconcile(req reconcile.Request) (reconcile.Result, error) {
	obj := r.prototype.DeepCopyObject()
	if err := r.client.Get(context.Background(), req.NamespacedName, obj); err != nil {
		if !errors.IsNotFound(err) {
			return reconcile.Result{}, fmt.Errorf("cannot get %s: %v",
				r.objectErrorString(req.Name, req.Namespace), err)
		}
		return reconcile.Result{}, nil
	}

	needUpdate, err := r.applier.NeedUpdate(obj)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("cannot determine whether %s needs update: %v",
			r.objectErrorString(req.Name, req.Namespace), err)
	}

	if !needUpdate {
		return reconcile.Result{}, nil
	}

	if err := r.applier.Mutate(obj); err != nil {
		return reconcile.Result{}, fmt.Errorf("cannot mutate %s: %v",
			r.objectErrorString(req.Name, req.Namespace), err)
	}

	if err := r.client.Update(context.Background(), obj); err != nil && !patterns.IsOptimisticLockError(err) {
		return reconcile.Result{}, fmt.Errorf("cannot update %s: %v",
			r.objectErrorString(req.Name, req.Namespace), err)
	}

	return reconcile.Result{}, nil
}

func (r *reconciler) objectErrorString(name string, namespace string) string {
	return fmt.Sprintf("%s %s in namespace %s", r.gvk.Kind, name, namespace)
}

type Applier interface {
	NeedUpdate(obj interface{}) (bool, error)
	Mutate(obj interface{}) error
}
