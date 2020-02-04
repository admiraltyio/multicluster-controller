package gc

import (
	"context"
	"fmt"

	"admiralty.io/multicluster-controller/pkg/cluster"
	"admiralty.io/multicluster-controller/pkg/controller"
	"admiralty.io/multicluster-controller/pkg/patterns"
	"admiralty.io/multicluster-controller/pkg/reconcile"
	"admiralty.io/multicluster-controller/pkg/reference"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	LabelParentUID = "multicluster.admiralty.io/parent-uid"
)

func NewController(parentClusters []*cluster.Cluster, childClusters []*cluster.Cluster, o Options) (*controller.Controller, error) {
	r := &reconciler{Options: o}

	parentGVKs, _, err := parentClusters[0].GetScheme().ObjectKinds(r.ParentPrototype)
	if err != nil {
		return nil, fmt.Errorf("getting GVKs for parent prototype: %v", err)
	}
	if len(parentGVKs) != 1 {
		return nil, fmt.Errorf("parent cluster scheme has %d GVK(s) for parent prototype when 1 is expected")
	}
	r.parentGVK = parentGVKs[0]

	childGVKs, _, err := childClusters[0].GetScheme().ObjectKinds(r.ChildPrototype)
	if err != nil {
		return nil, fmt.Errorf("getting GVKs for child prototype: %v", err)
	}
	if len(childGVKs) != 1 {
		return nil, fmt.Errorf("child cluster scheme has %d GVK(s) for child prototype when 1 is expected")
	}
	r.childGVK = childGVKs[0]

	co := controller.New(r, controller.Options{})

	r.parentClients = make(map[string]client.Client, len(parentClusters))
	for _, clu := range parentClusters {
		cli, err := clu.GetDelegatingClient()
		if err != nil {
			return nil, fmt.Errorf("getting delegating client for parent cluster: %v", err)
		}
		r.parentClients[clu.Name] = cli

		if err := co.WatchResourceReconcileObject(clu, r.ParentPrototype, r.ParentWatchOptions); err != nil {
			return nil, fmt.Errorf("setting up watch for %s: %v", r.parentResourceErrorString(clu.Name), err)
		}
	}

	r.childClients = make(map[string]client.Client, len(childClusters))
	for _, clu := range childClusters {
		cli, err := clu.GetDelegatingClient()
		if err != nil {
			return nil, fmt.Errorf("getting delegating client for child cluster: %v", err)
		}
		r.childClients[clu.Name] = cli

		if err := co.WatchResourceReconcileController(clu, r.ChildPrototype, controller.WatchOptions{Namespace: r.ChildNamespace}); err != nil {
			return nil, fmt.Errorf("setting up watch for %s: %v", r.childResourceErrorString(clu.Name), err)
		}
	}

	r.childWriters = make(map[string]map[string]client.Client, len(parentClusters))
	for _, p := range parentClusters {
		r.childWriters[p.Name] = make(map[string]client.Client, len(childClusters))
		for _, c := range childClusters {
			if o.GetImpersonatorForChildWriter != nil {
				cfg := rest.CopyConfig(c.Config)
				cfg.Impersonate = rest.ImpersonationConfig{
					UserName: r.GetImpersonatorForChildWriter(p.Name),
				}
				cli, err := client.New(cfg, client.Options{})
				if err != nil {
					return nil, err
				}
				r.childWriters[p.Name][c.Name] = cli
			} else {
				r.childWriters[p.Name][c.Name] = r.childClients[c.Name]
			}
		}
	}

	if r.MakeSelector == nil {
		r.MakeSelector = r.defaultMakeSelector
	}

	return co, nil
}

type Options struct {
	ParentPrototype               runtime.Object
	ChildPrototype                runtime.Object
	ParentWatchOptions            controller.WatchOptions
	ChildNamespace                string // optional, can optimize List operations vs. it only be set in MakeChild
	Applier                       Applier
	CopyLabels                    bool
	MakeSelector                  func(parent interface{}) labels.Set // optional
	MakeExpectedChildWhenFound    bool
	GetImpersonatorForChildWriter func(clusterName string) string
}

type reconciler struct {
	parentClients map[string]client.Client
	childClients  map[string]client.Client
	childWriters  map[string]map[string]client.Client
	parentGVK     schema.GroupVersionKind
	childGVK      schema.GroupVersionKind
	Options
}

func (r *reconciler) defaultMakeSelector(parent interface{}) labels.Set {
	parentMeta := parent.(metav1.Object)
	s := labels.Set{LabelParentUID: string(parentMeta.GetUID())}
	return s
}

func (r *reconciler) Reconcile(req reconcile.Request) (reconcile.Result, error) {
	// filter out requests enqueued for children whose parents are in a different cluster
	// TODO move this upstream to an option or variation of WatchResourceReconcileController
	parentClusterName := req.Context
	_, ok := r.parentClients[parentClusterName]
	if !ok {
		return reconcile.Result{}, nil
	}

	parent := r.ParentPrototype.DeepCopyObject()
	child := r.ChildPrototype.DeepCopyObject()
	expectedChild := r.ChildPrototype.DeepCopyObject()

	parentMeta := parent.(metav1.Object)
	childMeta := child.(metav1.Object)
	expectedChildMeta := child.(metav1.Object)

	if err := r.parentClients[parentClusterName].Get(context.Background(), req.NamespacedName, parent); err != nil {
		if !errors.IsNotFound(err) {
			return reconcile.Result{}, fmt.Errorf("cannot get %s: %v",
				r.parentObjectErrorString(req.Name, req.Namespace, parentClusterName), err)
		}
		return reconcile.Result{}, nil
	}
	parentMeta.SetClusterName(parentClusterName) // used by

	childClusterName, err := r.Applier.ChildClusterName(parent)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("cannot get child cluster name for %s: %v",
			r.parentObjectErrorString(req.Name, req.Namespace, parentClusterName), err)
	}

	childFound := true

	if err := r.getChild(parent, child, childClusterName); err != nil {
		if !IsChildNotFoundErr(err) {
			// TODO? consider ignoring errors, so we remove finalizers if child cluster is disconnected
			return reconcile.Result{}, fmt.Errorf("cannot get child object of %s: %v",
				r.parentObjectErrorString(req.Name, req.Namespace, parentClusterName), err)
		}
		childFound = false
	}

	needUpdate, needStatusUpdate, err := r.Applier.MutateParent(parent, childFound, child)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("cannot mutate or determine whether %s needs update: %v",
			r.parentObjectErrorString(req.Name, req.Namespace, parentClusterName), err)
	}
	if needUpdate {
		if err := r.parentClients[parentClusterName].Update(context.Background(), parent); err != nil {
			if patterns.IsOptimisticLockError(err) {
				return reconcile.Result{}, nil
			} else {
				return reconcile.Result{}, fmt.Errorf("cannot update %s: %v",
					r.parentObjectErrorString(parentMeta.GetName(), parentMeta.GetNamespace(), parentClusterName), err)
			}
		}
	}
	if needStatusUpdate {
		if err := r.parentClients[parentClusterName].Status().Update(context.Background(), parent); err != nil {
			if patterns.IsOptimisticLockError(err) {
				return reconcile.Result{}, nil
			} else {
				return reconcile.Result{}, fmt.Errorf("cannot update status of %s: %v",
					r.parentObjectErrorString(parentMeta.GetName(), parentMeta.GetNamespace(), parentClusterName), err)
			}
		}
	}

	parentTerminating := parentMeta.GetDeletionTimestamp() != nil

	finalizers := parentMeta.GetFinalizers()
	j := -1
	for i, f := range finalizers {
		if f == "multicluster.admiralty.io/multiclusterForegroundDeletion" {
			j = i
			break
		}
	}
	parentHasFinalizer := j > -1

	if parentTerminating {
		if childFound {
			if err := r.childWriters[parentClusterName][childClusterName].Delete(context.Background(), child); err != nil && !errors.IsNotFound(err) {
				return reconcile.Result{}, fmt.Errorf("cannot delete %s: %v",
					r.childObjectErrorString(childMeta.GetName(), childMeta.GetNamespace(), childClusterName), err)
			}
		} else if parentHasFinalizer {
			// remove finalizer
			parentMeta.SetFinalizers(append(finalizers[:j], finalizers[j+1:]...))
			if err := r.parentClients[parentClusterName].Update(context.Background(), parent); err != nil && !patterns.IsOptimisticLockError(err) {
				return reconcile.Result{}, fmt.Errorf("cannot remove finalizer from %s: %v",
					r.parentObjectErrorString(parentMeta.GetName(), parentMeta.GetNamespace(), parentClusterName), err)
			}
		}
	} else {
		if !parentHasFinalizer {
			parentMeta.SetFinalizers(append(finalizers, "multicluster.admiralty.io/multiclusterForegroundDeletion"))
			if err := r.parentClients[parentClusterName].Update(context.Background(), parent); err != nil && !patterns.IsOptimisticLockError(err) {
				return reconcile.Result{}, fmt.Errorf("cannot add finalizer to %s: %v",
					r.parentObjectErrorString(parentMeta.GetName(), parentMeta.GetNamespace(), parentClusterName), err)
			}
		} else {
			if !childFound || r.MakeExpectedChildWhenFound {
				if err := r.makeChildWrapper(parent, expectedChild); err != nil {
					return reconcile.Result{}, fmt.Errorf("cannot make child from %s: %v",
						r.parentObjectErrorString(parentMeta.GetName(), parentMeta.GetNamespace(), parentClusterName), err)
				}
			}
			if !childFound {
				if err := r.childWriters[parentClusterName][childClusterName].Create(context.Background(), expectedChild); err != nil && !errors.IsAlreadyExists(err) {
					return reconcile.Result{}, fmt.Errorf("cannot create %s: %v",
						r.childObjectErrorString(expectedChildMeta.GetName(), expectedChildMeta.GetNamespace(), childClusterName), err)
				}
			} else {
				needUpdate, err := r.Applier.MutateChild(parent, child, expectedChild)
				if err != nil {
					return reconcile.Result{}, fmt.Errorf("cannot mutate or determine whether %s needs update: %v",
						r.childObjectErrorString(childMeta.GetName(), childMeta.GetNamespace(), childClusterName), err)
				}
				if needUpdate {
					if err := r.childWriters[parentClusterName][childClusterName].Update(context.Background(), child); err != nil && !patterns.IsOptimisticLockError(err) {
						return reconcile.Result{}, fmt.Errorf("cannot update %s: %v",
							r.childObjectErrorString(childMeta.GetName(), childMeta.GetNamespace(), childClusterName), err)
					}
				}
			}
		}
	}

	return reconcile.Result{}, nil
}

func (r *reconciler) getChild(parent runtime.Object, child runtime.Object, childClusterName string) error {
	childList := &unstructured.UnstructuredList{}
	childList.SetGroupVersionKind(r.childGVK)
	s := labels.SelectorFromValidatedSet(r.MakeSelector(parent))
	err := r.childClients[childClusterName].List(context.Background(), childList, client.InNamespace(r.ChildNamespace), client.MatchingLabelsSelector{Selector: s})
	if err != nil {
		return fmt.Errorf("cannot list %s with label selector %s: %v", r.childResourceErrorString(childClusterName), s, err)
	}
	if len(childList.Items) == 0 {
		return r.ChildNotFoundErr(childClusterName, s)
	} else if len(childList.Items) > 1 {
		return r.DuplicateChildErr(childClusterName, s)
	}

	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(childList.Items[0].Object, child); err != nil {
		panic(err)
	}

	childMeta := child.(metav1.Object)
	childMeta.SetClusterName(childClusterName)

	return nil
}

func (r *reconciler) makeChildWrapper(parent runtime.Object, expectedChild runtime.Object) error {
	parentMeta := parent.(metav1.Object)
	expectedChildMeta := expectedChild.(metav1.Object)

	if err := r.Applier.MakeChild(parent, expectedChild); err != nil {
		return err
	}

	if r.ChildNamespace != "" {
		expectedChildMeta.SetNamespace(r.ChildNamespace)
	}

	// we use generate name instead of clusterName-namespace-name combinations to avoid (unlikely) conflicts such as:
	// namespace=foo-bar, name=baz vs. namespace=foo, name=bar-baz
	expectedChildMeta.SetGenerateName(parentMeta.GetName() + "-")

	l := expectedChildMeta.GetLabels()
	if l == nil {
		l = make(labels.Set)
	}
	for k, v := range r.MakeSelector(parent) {
		l[k] = v
	}
	if r.CopyLabels {
		for k, v := range parentMeta.GetLabels() {
			l[k] = v
		}
	}
	expectedChildMeta.SetLabels(l)

	ref := reference.NewMulticlusterOwnerReference(parentMeta, r.parentGVK, parentMeta.GetClusterName())
	if err := reference.SetMulticlusterControllerReference(expectedChildMeta, ref); err != nil {
		return fmt.Errorf("cannot set multi-cluster controller reference: %v", err)
	}

	return nil
}

type Applier interface {
	ChildClusterName(parent interface{}) (string, error)
	MakeChild(parent interface{}, expectedChild interface{}) error
	MutateChild(parent interface{}, child interface{}, expectedChild interface{}) (needUpdate bool, err error)
	MutateParent(parent interface{}, childFound bool, child interface{}) (needUpdate bool, needStatusUpdate bool, err error)
}
