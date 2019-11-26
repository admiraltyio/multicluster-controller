package gc

import (
	"context"
	"fmt"

	"admiralty.io/multicluster-controller/pkg/cluster"
	"admiralty.io/multicluster-controller/pkg/controller"
	"admiralty.io/multicluster-controller/pkg/reconcile"
	"admiralty.io/multicluster-controller/pkg/reference"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	Prefix                 = "multicluster.admiralty.io/parent-"
	LabelParentName        = Prefix + "name"
	LabelParentNamespace   = Prefix + "namespace"
	LabelParentClusterName = Prefix + "clusterName"
)

func NewController(parentCluster *cluster.Cluster, childCluster *cluster.Cluster, o Options) (*controller.Controller, error) {
	r := &reconciler{Options: o}

	parentClient, err := parentCluster.GetDelegatingClient()
	if err != nil {
		return nil, fmt.Errorf("getting delegating client for parent cluster: %v", err)
	}
	r.parentClient = parentClient

	childClient, err := childCluster.GetDelegatingClient()
	if err != nil {
		return nil, fmt.Errorf("getting delegating client for child cluster: %v", err)
	}
	r.childClient = childClient

	r.isMulticluster = r.parentClient != r.childClient
	r.parentClusterName = parentCluster.Name
	r.childClusterName = childCluster.Name

	parentGVKs, _, err := parentCluster.GetScheme().ObjectKinds(r.ParentPrototype)
	if err != nil {
		return nil, fmt.Errorf("getting GVKs for parent prototype: %v", err)
	}
	if len(parentGVKs) != 1 {
		return nil, fmt.Errorf("parent cluster scheme has %d GVK(s) for parent prototype when 1 is expected")
	}
	r.parentGVK = parentGVKs[0]

	childGVKs, _, err := childCluster.GetScheme().ObjectKinds(r.ChildPrototype)
	if err != nil {
		return nil, fmt.Errorf("getting GVKs for child prototype: %v", err)
	}
	if len(childGVKs) != 1 {
		return nil, fmt.Errorf("child cluster scheme has %d GVK(s) for child prototype when 1 is expected")
	}
	r.childGVK = childGVKs[0]

	if r.MakeSelector == nil {
		r.MakeSelector = r.defaultMakeSelector
	}

	co := controller.New(r, controller.Options{})

	if err := co.WatchResourceReconcileObject(parentCluster, r.ParentPrototype, r.ParentWatchOptions); err != nil {
		return nil, fmt.Errorf("setting up watch for %s: %v", r.parentResourceErrorString(), err)
	}
	if err := co.WatchResourceReconcileController(childCluster, r.ChildPrototype, controller.WatchOptions{Namespace: r.ChildNamespace}); err != nil {
		return nil, fmt.Errorf("setting up watch for %s: %v", r.childResourceErrorString(), err)
	}

	return co, nil
}

type Options struct {
	ParentPrototype            runtime.Object
	ChildPrototype             runtime.Object
	ParentWatchOptions         controller.WatchOptions
	ChildNamespace             string // optional, can optimize List operations vs. it only be set in MakeChild
	Applier                    Applier
	CopyLabels                 bool
	MakeSelector               func(parent interface{}) labels.Set // optional
	MakeExpectedChildWhenFound bool
}

type reconciler struct {
	parentClient      client.Client
	childClient       client.Client
	isMulticluster    bool
	parentClusterName string // used for owner refs and error msgs, empty for single-cluster, cross-namespace use case
	childClusterName  string // only used for error messages
	parentGVK         schema.GroupVersionKind
	childGVK          schema.GroupVersionKind
	Options
}

func (r *reconciler) defaultMakeSelector(parent interface{}) labels.Set {
	parentMeta := parent.(metav1.Object)
	s := labels.Set{
		LabelParentName:      parentMeta.GetName(),
		LabelParentNamespace: parentMeta.GetNamespace(),
	}
	if r.isMulticluster {
		s[LabelParentClusterName] = r.parentClusterName
	}
	return s
}

func (r *reconciler) Reconcile(req reconcile.Request) (reconcile.Result, error) {
	parent := r.ParentPrototype.DeepCopyObject()
	child := r.ChildPrototype.DeepCopyObject()
	expectedChild := r.ChildPrototype.DeepCopyObject()

	parentMeta := parent.(metav1.Object)
	childMeta := child.(metav1.Object)
	expectedChildMeta := child.(metav1.Object)

	if err := r.parentClient.Get(context.Background(), req.NamespacedName, parent); err != nil {
		if !errors.IsNotFound(err) {
			return reconcile.Result{}, fmt.Errorf("cannot get %s: %v",
				r.parentObjectErrorString(req.Name, req.Namespace), err)
		}
		return reconcile.Result{}, nil
	}

	childFound := true

	if err := r.getChild(parent, child); err != nil {
		if !IsChildNotFoundErr(err) {
			// TODO? consider ignoring errors, so we remove finalizers if child cluster is disconnected
			return reconcile.Result{}, fmt.Errorf("cannot get child object of %s: %v",
				r.parentObjectErrorString(req.Name, req.Namespace), err)
		}
		childFound = false
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
			if err := r.childClient.Delete(context.Background(), child); err != nil && !errors.IsNotFound(err) {
				return reconcile.Result{}, fmt.Errorf("cannot delete %s: %v",
					r.childObjectErrorString(childMeta.GetName(), childMeta.GetNamespace()), err)
			}
		} else if parentHasFinalizer {
			// remove finalizer
			parentMeta.SetFinalizers(append(finalizers[:j], finalizers[j+1:]...))
			if err := r.parentClient.Update(context.Background(), parent); err != nil {
				return reconcile.Result{}, fmt.Errorf("cannot remove finalizer from %s: %v",
					r.parentObjectErrorString(parentMeta.GetName(), parentMeta.GetNamespace()), err)
			}
		}
	} else {
		if !parentHasFinalizer {
			parentMeta.SetFinalizers(append(finalizers, "multicluster.admiralty.io/multiclusterForegroundDeletion"))
			if err := r.parentClient.Update(context.Background(), parent); err != nil {
				return reconcile.Result{}, fmt.Errorf("cannot add finalizer to %s: %v",
					r.parentObjectErrorString(parentMeta.GetName(), parentMeta.GetNamespace()), err)
			}
		} else {
			if !childFound || r.MakeExpectedChildWhenFound {
				if err := r.makeChildWrapper(parent, expectedChild); err != nil {
					return reconcile.Result{}, fmt.Errorf("cannot make child from %s: %v",
						r.parentObjectErrorString(parentMeta.GetName(), parentMeta.GetNamespace()), err)
				}
			}
			if !childFound {
				if err := r.childClient.Create(context.Background(), expectedChild); err != nil && !errors.IsAlreadyExists(err) {
					return reconcile.Result{}, fmt.Errorf("cannot create %s: %v",
						r.childObjectErrorString(expectedChildMeta.GetName(), expectedChildMeta.GetNamespace()), err)
				}
			} else {
				needUpdate, err := r.Applier.ChildNeedsUpdate(parent, child, expectedChild)
				if err != nil {
					return reconcile.Result{}, fmt.Errorf("cannot determine whether %s needs update: %v",
						r.childObjectErrorString(childMeta.GetName(), childMeta.GetNamespace()), err)
				}
				if needUpdate {
					if err := r.Applier.MutateChild(parent, child, expectedChild); err != nil {
						return reconcile.Result{}, fmt.Errorf("cannot mutate %s: %v",
							r.childObjectErrorString(childMeta.GetName(), childMeta.GetNamespace()), err)
					}
					if err := r.childClient.Update(context.Background(), child); err != nil {
						return reconcile.Result{}, fmt.Errorf("cannot update %s: %v",
							r.childObjectErrorString(childMeta.GetName(), childMeta.GetNamespace()), err)
					}
				}
			}
		}
	}

	return reconcile.Result{}, nil
}

func (r *reconciler) getChild(parent runtime.Object, child runtime.Object) error {
	childList := &unstructured.UnstructuredList{}
	childList.SetGroupVersionKind(r.childGVK)
	s := labels.SelectorFromValidatedSet(r.MakeSelector(parent))
	err := r.childClient.List(context.Background(), &client.ListOptions{Namespace: r.ChildNamespace, LabelSelector: s}, childList)
	if err != nil {
		return fmt.Errorf("cannot list %s with label selector %s: %v", r.childResourceErrorString(), s, err)
	}
	if len(childList.Items) == 0 {
		return r.ChildNotFoundErr(s)
	} else if len(childList.Items) > 1 {
		return r.DuplicateChildErr(s)
	}

	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(childList.Items[0].Object, child); err != nil {
		panic(err)
	}

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

	// we use generate name to avoid (unlikely) conflicts such as:
	// namespace=foo-bar, name=baz vs. namespace=foo, name=bar-baz
	genName := ""
	if r.isMulticluster {
		genName += fmt.Sprintf("%s-", r.parentClusterName)
	}
	if parentMeta.GetNamespace() != "" {
		genName += fmt.Sprintf("%s-", parentMeta.GetNamespace())
	}
	genName += fmt.Sprintf("%s-", parentMeta.GetName())
	if len(genName) > 253 {
		genName = genName[0:253]
	}
	expectedChildMeta.SetGenerateName(genName)

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

	ref := reference.NewMulticlusterOwnerReference(parentMeta, r.parentGVK, r.parentClusterName)
	if err := reference.SetMulticlusterControllerReference(expectedChildMeta, ref); err != nil {
		return fmt.Errorf("cannot set multi-cluster controller reference: %v", err)
	}

	return nil
}

type Applier interface {
	MakeChild(parent interface{}, expectedChild interface{}) error
	ChildNeedsUpdate(parent interface{}, child interface{}, expectedChild interface{}) (bool, error)
	MutateChild(parent interface{}, child interface{}, expectedChild interface{}) error
}
