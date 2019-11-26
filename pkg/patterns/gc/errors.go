package gc

import (
	"fmt"
	"k8s.io/apimachinery/pkg/labels"
)

func (r *reconciler) parentResourceErrorString() string {
	inCluster := ""
	if r.isMulticluster {
		inCluster = fmt.Sprintf(" in cluster %s", r.parentClusterName)
	}
	return "parent resource " + r.parentGVK.Kind + inCluster
}

func (r *reconciler) childResourceErrorString() string {
	inNS := ""
	if r.ChildNamespace != "" {
		inNS = fmt.Sprintf(" in namespace %s", r.ChildNamespace)
	}
	inCluster := ""
	if r.isMulticluster {
		inCluster = fmt.Sprintf(" in cluster %s", r.childClusterName)
	}
	return "child resource " + r.childGVK.Kind + inNS + inCluster
}

func (r *reconciler) parentObjectErrorString(name string, namespace string) string {
	inCluster := ""
	if r.isMulticluster {
		inCluster = fmt.Sprintf(" in cluster %s", r.parentClusterName)
	}
	return fmt.Sprintf("parent object %s %s in namespace %s%s", r.parentGVK.Kind, name, namespace, inCluster)
}

func (r *reconciler) childObjectErrorString(name string, namespace string) string {
	inCluster := ""
	if r.isMulticluster {
		inCluster = fmt.Sprintf(" in cluster %s", r.childClusterName)
	}
	return fmt.Sprintf("child object %s %s in namespace %s%s", r.childGVK.Kind, name, namespace, inCluster)
}

type childNotFoundErr struct {
	s string
}

func (e *childNotFoundErr) Error() string {
	return e.s
}

func (r *reconciler) ChildNotFoundErr(s labels.Selector) error {
	return &childNotFoundErr{s: fmt.Sprintf("%s not found with label selector %s",
		r.childResourceErrorString(), s)}
}

func IsChildNotFoundErr(err error) bool {
	_, ok := err.(*childNotFoundErr)
	return ok
}

type duplicateChildErr struct {
	s string
}

func (e *duplicateChildErr) Error() string {
	return e.s
}

func (r *reconciler) DuplicateChildErr(s labels.Selector) error {
	return &duplicateChildErr{s: fmt.Sprintf("duplicate %s found with label selector %s",
		r.childResourceErrorString(), s)}
}

func IsDuplicateChildErr(err error) bool {
	_, ok := err.(*duplicateChildErr)
	return ok
}
