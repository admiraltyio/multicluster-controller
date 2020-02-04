package gc

import (
	"fmt"

	"k8s.io/apimachinery/pkg/labels"
)

func (r *reconciler) parentResourceErrorString(clusterName string) string {
	return fmt.Sprintf("parent resource %s in cluster %s", r.parentGVK.Kind, clusterName)
}

func (r *reconciler) childResourceErrorString(clusterName string) string {
	inNS := ""
	if r.ChildNamespace != "" {
		inNS = fmt.Sprintf(" in namespace %s", r.ChildNamespace)
	}
	return fmt.Sprintf("child resource %s%s in cluster %s", r.childGVK.Kind, inNS, clusterName)
}

func (r *reconciler) parentObjectErrorString(name, namespace, clusterName string) string {
	inNS := ""
	if namespace != "" {
		inNS = fmt.Sprintf(" in namespace %s", namespace)
	}
	return fmt.Sprintf("parent object %s %s%s in cluster %s", r.parentGVK.Kind, name, inNS, clusterName)
}

func (r *reconciler) childObjectErrorString(name, namespace, clusterName string) string {
	inNS := ""
	if namespace != "" {
		inNS = fmt.Sprintf(" in namespace %s", namespace)
	}
	return fmt.Sprintf("child object %s %s%s in cluster %s", r.childGVK.Kind, name, inNS, clusterName)
}

type childNotFoundErr struct {
	s string
}

func (e *childNotFoundErr) Error() string {
	return e.s
}

func (r *reconciler) ChildNotFoundErr(clusterName string, s labels.Selector) error {
	return &childNotFoundErr{s: fmt.Sprintf("%s not found with label selector %s",
		r.childResourceErrorString(clusterName), s)}
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

func (r *reconciler) DuplicateChildErr(clusterName string, s labels.Selector) error {
	return &duplicateChildErr{s: fmt.Sprintf("duplicate %s found with label selector %s",
		r.childResourceErrorString(clusterName), s)}
}

func IsDuplicateChildErr(err error) bool {
	_, ok := err.(*duplicateChildErr)
	return ok
}
