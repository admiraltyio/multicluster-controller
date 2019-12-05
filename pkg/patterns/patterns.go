package patterns

import "strings"

// from k8s.io/apiserver/pkg/registry/generic/registry/store.go#L201 (no need to import package fro just the one constant)
var optimisticLockErrorMsg = "the object has been modified; please apply your changes to the latest version and try again"

func IsOptimisticLockError(err error) bool {
	return strings.Contains(err.Error(), optimisticLockErrorMsg)
}
