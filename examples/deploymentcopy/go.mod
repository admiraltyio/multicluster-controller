module admiralty.io/multicluster-controller/examples/deploymentcopy

go 1.13

require (
	admiralty.io/multicluster-controller v0.6.0
	admiralty.io/multicluster-service-account v0.6.1
	k8s.io/api v0.18.3
	k8s.io/apimachinery v0.18.3
	k8s.io/client-go v0.18.3
	k8s.io/sample-controller v0.18.3
	sigs.k8s.io/controller-runtime v0.6.0
)

replace admiralty.io/multicluster-controller => ../../
