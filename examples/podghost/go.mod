module admiralty.io/multicluster-controller/examples/podghost

go 1.13

require (
	admiralty.io/multicluster-controller v0.3.1
	admiralty.io/multicluster-service-account v0.6.1
	k8s.io/api v0.17.0
	k8s.io/apimachinery v0.17.0
	k8s.io/client-go v0.17.0
	k8s.io/sample-controller v0.17.0
	sigs.k8s.io/controller-runtime v0.4.0
)

replace admiralty.io/multicluster-controller => ../../
