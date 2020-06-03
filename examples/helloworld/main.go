/*
Copyright 2018 The Multicluster-Controller Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"flag"
	"log"
	"strings"

	"admiralty.io/multicluster-service-account/pkg/config"
	"k8s.io/api/core/v1"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/sample-controller/pkg/signals"

	"admiralty.io/multicluster-controller/pkg/cluster"
	"admiralty.io/multicluster-controller/pkg/controller"
	"admiralty.io/multicluster-controller/pkg/manager"
	"admiralty.io/multicluster-controller/pkg/reconcile"
)

func main() {
	stopCh := signals.SetupSignalHandler()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-stopCh
		cancel()
	}()

	var f = flag.String("contexts", "", "a comma-separated list of contexts to watch, e.g., cluster1,cluster2")
	flag.Parse()
	kubeCtxs := strings.Split(*f, ",")

	co := controller.New(&reconciler{}, controller.Options{})

	for _, kubeCtx := range kubeCtxs {
		cfg, _, err := config.NamedConfigAndNamespace(kubeCtx)
		if err != nil {
			log.Fatal(err)
		}
		cl := cluster.New(kubeCtx, cfg, cluster.Options{})
		if err := co.WatchResourceReconcileObject(ctx, cl, &v1.Pod{}, controller.WatchOptions{}); err != nil {
			log.Fatal(err)
		}
	}

	m := manager.New()
	m.AddController(co)

	if err := m.Start(stopCh); err != nil {
		log.Fatal(err)
	}
}

type reconciler struct{}

func (r *reconciler) Reconcile(req reconcile.Request) (reconcile.Result, error) {
	log.Printf("%s / %s / %s", req.Context, req.Namespace, req.Name)
	return reconcile.Result{}, nil
}
