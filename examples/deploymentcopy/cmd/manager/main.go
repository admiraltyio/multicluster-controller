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
	"flag"
	"log"

	"admiralty.io/multicluster-controller/examples/deploymentcopy/pkg/controller/deploymentcopy"
	"admiralty.io/multicluster-controller/pkg/cluster"
	"admiralty.io/multicluster-controller/pkg/manager"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/sample-controller/pkg/signals"
)

func main() {
	flag.Parse()
	if flag.NArg() != 2 {
		log.Fatalf("Usage: deploymentcopy sourcecontext destinationcontext")
	}
	srcCtx, dstCtx := flag.Arg(0), flag.Arg(1)

	cl1 := cluster.New(cluster.Options{Context: srcCtx, CacheOptions: cluster.CacheOptions{Namespace: "default"}})
	cl2 := cluster.New(cluster.Options{Context: dstCtx, CacheOptions: cluster.CacheOptions{Namespace: "default"}})

	co, err := deploymentcopy.NewController(cl1, cl2)
	if err != nil {
		log.Fatalf("creating deploymentcopy controller: %v", err)
	}

	m := manager.New()
	m.AddController(co)

	if err := m.Start(signals.SetupSignalHandler()); err != nil {
		log.Fatalf("while or after starting manager: %v", err)
	}
}
