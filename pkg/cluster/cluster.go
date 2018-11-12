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

// Package cluster handles Kubernetes dependencies.
// They are grouped by cluster under Cluster structs.
package cluster // import "admiralty.io/multicluster-controller/pkg/cluster"

import (
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	clientgocache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// Cluster stores a Kubernetes client, cache, and other cluster-scoped dependencies.
// The dependencies are lazily created in getters and cached for reuse.
type Cluster struct {
	Name   string
	Config *rest.Config
	scheme *runtime.Scheme
	mapper meta.RESTMapper
	cache  cache.Cache
	client *client.DelegatingClient
	Options
}

// Options is used as an argument of New.
// For now it only embeds CacheOptions but we could add non-cache options in the future.
type Options struct {
	CacheOptions
}

// CacheOptions is embedded in Options to configure the new Cluster's cache.
type CacheOptions struct {
	// Resync is the period between cache resyncs.
	// A cache resync triggers event handlers for each object watched by the cache.
	// It can be useful if your level-based logic isn't perfect.
	Resync *time.Duration
	// Namespace can be used to watch only a single namespace.
	// If unset (Namespace == ""), all namespaces are watched.
	Namespace string
}

// New creates a new Cluster.
func New(name string, config *rest.Config, o Options) *Cluster {
	return &Cluster{Name: name, Config: config, Options: o}
}

// GetClusterName returns the context given when Cluster c was created.
func (c *Cluster) GetClusterName() string {
	return c.Name
}

// GetScheme returns the default client-go scheme.
// It is used by other Cluster getters, and to add custom resources to the scheme.
func (c *Cluster) GetScheme() *runtime.Scheme {
	return scheme.Scheme
}

// GetMapper returns a lazily created apimachinery RESTMapper.
// It is used by other Cluster getters. TODO: consider not exporting.
func (c *Cluster) GetMapper() (meta.RESTMapper, error) {
	if c.mapper != nil {
		return c.mapper, nil
	}

	mapper, err := apiutil.NewDiscoveryRESTMapper(c.Config)
	if err != nil {
		return nil, err
	}

	c.mapper = mapper
	return mapper, nil
}

// GetCache returns a lazily created controller-runtime Cache.
// It is used by other Cluster getters. TODO: consider not exporting.
func (c *Cluster) GetCache() (cache.Cache, error) {
	if c.cache != nil {
		return c.cache, nil
	}

	m, err := c.GetMapper()
	if err != nil {
		return nil, err
	}

	ca, err := cache.New(c.Config, cache.Options{
		Scheme:    c.GetScheme(),
		Mapper:    m,
		Resync:    c.Resync,
		Namespace: c.Namespace,
	})
	if err != nil {
		return nil, err
	}

	c.cache = ca
	return ca, nil
}

// GetDelegatingClient returns a lazily created controller-runtime DelegatingClient.
// It is used by other Cluster getters, and by reconcilers.
// TODO: consider implementing Reader, Writer and StatusClient in Cluster
// and forwarding to actual delegating client.
func (c *Cluster) GetDelegatingClient() (*client.DelegatingClient, error) {
	if c.client != nil {
		return c.client, nil
	}

	ca, err := c.GetCache()
	if err != nil {
		return nil, err
	}

	m, err := c.GetMapper()
	if err != nil {
		return nil, err
	}

	cl, err := client.New(c.Config, client.Options{
		Scheme: c.GetScheme(),
		Mapper: m,
	})
	if err != nil {
		return nil, err
	}

	dc := &client.DelegatingClient{
		Reader: &client.DelegatingReader{
			CacheReader:  ca,
			ClientReader: cl,
		},
		Writer:       cl,
		StatusClient: cl,
	}

	c.client = dc
	return dc, nil
}

// AddEventHandler instructs the Cluster's cache to watch objectType's resource,
// if it doesn't already, and to add handler as an event handler.
func (c *Cluster) AddEventHandler(objectType runtime.Object, handler clientgocache.ResourceEventHandler) error {
	ca, err := c.GetCache()
	if err != nil {
		return err
	}

	i, err := ca.GetInformer(objectType)
	if err != nil {
		return err
	}

	i.AddEventHandler(handler)
	return nil
}

// Start starts the Cluster's cache and blocks,
// until an empty struct is sent to the stop channel.
func (c *Cluster) Start(stop <-chan struct{}) error {
	ca, err := c.GetCache()
	if err != nil {
		return err
	}
	return ca.Start(stop)
}

// WaitForCacheSync waits for the Cluster's cache to sync,
// OR until an empty struct is sent to the stop channel.
func (c *Cluster) WaitForCacheSync(stop <-chan struct{}) bool {
	ca, err := c.GetCache()
	if err != nil {
		return false
	}
	return ca.WaitForCacheSync(stop)
}
