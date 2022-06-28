/*
Copyright 2022 The SODA Authors.

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

// Code generated by client-gen. DO NOT EDIT.

package v1beta1

import (
	"context"
	"time"

	v1beta1 "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	scheme "github.com/soda-cdm/kahu/client/clientset/versioned/scheme"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	watch "k8s.io/apimachinery/pkg/watch"
	rest "k8s.io/client-go/rest"
)

// ProvidersGetter has a method to return a ProviderInterface.
// A group's client should implement this interface.
type ProvidersGetter interface {
	Providers() ProviderInterface
}

// ProviderInterface has methods to work with Provider resources.
type ProviderInterface interface {
	Create(ctx context.Context, provider *v1beta1.Provider, opts v1.CreateOptions) (*v1beta1.Provider, error)
	UpdateStatus(ctx context.Context, provider *v1beta1.Provider, opts v1.UpdateOptions) (*v1beta1.Provider, error)
	Delete(ctx context.Context, name string, opts v1.DeleteOptions) error
	DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error
	Get(ctx context.Context, name string, opts v1.GetOptions) (*v1beta1.Provider, error)
	List(ctx context.Context, opts v1.ListOptions) (*v1beta1.ProviderList, error)
	Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error)
	ProviderExpansion
}

// providers implements ProviderInterface
type providers struct {
	client rest.Interface
}

// newProviders returns a Providers
func newProviders(c *KahuV1beta1Client) *providers {
	return &providers{
		client: c.RESTClient(),
	}
}

// Get takes name of the provider, and returns the corresponding provider object, and an error if there is any.
func (c *providers) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1beta1.Provider, err error) {
	result = &v1beta1.Provider{}
	err = c.client.Get().
		Resource("providers").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do(ctx).
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of Providers that match those selectors.
func (c *providers) List(ctx context.Context, opts v1.ListOptions) (result *v1beta1.ProviderList, err error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	result = &v1beta1.ProviderList{}
	err = c.client.Get().
		Resource("providers").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Do(ctx).
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested providers.
func (c *providers) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	opts.Watch = true
	return c.client.Get().
		Resource("providers").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Watch(ctx)
}

// Create takes the representation of a provider and creates it.  Returns the server's representation of the provider, and an error, if there is any.
func (c *providers) Create(ctx context.Context, provider *v1beta1.Provider, opts v1.CreateOptions) (result *v1beta1.Provider, err error) {
	result = &v1beta1.Provider{}
	err = c.client.Post().
		Resource("providers").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(provider).
		Do(ctx).
		Into(result)
	return
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *providers) UpdateStatus(ctx context.Context, provider *v1beta1.Provider, opts v1.UpdateOptions) (result *v1beta1.Provider, err error) {
	result = &v1beta1.Provider{}
	err = c.client.Put().
		Resource("providers").
		Name(provider.Name).
		SubResource("status").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(provider).
		Do(ctx).
		Into(result)
	return
}

// Delete takes name of the provider and deletes it. Returns an error if one occurs.
func (c *providers) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	return c.client.Delete().
		Resource("providers").
		Name(name).
		Body(&opts).
		Do(ctx).
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *providers) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	var timeout time.Duration
	if listOpts.TimeoutSeconds != nil {
		timeout = time.Duration(*listOpts.TimeoutSeconds) * time.Second
	}
	return c.client.Delete().
		Resource("providers").
		VersionedParams(&listOpts, scheme.ParameterCodec).
		Timeout(timeout).
		Body(&opts).
		Do(ctx).
		Error()
}