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

package config

import (
	log "github.com/sirupsen/logrus"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"

	"github.com/soda-cdm/kahu/client"
	"github.com/soda-cdm/kahu/client/clientset/versioned"
	kahuinformer "github.com/soda-cdm/kahu/client/informers/externalversions"
	"github.com/soda-cdm/kahu/discovery"
	"github.com/soda-cdm/kahu/hooks"
)

const (
	controllerManagerClientAgent = "controller-manager"
	eventComponentName           = "kahu-controller-manager"
)

type Config struct {
	ControllerWorkers    int
	EnableLeaderElection bool
	DisableControllers   []string
	KahuClientConfig     client.Config
}

type CompletedConfig struct {
	*Config
	ClientFactory    client.Factory
	KubeClient       kubernetes.Interface
	KahuClient       versioned.Interface
	KahuInformer     kahuinformer.SharedInformerFactory
	DynamicClient    dynamic.Interface
	DiscoveryHelper  discovery.DiscoveryHelper
	EventBroadcaster record.EventBroadcaster
	HookExecutor     hooks.Hooks
}

func (cfg *Config) Complete() (*CompletedConfig, error) {
	clientFactory := client.NewFactory(controllerManagerClientAgent, &cfg.KahuClientConfig)

	kubeClient, err := clientFactory.KubeClient()
	if err != nil {
		return nil, err
	}

	kahuClient, err := clientFactory.KahuClient()
	if err != nil {
		return nil, err
	}

	dynamicClient, err := clientFactory.DynamicClient()
	if err != nil {
		return nil, err
	}

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartStructuredLogging(0)
	eventBroadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})

	discoveryHelper, err := discovery.NewDiscoveryHelper(kahuClient.Discovery(),
		log.WithField("client", "discovery"))
	if err != nil {
		return nil, err
	}

	// New Hook object
	restConfig, err := clientFactory.ClientConfig()
	if err != nil {
		return nil, err
	}
	hookExecutor, err := hooks.NewHooks(kubeClient, restConfig)
	if err != nil {
		log.Errorf("failed to create hook, error %s\n", err.Error())
		return nil, err
	}

	return &CompletedConfig{
		Config:           cfg,
		ClientFactory:    clientFactory,
		KubeClient:       kubeClient,
		KahuClient:       kahuClient,
		DynamicClient:    dynamicClient,
		DiscoveryHelper:  discoveryHelper,
		EventBroadcaster: eventBroadcaster,
		HookExecutor:     hookExecutor,
		KahuInformer:     kahuinformer.NewSharedInformerFactoryWithOptions(kahuClient, 0),
	}, nil
}

func (cfg *CompletedConfig) Print() {
	log.Infof("Controller manager config %+v", cfg.Config)
}
