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

package backup

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"

	metaservice "github.com/soda-cdm/kahu/providerframework/metaservice/lib/go"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	"github.com/soda-cdm/kahu/client/clientset/versioned"
	kahuv1client "github.com/soda-cdm/kahu/client/clientset/versioned/typed/kahu/v1beta1"
	kahuinformer "github.com/soda-cdm/kahu/client/informers/externalversions/kahu/v1beta1"
	kahulister "github.com/soda-cdm/kahu/client/listers/kahu/v1beta1"
	"github.com/soda-cdm/kahu/controllers"
	"github.com/soda-cdm/kahu/utils"
)

const (
	controllerName = "BackupController"
	controllerOps  = "Backup"
)

// var KindList map[string]int
var KindList = make(map[string]GroupResouceVersion)

type Config struct {
	MetaServicePort    uint
	MetaServiceAddress string
}

type Controller struct {
	config               *Config
	logger               log.FieldLogger
	restClientconfig     *restclient.Config
	controller           controllers.Controller
	client               kubernetes.Interface
	kahuClient           versioned.Interface
	backupLister         kahulister.BackupLister
	backupClient         kahuv1client.BackupInterface
	backupLocationClient kahuv1client.BackupLocationInterface
}

func NewController(config *Config,
	restClientconfig *restclient.Config,
	kahuClient versioned.Interface,
	backupInformer kahuinformer.BackupInformer) (controllers.Controller, error) {

	logger := log.WithField("controller", controllerName)
	backupController := &Controller{
		kahuClient:           kahuClient,
		backupLister:         backupInformer.Lister(),
		restClientconfig:     restClientconfig,
		backupClient:         kahuClient.KahuV1beta1().Backups(),
		backupLocationClient: kahuClient.KahuV1beta1().BackupLocations(),
		config:               config,
		logger:               logger,
	}

	// register to informer to receive events and push events to worker queue
	backupInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    backupController.handleAdd,
			DeleteFunc: backupController.handleDel,
		},
	)

	// construct controller interface to process worker queue
	controller, err := controllers.NewControllerBuilder(controllerName).
		SetLogger(logger).
		SetHandler(backupController.runBackup).
		Build()
	if err != nil {
		return nil, err
	}

	// reference back
	backupController.controller = controller
	return controller, err
}

func (c *Controller) handleAdd(obj interface{}) {
	backup := obj.(*v1beta1.Backup)

	switch backup.Status.Phase {
	case "", v1beta1.BackupPhaseInit:
	default:
		c.logger.WithFields(log.Fields{
			"backup": utils.NamespaceAndName(backup),
			"phase":  backup.Status.Phase,
		}).Infof("Backup: %s is not New, so will not be processed", backup.Name)
		return
	}
	c.controller.Enqueue(obj)
}

func (c *Controller) handleDel(obj interface{}) {
	c.controller.Enqueue(obj)
}

func (c *Controller) runBackup(key string) error {
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		c.logger.Errorf("splitting key into namespace and name, error %s\n", err.Error())
		return err
	}

	backup, err := c.backupLister.Get(name)
	if err != nil {
		c.logger.Errorf("error %s, Getting the backup resource from lister", err.Error())

		if apierrors.IsNotFound(err) {
			c.logger.Debugf("backup %s not found", name)
		}
		return err
	}

	c.logger.WithField(controllerOps, utils.NamespaceAndName(backup)).
		Info("Setting up backup log")

	var resourcesList []*KubernetesResource

	k8sClient, err := utils.GetK8sClient(c.restClientconfig)
	if err != nil {
		c.logger.Errorf("unable to get k8s client:%s", err)
		return err
	}

	_, resource, _ := k8sClient.ServerGroupsAndResources()
	for _, group := range resource {
		groupItems, err := c.getGroupItems(group)

		if err != nil {
			c.logger.WithError(err).WithField("apiGroup", group.String()).Error("Error collecting resources from API group")
			continue
		}

		resourcesList = append(resourcesList, groupItems...)

	}

	// TODO: Get address and port from backup location
	grpcConnection, err := utils.GetgrpcConn(c.config.MetaServiceAddress, c.config.MetaServicePort)
	if err != nil {
		c.logger.Errorf("grpc connection error %s", err)
		return err
	}

	metaClient := utils.GetMetaserviceClient(grpcConnection)
	backupClient, err := metaClient.Backup(context.Background())
	if err != nil {
		c.logger.Errorf("backup request error %s", err)
		return err
	}

	err = backupClient.Send(&metaservice.BackupRequest{
		Backup: &metaservice.BackupRequest_Identifier{
			Identifier: &metaservice.BackupIdentifier{
				BackupHandle: backup.Name,
			},
		},
	})
	c.logger.Infof("metaservice.BackupRequest_Identifier is ==== %+v", err)

	if err != nil {
		c.logger.Errorf("Unable to connect metadata service %s", err)
		return err
	}

	for _, kind := range KindList {
		err := c.backup(kind.group, kind.version, kind.resourceName, backupClient)
		if err != nil {
			c.logger.Errorf("backup was not successful for resource name %s. error:%s, continuing", kind.resourceName, err)
			//return err
			continue
		}
	}

	_, err = backupClient.CloseAndRecv()
	return err
}

// getGroupItems collects all relevant items from a single API group.
func (c *Controller) getGroupItems(group *metav1.APIResourceList) ([]*KubernetesResource, error) {
	c.logger.WithField("group", group.GroupVersion)

	// Parse so we can check if this is the core group
	gv, err := schema.ParseGroupVersion(group.GroupVersion)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing GroupVersion %q", group.GroupVersion)
	}
	if gv.Group == "" {
		sortCoreGroup(group)
	}

	var items []*KubernetesResource
	for _, resource := range group.APIResources {
		resourceItems, err := c.getResourceItems(gv, resource)
		if err != nil {
			c.logger.WithError(err).WithField("resource", resource.String()).Error("Error getting items for resource")
			continue
		}

		items = append(items, resourceItems...)
	}

	return items, nil
}

// getResourceItems collects all relevant items for a given group-version-resource.
func (c *Controller) getResourceItems(gv schema.GroupVersion, resource metav1.APIResource) ([]*KubernetesResource, error) {
	gvr := gv.WithResource(resource.Name)

	_, ok := KindList[resource.Kind]
	if !ok {
		groupResourceVersion := GroupResouceVersion{
			resourceName: gvr.Resource,
			version:      gvr.Version,
			group:        gvr.Group,
		}
		KindList[resource.Kind] = groupResourceVersion
	}

	var items []*KubernetesResource

	return items, nil
}

// sortCoreGroup sorts the core API group.
func sortCoreGroup(group *metav1.APIResourceList) {
	sort.SliceStable(group.APIResources, func(i, j int) bool {
		return CoreGroupResourcePriority(group.APIResources[i].Name) < CoreGroupResourcePriority(group.APIResources[j].Name)
	})
}

func (c *Controller) backup(group, version, resource string, backupClient metaservice.MetaService_BackupClient) error {

	dynamicClient, err := dynamic.NewForConfig(c.restClientconfig)
	if err != nil {
		c.logger.Errorf("error creating dynamic client: %v\n", err)
		return err
	}

	gvr := schema.GroupVersionResource{
		Group:    group,
		Version:  version,
		Resource: resource,
	}

	c.logger.Debugf("group:%s, version:%s, resource:%s", gvr.Group, gvr.Version, gvr.Resource)
	objectsList, err := dynamicClient.Resource(gvr).Namespace("default").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		c.logger.Debugf("error getting %s, %v\n", resource, err)
	}

	resourceObjects, err := meta.ExtractList(objectsList)
	if err != nil {
		return err
	}

	for _, o := range resourceObjects {
		runtimeObject, ok := o.(runtime.Unstructured)
		if !ok {
			c.logger.Errorf("error casting object: %v", o)
			return err
		}

		metadata, err := meta.Accessor(runtimeObject)
		if err != nil {
			return err
		}

		//resourceData, _ := json.MarshalIndent(metadata, "", " ")

		resourceData, err := json.Marshal(metadata)
		if err != nil {
			c.logger.Errorf("Unable to get resource content: %s", err)
			return err
		}

		c.logger.Infof("resourceData is ==== %+v", metadata)
		c.logger.Infof("metadata.GetName() is ==== %+v", metadata.GetName())
		c.logger.Infof("Kind is ==== %+v", runtimeObject.GetObjectKind().GroupVersionKind().Kind)

		err = backupClient.Send(&metaservice.BackupRequest{
			Backup: &metaservice.BackupRequest_BackupResource{
				BackupResource: &metaservice.BackupResource{
					Resource: &metaservice.Resource{
						Name:    metadata.GetName(),
						Group:   runtimeObject.GetObjectKind().GroupVersionKind().Group,
						Version: runtimeObject.GetObjectKind().GroupVersionKind().Version,
						Kind:    runtimeObject.GetObjectKind().GroupVersionKind().Kind,
					},
					Data: resourceData,
				},
			},
		})
		c.logger.Infof("backupClient.Send result ==== %+v", err)
	}
	return err
}
