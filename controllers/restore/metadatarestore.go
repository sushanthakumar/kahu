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

package restore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"k8s.io/apimachinery/pkg/util/sets"

	log "github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	metaservice "github.com/soda-cdm/kahu/providerframework/metaservice/lib/go"
	"github.com/soda-cdm/kahu/utils"
)

const (
	crdName = "CustomResourceDefinition"
)

var (
	excludedResources = sets.NewString(
		"Node",
		"Namespace",
		"Event",
	)
)

type backupInfo struct {
	backup         *kahuapi.Backup
	backupLocation *kahuapi.BackupLocation
	backupProvider *kahuapi.Provider
}

func (ctx *restoreContext) processMetadataRestore(restore *kahuapi.Restore) error {
	backup, err := ctx.fetchBackup(restore)
	if err != nil {
		// update failure reason
		if len(restore.Status.FailureReason) > 0 {
			restore, err = ctx.updateRestoreStatus(restore)
			return err
		}
		return err
	}

	// fetch backup info
	backupInfo, err := ctx.fetchBackupInfo(restore)
	if err != nil {
		if len(restore.Status.FailureReason) > 0 {
			restore, err = ctx.updateRestoreStatus(restore)
			return err
		}
		return err
	}

	// construct backup identifier
	backupIdentifier, err := utils.GetBackupIdentifier(backupInfo.backup,
		backupInfo.backupLocation,
		backupInfo.backupProvider)
	if err != nil {
		restore.Status.Phase = kahuapi.RestorePhaseFailed
		restore.Status.FailureReason = fmt.Sprintf("Failed to get backup identifier "+
			"for backup (%s). %s", backup.Name, err)
		restore, err = ctx.updateRestoreStatus(restore)
		return err
	}

	// fetch backup content and cache them
	err = ctx.fetchBackupContent(backupInfo.backupProvider, backupIdentifier, restore)
	if err != nil {
		restore.Status.Phase = kahuapi.RestorePhaseFailed
		restore.Status.FailureReason = fmt.Sprintf("Failed to get backup content "+
			"for backup (%s)", backup.Name)
		restore, err = ctx.updateRestoreStatus(restore)
		return err
	}

	// filter resources from cache
	err = ctx.filter.handle(restore)
	if err != nil {
		restore.Status.Phase = kahuapi.RestorePhaseFailed
		errMsg := fmt.Sprintf("Failed to filter resources. %s", err)
		restore.Status.FailureReason = errMsg
		restore, err = ctx.updateRestoreStatus(restore)
		return err
	}

	// add mutation
	err = ctx.mutator.handle(restore)
	if err != nil {
		restore.Status.Phase = kahuapi.RestorePhaseFailed
		restore.Status.FailureReason = fmt.Sprintf("Failed to mutate resources. %s", err)
		restore, err = ctx.updateRestoreStatus(restore)
		return err
	}

	// process CRD resource first
	err = ctx.applyCRD()
	if err != nil {
		restore.Status.Phase = kahuapi.RestorePhaseFailed
		restore.Status.FailureReason = fmt.Sprintf("Failed to apply CRD resources. %s", err)
		restore, err = ctx.updateRestoreStatus(restore)
		return err
	}

	// process resources
	err = ctx.applyIndexedResource()
	if err != nil {
		restore.Status.Phase = kahuapi.RestorePhaseFailed
		restore.Status.FailureReason = fmt.Sprintf("Failed to apply resources. %s", err)
		restore, err = ctx.updateRestoreStatus(restore)
		return err
	}

	restore.Status.Phase = kahuapi.RestorePhaseCompleted
	restore, err = ctx.updateRestoreStatus(restore)
	return err
}

func (ctx *restoreContext) fetchBackup(restore *kahuapi.Restore) (*kahuapi.Backup, error) {
	// fetch backup
	backupName := restore.Spec.BackupName
	backup, err := ctx.backupLister.Get(backupName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			ctx.logger.Errorf("Backup(%s) do not exist", backupName)
			restore.Status.Phase = kahuapi.RestorePhaseFailed
			restore.Status.FailureReason = fmt.Sprintf("Backup(%s) not available", backupName)
			return nil, err
		}
		ctx.logger.Errorf("Failed to get backup. %s", err)
		return nil, err
	}

	return backup, err
}

func (ctx *restoreContext) fetchBackupLocation(locationName string,
	restore *kahuapi.Restore) (*kahuapi.BackupLocation, error) {
	// fetch backup location
	backupLocation, err := ctx.backupLocationLister.Get(locationName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			ctx.logger.Errorf("Backup location(%s) do not exist", locationName)
			restore.Status.Phase = kahuapi.RestorePhaseFailed
			restore.Status.FailureReason = fmt.Sprintf("Backup location(%s) not available",
				locationName)
			return nil, err
		}
		ctx.logger.Errorf("Failed to get backup location. %s", err)
		return nil, err
	}

	return backupLocation, err
}

func (ctx *restoreContext) fetchProvider(providerName string,
	restore *kahuapi.Restore) (*kahuapi.Provider, error) {
	// fetch provider
	provider, err := ctx.providerLister.Get(providerName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			ctx.logger.Errorf("Metadata Provider(%s) do not exist", providerName)
			restore.Status.Phase = kahuapi.RestorePhaseFailed
			restore.Status.FailureReason = fmt.Sprintf("Metadata Provider(%s) not available", providerName)
			return nil, err
		}
		ctx.logger.Errorf("Failed to get metadata provider. %s", err)
		return nil, err
	}

	return provider, nil
}

func (ctx *restoreContext) fetchBackupInfo(restore *kahuapi.Restore) (*backupInfo, error) {
	backup, err := ctx.fetchBackup(restore)
	if err != nil {
		return nil, err
	}

	backupLocation, err := ctx.fetchBackupLocation(backup.Spec.MetadataLocation, restore)
	if err != nil {
		return nil, err
	}

	provider, err := ctx.fetchProvider(backupLocation.Spec.ProviderName, restore)
	if err != nil {
		return nil, err
	}

	return &backupInfo{
		backup:         backup,
		backupLocation: backupLocation,
		backupProvider: provider,
	}, nil
}

func (ctx *restoreContext) fetchMetaServiceClient(backupProvider *kahuapi.Provider,
	restore *kahuapi.Restore) (metaservice.MetaServiceClient, error) {
	if backupProvider.Spec.Type != kahuapi.ProviderTypeMetadata {
		restore.Status.Phase = kahuapi.RestorePhaseFailed
		errMsg := fmt.Sprintf("Invalid metadata provider type (%s)",
			backupProvider.Spec.Type)
		restore.Status.FailureReason = errMsg
		return nil, fmt.Errorf(errMsg)
	}

	// fetch service name
	providerService, exist := backupProvider.Annotations[utils.BackupLocationServiceAnnotation]
	if !exist {
		restore.Status.Phase = kahuapi.RestorePhaseFailed
		errMsg := fmt.Sprintf("Failed to get metadata provider(%s) service info",
			backupProvider.Name)
		restore.Status.FailureReason = errMsg
		return nil, fmt.Errorf(errMsg)
	}

	metaServiceClient, err := metaservice.GetMetaServiceClient(providerService)
	if err != nil {
		restore.Status.Phase = kahuapi.RestorePhaseFailed
		errMsg := fmt.Sprintf("Failed to get metadata service client(%s)",
			providerService)
		restore.Status.FailureReason = errMsg
		return nil, fmt.Errorf(errMsg)
	}

	return metaServiceClient, nil
}

func (ctx *restoreContext) fetchBackupContent(backupProvider *kahuapi.Provider,
	backupIdentifier *metaservice.BackupIdentifier,
	restore *kahuapi.Restore) error {
	// fetch meta service client
	metaServiceClient, err := ctx.fetchMetaServiceClient(backupProvider, restore)
	if err != nil {
		if len(restore.Status.FailureReason) > 0 {
			restore, err = ctx.updateRestoreStatus(restore)
			return err
		}
		return err
	}

	// fetch metadata backup file
	restoreClient, err := metaServiceClient.Restore(context.TODO(), &metaservice.RestoreRequest{
		Id: backupIdentifier,
	})
	if err != nil {
		restore.Status.Phase = kahuapi.RestorePhaseFailed
		restore.Status.FailureReason = fmt.Sprintf("Failed to connect with metaservice. %s",
			err)
		restore, err = ctx.updateRestoreStatus(restore)
		return err
	}
	ctx.logger.Infof("restore client service %+v, %s", restoreClient, err)

	for {
		res, err := restoreClient.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Errorf("Failed fetching data. %s", err)
			break
		}

		obj := new(unstructured.Unstructured)
		err = json.Unmarshal(res.GetBackupResource().GetData(), obj)
		if err != nil {
			log.Errorf("Failed to unmarshal on backed up data %s", err)
			continue
		}

		if !excludedResources.Has(obj.GetKind()) {
			err = ctx.backupObjectIndexer.Add(obj)
			if err != nil {
				log.Errorf("Failed to index on backed up data %s", err)
				continue
			}
		}

	}
	restoreClient.CloseSend()

	return nil
}

func (ctx *restoreContext) applyCRD() error {
	crds, err := ctx.backupObjectIndexer.ByIndex(backupObjectResourceIndex, crdName)
	if err != nil {
		ctx.logger.Errorf("error fetching CRDs from indexer %s", err)
		return err
	}

	unstructuredCRDs := make([]*unstructured.Unstructured, 0)
	for _, crd := range crds {
		ubstructure, ok := crd.(*unstructured.Unstructured)
		if !ok {
			ctx.logger.Warningf("Restore index cache has invalid object type. %v",
				reflect.TypeOf(crd))
			continue
		}
		unstructuredCRDs = append(unstructuredCRDs, ubstructure)
	}

	for _, unstructuredCRD := range unstructuredCRDs {
		err := ctx.applyResource(unstructuredCRD)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ctx *restoreContext) applyIndexedResource() error {
	indexedResources := ctx.backupObjectIndexer.List()

	unstructuredResources := make([]*unstructured.Unstructured, 0)
	for _, indexedResource := range indexedResources {
		unstructuredResource, ok := indexedResource.(*unstructured.Unstructured)
		if !ok {
			ctx.logger.Warningf("Restore index cache has invalid object type. %v",
				reflect.TypeOf(unstructuredResource))
			continue
		}
		// ignore CRDs
		if unstructuredResource.GetObjectKind().GroupVersionKind().Kind == crdName {
			continue
		}
		unstructuredResources = append(unstructuredResources, unstructuredResource)
	}

	for _, unstructuredCRD := range unstructuredResources {
		err := ctx.applyResource(unstructuredCRD)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ctx *restoreContext) applyResource(resource *unstructured.Unstructured) error {
	gvk := resource.GroupVersionKind()
	gvr, _, err := ctx.discoveryHelper.ByGroupVersionKind(gvk)
	if err != nil {
		ctx.logger.Errorf("unable to fetch GroupVersionResource for %s", gvk)
		return err
	}

	resourceClient := ctx.dynamicClient.Resource(gvr).Namespace(resource.GetNamespace())
	ctx.preProcessResource(resource)
	_, err = resourceClient.Create(context.TODO(), resource, metav1.CreateOptions{})
	if err != nil && apierrors.IsAlreadyExists(err) {
		// ignore if already exist
		return nil
	}
	return err
}

func (ctx *restoreContext) preProcessResource(resource *unstructured.Unstructured) error {
	// remove resource version
	resource.SetResourceVersion("")

	// TODO(Amit Roushan): Add resource specific handling
	return nil
}
