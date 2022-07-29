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
	"fmt"
	jsonpatch "github.com/evanphx/json-patch"
	metaservice "github.com/soda-cdm/kahu/providerframework/metaservice/lib/go"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1"
)

func getVBCName() string {
	return fmt.Sprintf("vbc-%s", uuid.New().String())
}

func (ctrl *controller) processVolumeBackup(backup *kahuapi.Backup, ctx Context) (*kahuapi.Backup, error) {
	ctrl.logger.Infof("Processing Volume backup(%s)", backup.Name)

	pvs, err := ctrl.getVolumes(backup, ctx)
	if err != nil {
		return backup, err
	}

	if len(pvs) == 0 {
		ctrl.logger.Infof("No volume for backup. " +
			"Setting stage to volume backup completed")
		// set annotation for
		backup, err = ctrl.annotateBackup(annVolumeBackupCompleted, backup)
		return backup, err
	}

	// group pv with providers
	pvProviderMap := make(map[string][]corev1.PersistentVolume, 0)
	for _, pv := range pvs {
		pvList, ok := pvProviderMap[pv.Spec.CSI.Driver]
		if !ok {
			pvList = make([]corev1.PersistentVolume, 0)
		}
		pvList = append(pvList, pv)
		pvProviderMap[pv.Spec.CSI.Driver] = pvList
	}

	// ensure volume backup content
	return backup, ctrl.ensureVolumeBackupContent(backup.Name, pvProviderMap)
}

func (ctrl *controller) removeVolumeBackup(
	backup *kahuapi.Backup) error {
	vbcList, err := ctrl.volumeBackupClient.List(context.TODO(), metav1.ListOptions{
		LabelSelector: labels.Set{
			volumeContentBackupLabel: backup.Name,
		}.String(),
	})
	if err != nil {
		ctrl.logger.Errorf("Unable to get volume backup content list %s", err)
		return errors.Wrap(err, "Unable to get volume backup content list")
	}

	for _, vbc := range vbcList.Items {
		if vbc.DeletionTimestamp != nil { // ignore deleting volume backup content
			continue
		}
		err := ctrl.volumeBackupClient.Delete(context.TODO(), vbc.Name, metav1.DeleteOptions{})
		if err != nil {
			ctrl.logger.Errorf("Failed to delete volume backup content %s", err)
			return errors.Wrap(err, "Unable to delete volume backup content")
		}
	}

	return nil
}

func (ctrl *controller) getVolumes(
	backup *kahuapi.Backup,
	ctx Context) ([]corev1.PersistentVolume, error) {
	// retrieve all persistent volumes for backup
	ctrl.logger.Infof("Getting PersistentVolume for backup(%s)", backup.Name)

	// check if volume backup content already available
	unstructuredPVCs := ctx.GetKindResources(PVCKind)
	unstructuredPVs := ctx.GetKindResources(PVKind)
	pvs := make([]corev1.PersistentVolume, 0)
	// list all PVs
	pvNames := sets.NewString()
	for _, unstructuredPV := range unstructuredPVs {
		var pv corev1.PersistentVolume
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredPV.Object, &pv)
		if err != nil {
			ctrl.logger.Errorf("Failed to translate unstructured (%s) to "+
				"pv. %s", unstructuredPV.GetName(), err)
			return pvs, err
		}
		pvs = append(pvs, pv)
		pvNames.Insert(pv.Name)
	}

	for _, unstructuredPVC := range unstructuredPVCs {
		var pvc corev1.PersistentVolumeClaim
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredPVC.Object, &pvc)
		if err != nil {
			ctrl.logger.Warningf("Failed to translate unstructured (%s) to "+
				"pvc. %s", unstructuredPVC.GetName(), err)
			return pvs, err
		}

		if len(pvc.Spec.VolumeName) == 0 ||
			pvc.DeletionTimestamp != nil {
			// ignore unbound PV
			continue
		}
		k8sPV, err := ctrl.kubeClient.CoreV1().PersistentVolumes().Get(context.TODO(),
			pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			ctrl.logger.Errorf("unable to get PV %s", pvc.Spec.VolumeName)
			return pvs, err
		}

		if k8sPV.Spec.CSI == nil || // ignore if not CSI
			pvNames.Has(k8sPV.Name) { // ignore if all-ready considered
			// ignoring non CSI Volumes
			continue
		}

		var pv corev1.PersistentVolume
		k8sPV.DeepCopyInto(&pv)
		pvs = append(pvs, pv)
		pvNames.Insert(pv.Name)
	}

	return pvs, nil
}

func (ctrl *controller) ensureVolumeBackupContent(
	backupName string,
	pvProviderMap map[string][]corev1.PersistentVolume) error {
	// ensure volume backup content
	for provider, pvList := range pvProviderMap {
		// check if volume content already available
		// backup name and provider name is unique tuple for volume backup content
		vbcList, err := ctrl.volumeBackupClient.List(context.TODO(), metav1.ListOptions{
			LabelSelector: labels.Set{
				volumeContentBackupLabel:    backupName,
				volumeContentVolumeProvider: provider,
			}.String(),
		})
		if err != nil {
			ctrl.logger.Errorf("Unable to get volume backup content list %s", err)
			return errors.Wrap(err, "Unable to get volume backup content list")
		}
		if len(vbcList.Items) > 0 {
			continue
		}
		time := metav1.Now()
		volumeBackupContent := &kahuapi.VolumeBackupContent{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					volumeContentBackupLabel:    backupName,
					volumeContentVolumeProvider: provider,
				},
				Name: getVBCName(),
			},
			Spec: kahuapi.VolumeBackupContentSpec{
				BackupName:     backupName,
				Volumes:        pvList,
				VolumeProvider: &provider,
			},
			Status: kahuapi.VolumeBackupContentStatus{
				Phase:          kahuapi.VolumeBackupContentPhaseInit,
				StartTimestamp: &time,
			},
		}

		_, err = ctrl.volumeBackupClient.Create(context.TODO(), volumeBackupContent, metav1.CreateOptions{})
		if err != nil {
			ctrl.logger.Errorf("unable to create volume backup content "+
				"for provider %s", provider)
			return errors.Wrap(err, "unable to create volume backup content")
		}
	}

	return nil
}

func (ctrl *controller) backupVolumeBackupContent(
	backup *PrepareBackup,
	backupClient metaservice.MetaService_BackupClient) error {
	ctrl.logger.Infoln("Starting collecting volume backup content")

	selectors := labels.Set(map[string]string{
		volumeContentBackupLabel: backup.Name,
	}).AsSelector()

	list, err := ctrl.volumeBackupLister.List(selectors)
	if err != nil {
		return err
	}
	for _, volumeBackupContent := range list {
		// ctrl.discoveryHelper.ByGroupVersionKind()
		resourceData, err := json.Marshal(volumeBackupContent)
		if err != nil {
			ctrl.logger.Errorf("Unable to get resource content: %s", err)
			return err
		}

		ctrl.logger.Infof("sending metadata for object %s/%s", volumeBackupContent.APIVersion,
			volumeBackupContent.Name)

		err = backupClient.Send(&metaservice.BackupRequest{
			Backup: &metaservice.BackupRequest_BackupResource{
				BackupResource: &metaservice.BackupResource{
					Resource: &metaservice.Resource{
						Name:    volumeBackupContent.Name,
						Group:   volumeBackupContent.APIVersion,
						Version: volumeBackupContent.APIVersion,
						Kind:    volumeBackupContent.Kind,
					},
					Data: resourceData,
				},
			},
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (ctrl *controller) annotateBackup(
	annotation string,
	backup *kahuapi.Backup) (*kahuapi.Backup, error) {
	backupName := backup.Name
	ctrl.logger.Infof("Annotating backup(%s) with %s", backupName, annotation)

	_, ok := backup.Annotations[annotation]
	if ok {
		ctrl.logger.Infof("Backup(%s) all-ready annotated with %s", backupName, annotation)
		return backup, nil
	}

	backupClone := backup.DeepCopy()
	metav1.SetMetaDataAnnotation(&backupClone.ObjectMeta, annotation, "true")

	origBytes, err := json.Marshal(backup)
	if err != nil {
		return backup, errors.Wrap(err, "error marshalling backup")
	}

	updatedBytes, err := json.Marshal(backupClone)
	if err != nil {
		return backup, errors.Wrap(err, "error marshalling updated backup")
	}

	patchBytes, err := jsonpatch.CreateMergePatch(origBytes, updatedBytes)
	if err != nil {
		return backup, errors.Wrap(err, "error creating json merge patch for backup")
	}

	backup, err = ctrl.backupClient.Patch(context.TODO(), backupName,
		types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		ctrl.logger.Error("Unable to update backup(%s) for volume completeness. %s",
			backupName, err)
		errors.Wrap(err, "error annotating volume backup completeness")
	}

	return backup, nil
}
