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
	"regexp"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"

	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1"
	"github.com/soda-cdm/kahu/client/clientset/versioned"
	kahuscheme "github.com/soda-cdm/kahu/client/clientset/versioned/scheme"
	kahuv1client "github.com/soda-cdm/kahu/client/clientset/versioned/typed/kahu/v1"
	kahuinformer "github.com/soda-cdm/kahu/client/informers/externalversions"
	kahulister "github.com/soda-cdm/kahu/client/listers/kahu/v1"
	"github.com/soda-cdm/kahu/controllers"
	"github.com/soda-cdm/kahu/discovery"
	"github.com/soda-cdm/kahu/hooks"
	"github.com/soda-cdm/kahu/utils"
)

type controller struct {
	ctx                  context.Context
	logger               log.FieldLogger
	genericController    controllers.Controller
	kubeClient           kubernetes.Interface
	dynamicClient        dynamic.Interface
	backupClient         kahuv1client.BackupInterface
	backupLister         kahulister.BackupLister
	backupLocationLister kahulister.BackupLocationLister
	eventRecorder        record.EventRecorder
	discoveryHelper      discovery.DiscoveryHelper
	providerLister       kahulister.ProviderLister
	volumeBackupClient   kahuv1client.VolumeBackupContentInterface
	volumeBackupLister   kahulister.VolumeBackupContentLister
	hookExecutor         hooks.Hooks
}

func NewController(
	ctx context.Context,
	kubeClient kubernetes.Interface,
	kahuClient versioned.Interface,
	dynamicClient dynamic.Interface,
	informer kahuinformer.SharedInformerFactory,
	eventBroadcaster record.EventBroadcaster,
	discoveryHelper discovery.DiscoveryHelper,
	hookExecutor hooks.Hooks) (controllers.Controller, error) {

	logger := log.WithField("controller", controllerName)
	backupController := &controller{
		ctx:                  ctx,
		logger:               logger,
		kubeClient:           kubeClient,
		backupClient:         kahuClient.KahuV1().Backups(),
		backupLister:         informer.Kahu().V1().Backups().Lister(),
		backupLocationLister: informer.Kahu().V1().BackupLocations().Lister(),
		dynamicClient:        dynamicClient,
		discoveryHelper:      discoveryHelper,
		providerLister:       informer.Kahu().V1().Providers().Lister(),
		volumeBackupClient:   kahuClient.KahuV1().VolumeBackupContents(),
		volumeBackupLister:   informer.Kahu().V1().VolumeBackupContents().Lister(),
		hookExecutor:         hookExecutor,
	}

	// construct controller interface to process worker queue
	genericController, err := controllers.NewControllerBuilder(controllerName).
		SetLogger(logger).
		SetHandler(backupController.processQueue).
		Build()
	if err != nil {
		return nil, err
	}

	// register to informer to receive events and push events to worker queue
	informer.Kahu().
		V1().
		Backups().
		Informer().
		AddEventHandler(
			cache.ResourceEventHandlerFuncs{
				AddFunc: genericController.Enqueue,
				UpdateFunc: func(oldObj, newObj interface{}) {
					genericController.Enqueue(newObj)
				},
			},
		)

	// initialize event recorder
	eventRecorder := eventBroadcaster.NewRecorder(kahuscheme.Scheme,
		v1.EventSource{Component: controllerName})
	backupController.eventRecorder = eventRecorder

	// reference back
	backupController.genericController = genericController

	// start volume backup reconciler
	go newReconciler(defaultReconcileTimeLoop,
		backupController.logger.WithField("source", "reconciler"),
		informer.Kahu().V1().VolumeBackupContents().Lister(),
		backupController.backupClient,
		backupController.backupLister).Run(ctx.Done())

	return genericController, err
}

func (ctrl *controller) processQueue(key string) error {
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		ctrl.logger.Errorf("splitting key into namespace and name, error %s", err)
		return err
	}

	ctrl.logger.Infof("Processing backup(%s) request", name)
	backup, err := ctrl.backupLister.Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			ctrl.logger.Infof("Backup %s already deleted", name)
			return nil
		}
		// re enqueue for processing
		return errors.Wrap(err, fmt.Sprintf("error getting backup %s from lister", name))
	}

	// TODO (Amit Roushan): Add check for already processed backup
	backupClone := backup.DeepCopy()
	if backupClone.DeletionTimestamp != nil {
		return ctrl.deleteBackup(backupClone)
	}

	// setup finalizer if not present
	if isBackupInitNeeded(backupClone) {
		backupClone, err = ctrl.backupInitialize(backupClone)
		if err != nil {
			ctrl.logger.Errorf("failed to initialize finalizer backup(%s)", backupClone.Name)
			return err
		}
	}

	return ctrl.syncBackup(backupClone)
}

func (ctrl *controller) deleteBackup(backup *kahuapi.Backup) error {
	ctrl.logger.Infof("Initiating backup(%s) delete", backup.Name)

	err := ctrl.removeVolumeBackup(backup)
	if err != nil {
		ctrl.logger.Errorf("Unable to delete volume backup. %s", err)
		return err
	}

	// check if all volume backup contents are deleted
	vbsList, err := ctrl.volumeBackupLister.List(
		labels.Set{volumeContentBackupLabel: backup.Name}.AsSelector())
	if err != nil {
		ctrl.logger.Errorf("Unable to get volume backup list. %s", err)
		return err
	}
	if len(vbsList) > 0 {
		ctrl.logger.Errorf("Volume backup list is not empty. Continue to wait for Volume backup delete")
		return nil
	}

	ctrl.logger.Info("Volume backup deleted successfully")

	err = ctrl.deleteMetadataBackup(backup)
	if err != nil {
		ctrl.logger.Errorf("Unable to delete meta backup. %s", err)
		return err
	}

	backupUpdate := backup.DeepCopy()
	utils.RemoveFinalizer(backupUpdate, backupFinalizer)
	_, err = ctrl.patchBackup(backup, backupUpdate)
	if err != nil {
		ctrl.logger.Errorf("removing finalizer failed for %s", backup.Name)
	}
	return err
}

func (ctrl *controller) syncBackup(backup *kahuapi.Backup) error {
	if backup.Status.State == kahuapi.BackupStateDeleting {
		return nil
	}

	ctrl.logger.Infof("Validating backup(%s) specifications", backup.Name)
	err := ctrl.validateBackup(backup)
	if err != nil {
		return err
	}

	ctrl.logger.Infof("Backup validation successful")
	return ctrl.syncVolumeBackup(backup)
}

func (ctrl *controller) syncVolumeBackup(
	backup *kahuapi.Backup) (err error) {

	ctrl.logger.Infof("DEBUG: Backup: syncVolumeBackup backup stage %s", backup.Status.Stage)
	ctrl.logger.Infof("DEBUG: Backup: syncVolumeBackup backup status %s", backup.Status.State)

	var backupContext Context
	switch backup.Status.Stage {
	case kahuapi.BackupStageInitial:
		ctrl.logger.Infof("DEBUG: Backup: --BackupStageInitial--  %s", backup.Name)
		// preprocess backup spec and try to get all backup resources
		backupContext = newContext(backup, ctrl)
		err = backupContext.Complete()
		if err != nil {
			ctrl.logger.Errorf("Unable to filter resources. %s", err)
			backup.Status.State = kahuapi.BackupStateFailed
			backup.Status.ValidationErrors = append(backup.Status.ValidationErrors, fmt.Sprintf("%v", err))
			ctrl.updateStatus(backup, ctrl.backupClient, backup.Status)
			return err
		}
		// update state
		backup.Status.State = kahuapi.BackupStateProcessing
		backup.Status.ValidationErrors = []string{}
		ctrl.updateStatus(backup, ctrl.backupClient, backup.Status)

		ctrl.logger.Infof("DEBUG: Backup: created new backup context")
		// sync resources with backup.Status.Resources
		backup, err = backupContext.SyncResources(backup)
		if err != nil {
			ctrl.logger.Errorf("Update backup(%s) processing: failed to "+
				"sync backup resources for volume backup", backup.Name)
			backup.Status.State = kahuapi.BackupStateFailed
			backup.Status.ValidationErrors = append(backup.Status.ValidationErrors, fmt.Sprintf("%v", err))
			ctrl.updateStatus(backup, ctrl.backupClient, backup.Status)
			return err
		}
		if ctrl.hookExecutor.IsHooksSpecified(backup.Spec.Hook.Resources, hooks.PreHookPhase) {
			ctrl.logger.Infof("DEBUG: Backup: PRE hooks is specified for  %s", backup.Name)
			backup, err = ctrl.updateBackupStatusWithEvent(backup,
				kahuapi.BackupStatus{
					Stage: kahuapi.BackupStagePreHook,
				},
				v1.EventTypeNormal,
				string(kahuapi.BackupStagePreHook),
				"Starting prehook execution")
			if err != nil {
				ctrl.logger.Errorf("Update backup(%s) processing: failed to "+
					"update Prehook stage for volume backup", backup.Name)
				return err
			}
			ctrl.logger.Infof("DEBUG: Backup: updated backup status for prehook  %s", backup.Name)
		} else {
			backup, err = ctrl.updateBackupStatusWithEvent(backup,
				kahuapi.BackupStatus{
					Stage: kahuapi.BackupStageVolumes,
				},
				v1.EventTypeNormal,
				string(kahuapi.BackupStageVolumes),
				"Starting backup volumes")
			if err != nil {
				ctrl.logger.Infof("DEBUG: Backup: Update status for volumeback failed@  %s", backup.Name)
				return err
			}
		}
	case kahuapi.BackupStagePreHook:
		ctrl.logger.Infof("DEBUG: Backup: --BackupStagePreHook--  %s", backup.Name)
		// Execute pre hooks
		err = ctrl.hookExecutor.ExecuteHook(&backup.Spec.Hook, hooks.PreHookPhase)
		if err != nil {
			ctrl.logger.Infof("DEBUG: Backup: updated backup status for prehook  %s", backup.Name)
			ctrl.logger.Errorf("Update backup(%s) processing: failed to "+
				"process Prehook for volume backup", backup.Name)
			backup.Status.State = kahuapi.BackupStateFailed
			backup.Status.ValidationErrors = append(backup.Status.ValidationErrors, fmt.Sprintf("%v", err))
			ctrl.updateStatus(backup, ctrl.backupClient, backup.Status)
			return nil
		}
		backup, err = ctrl.updateBackupStatusWithEvent(backup,
			kahuapi.BackupStatus{
				Stage: kahuapi.BackupStageVolumes,
			},
			v1.EventTypeNormal,
			string(kahuapi.BackupStageVolumes),
			"Starting backup volumes")
		if err != nil {
			ctrl.logger.Infof("DEBUG: Backup: Update status for volumeback failed@  %s", backup.Name)
			return err
		}

	case kahuapi.BackupStageVolumes:
		ctrl.logger.Infof("DEBUG: Backup: --BackupStageVolumes--  %s", backup.Name)
		// preprocess backup spec and try to get all backup resources
		backupContext = newContext(backup, ctrl)
		err = backupContext.Complete()
		if err != nil {
			ctrl.logger.Errorf("Unable to filter resources. %s", err)
			backup.Status.State = kahuapi.BackupStateFailed
			backup.Status.ValidationErrors = append(backup.Status.ValidationErrors, fmt.Sprintf("%v", err))
			ctrl.updateStatus(backup, ctrl.backupClient, backup.Status)
			return err
		}

		ctrl.logger.Infof("DEBUG: Backup: created new backup context")
		// sync resources with backup.Status.Resources
		backup, err = backupContext.SyncResources(backup)
		if err != nil {
			ctrl.logger.Errorf("Update backup(%s) processing: failed to "+
				"sync backup resources for volume backup", backup.Name)
			backup.Status.State = kahuapi.BackupStateFailed
			backup.Status.ValidationErrors = append(backup.Status.ValidationErrors, fmt.Sprintf("%v", err))
			ctrl.updateStatus(backup, ctrl.backupClient, backup.Status)
			return err
		}
		backup, err = ctrl.processVolumeBackup(backup, backupContext)
		if err != nil {
			backup.Status.State = kahuapi.BackupStateFailed
			backup.Status.ValidationErrors = append(backup.Status.ValidationErrors, fmt.Sprintf("%v", err))
			ctrl.updateStatus(backup, ctrl.backupClient, backup.Status)
			return err
		}
		// populate all meta service
		backup, err = ctrl.updateBackupStatusWithEvent(backup,
			kahuapi.BackupStatus{
				Stage: kahuapi.BackupStageResources,
			},
			v1.EventTypeNormal,
			"VolumeBackupScheduled",
			"Volume backup Scheduled")
		if err != nil {
			ctrl.logger.Infof("DEBUG: Backup: Update status for resourcesbk failed@  %s", backup.Name)
			return err
		}

	case kahuapi.BackupStageResources:
		ctrl.logger.Infof("DEBUG: Backup: --BackupStageResources--  %s", backup.Name)
		if !metav1.HasAnnotation(backup.ObjectMeta, annVolumeBackupCompleted) {
			ctrl.logger.Infof("DEBUG: Backup: no annotation return  %s", backup.Name)
			return nil
		}
		// add volume backup content in resource backup list
		err := ctrl.syncResourceBackup(backup)
		if err != nil {
			backup.Status.State = kahuapi.BackupStateFailed
			backup.Status.ValidationErrors = append(backup.Status.ValidationErrors, fmt.Sprintf("%v", err))
			ctrl.updateStatus(backup, ctrl.backupClient, backup.Status)
			return nil
		}
		if ctrl.hookExecutor.IsHooksSpecified(backup.Spec.Hook.Resources, hooks.PreHookPhase) {
			backup, err = ctrl.updateBackupStatusWithEvent(backup, kahuapi.BackupStatus{
				Stage: kahuapi.BackupStagePostHook,
			}, v1.EventTypeNormal, string(kahuapi.BackupStagePostHook), "Starting to execute post hook")
			if err != nil {
				ctrl.logger.Infof("DEBUG: Backup: Update status for post hook failed@  %s", backup.Name)
				return err
			}
		} else {
			backup, err = ctrl.updateBackupStatusWithEvent(backup,
				kahuapi.BackupStatus{
					Stage: kahuapi.BackupStageFinished,
				},
				v1.EventTypeNormal,
				string(kahuapi.BackupStageResources),
				"Metdata backup success")
			if err != nil {
				ctrl.logger.Infof("DEBUG: Backup: Update status for backup failed@  %s", backup.Name)
				return err
			}
		}
	case kahuapi.BackupStagePostHook:
		ctrl.logger.Infof("DEBUG: Backup: --BackupStagePostHook--  %s", backup.Name)
		// Execute post hooks
		err = ctrl.hookExecutor.ExecuteHook(&backup.Spec.Hook, hooks.PostHookPhase)
		if err != nil {
			ctrl.logger.Errorf("failed to Execute post hooks: %s", err.Error())
			backup.Status.State = kahuapi.BackupStateFailed
			backup.Status.ValidationErrors = append(backup.Status.ValidationErrors, fmt.Sprintf("%v", err))
			ctrl.updateStatus(backup, ctrl.backupClient, backup.Status)
		}
		backup, err = ctrl.updateBackupStatusWithEvent(backup,
			kahuapi.BackupStatus{
				Stage: kahuapi.BackupStageFinished,
			},
			v1.EventTypeNormal,
			string(kahuapi.BackupStageResources),
			"Metdata backup success")
		if err != nil {
			ctrl.logger.Infof("DEBUG: Backup: Update status for backup failed@  %s", backup.Name)
			return err
		}
	case kahuapi.BackupStageFinished:
		ctrl.logger.Infof("DEBUG: Backup: --BackupStageFinished--  %s", backup.Name)
		if backup.Status.State == kahuapi.BackupStateCompleted {
			return nil
		}
		backup.Status.LastBackup = &metav1.Time{Time: time.Now()}
		time := metav1.Now()
		backup.Status.CompletionTimestamp = &time
		backup.Status.State = kahuapi.BackupStateCompleted

		ctrl.logger.Infof("completed backup with status: %s", backup.Status.Stage)
		ctrl.updateStatus(backup, ctrl.backupClient, backup.Status)
		ctrl.logger.Infof("DEBUG: Backup: BACKUP of all completed for  %s ----", backup.Name)
	default:
		ctrl.logger.Infof("DEBUG: Backup: --default--  %s", backup.Name)
	}

	// // check if volume backup required
	// if metav1.HasAnnotation(backup.ObjectMeta, annVolumeBackupCompleted) {
	// 	ctrl.logger.Infof("DEBUG: Backup: YES COMPLETED annotations!!! %s", backup.Name)
	// 	backup, err = ctrl.updateBackupStatusWithEvent(backup,
	// 		kahuapi.BackupStatus{Stage: kahuapi.BackupStageResources},
	// 		v1.EventTypeNormal, "VolumeBackupSuccess", "Resource backup starting")
	// 	if err != nil {
	// 		ctrl.logger.Errorf("Unable to update backup status to resources1. %s", err)
	// 		return err
	// 	}
	// 	ctrl.logger.Infof("DEBUG: Backup: volume backup completed for  %s", backup.Name)
	// 	// if backup.Status.Stage == kahuapi.BackupStageFinished &&
	// 	// 	backup.Status.State == kahuapi.BackupStateCompleted {
	// 	// 	ctrl.logger.Infof("Backup is finished already")
	// 	// 	return nil
	// 	// }

	// 	// add volume backup content in resource backup list
	// 	err := ctrl.syncResourceBackup(backup)
	// 	if err != nil {
	// 		backup.Status.State = kahuapi.BackupStateFailed
	// 		backup.Status.ValidationErrors = append(backup.Status.ValidationErrors, fmt.Sprintf("%v", err))
	// 		ctrl.updateStatus(backup, ctrl.backupClient, backup.Status)
	// 		return nil
	// 	}

	// 	ctrl.logger.Infof("DEBUG: Backup: Resource backup completed for  %s", backup.Name)

	// 	if ctrl.hookExecutor.IsHooksSpecified(backup.Spec.Hook.Resources, hooks.PreHookPhase) {
	// 		// Execute post hooks
	// 		err = ctrl.hookExecutor.ExecuteHook(&backup.Spec.Hook, hooks.PostHookPhase)
	// 		if err != nil {
	// 			ctrl.logger.Errorf("failed to Execute post hooks: %s", err.Error())
	// 			backup.Status.State = kahuapi.BackupStateFailed
	// 			backup.Status.ValidationErrors = append(backup.Status.ValidationErrors, fmt.Sprintf("%v", err))
	// 			ctrl.updateStatus(backup, ctrl.backupClient, backup.Status)
	// 		}
	// 	}
	// 	ctrl.logger.Infof("DEBUG: Backup: Posthook for backup completed for  %s", backup.Name)
	// 	// populate all meta service status
	// 	backup, err = ctrl.updateBackupStatusWithEvent(backup,
	// 		kahuapi.BackupStatus{
	// 			Stage: kahuapi.BackupStageFinished,
	// 		},
	// 		v1.EventTypeNormal,
	// 		string(kahuapi.BackupStageResources),
	// 		"Metdata backup success")
	// 	if err != nil {
	// 		ctrl.logger.Infof("DEBUG: Backup: Update status for backup failed@  %s", backup.Name)
	// 		return err
	// 	}
	// }

	// ctrl.logger.Infof("DEBUG: Backup: No COMPLETED annotations!!! %s", backup.Name)

	// // backup, err = ctrl.updateBackupStatusWithEvent(backup,
	// // 	kahuapi.BackupStatus{
	// // 		Stage: kahuapi.BackupStageInitial,
	// // 	},
	// // 	v1.EventTypeNormal,
	// // 	string(kahuapi.BackupStageInitial),
	// // 	"Starting backup")
	// // if err != nil {
	// // 	ctrl.logger.Errorf("Update backup(%s) processing: failed to "+
	// // 	"update initial stage for volume backup", backup.Name)
	// // 	return err
	// // }

	// ctrl.logger.Infof("DEBUG: Backup:synced Resources to context %s", backup.Name)

	// if ctrl.hookExecutor.IsHooksSpecified(backup.Spec.Hook.Resources, hooks.PreHookPhase) {

	// }
	// ctrl.logger.Infof("DEBUG: Backup: updated backup status for prehook  %s", backup.Name)

	return nil
}

func (ctrl *controller) syncResourceBackup(
	backup *kahuapi.Backup) (err error) {
	err = ctrl.processMetadataBackup(backup)
	if err != nil {
		return err
	}
	return err
}

func (ctrl *controller) validateBackup(backup *kahuapi.Backup) error {
	var validationErrors []string
	// namespace validation
	includeNamespaces := sets.NewString(backup.Spec.IncludeNamespaces...)
	excludeNamespaces := sets.NewString(backup.Spec.ExcludeNamespaces...)
	// check common namespace name in include/exclude list
	if intersection := includeNamespaces.Intersection(excludeNamespaces); intersection.Len() > 0 {
		validationErrors = append(validationErrors,
			fmt.Sprintf("common namespace name (%s) in include and exclude namespace list",
				strings.Join(intersection.List(), ",")))
	}

	// resource validation
	// include resource validation
	for _, resourceSpec := range backup.Spec.IncludeResources {
		if _, err := regexp.Compile(resourceSpec.Name); err != nil {
			validationErrors = append(validationErrors,
				fmt.Sprintf("invalid include resource expression (%s)", resourceSpec.Name))
		}
	}

	// exclude resource validation
	for _, resourceSpec := range backup.Spec.ExcludeResources {
		if _, err := regexp.Compile(resourceSpec.Name); err != nil {
			validationErrors = append(validationErrors,
				fmt.Sprintf("invalid exclude resource expression (%s)", resourceSpec.Name))
		}
	}

	if len(validationErrors) == 0 {
		return nil
	}

	ctrl.logger.Errorf("Backup validation failed. %s", strings.Join(validationErrors, ", "))
	_, err := ctrl.updateBackupStatusWithEvent(backup, kahuapi.BackupStatus{
		State:            kahuapi.BackupStateFailed,
		ValidationErrors: validationErrors,
	}, v1.EventTypeWarning, string(kahuapi.BackupStateFailed),
		fmt.Sprintf("Backup validation failed. %s", strings.Join(validationErrors, ", ")))

	return errors.Wrap(err, "backup validation failed")
}

func isBackupInitNeeded(backup *kahuapi.Backup) bool {
	if !utils.ContainsFinalizer(backup, backupFinalizer) ||
		backup.Status.Stage == "" ||
		backup.Status.Stage == kahuapi.BackupStageInitial {
		return true
	}

	return false
}

func (ctrl *controller) backupInitialize(backup *kahuapi.Backup) (*kahuapi.Backup, error) {
	var err error
	backupClone := backup.DeepCopy()

	if !utils.ContainsFinalizer(backup, backupFinalizer) {
		utils.SetFinalizer(backupClone, backupFinalizer)
		backupClone, err = ctrl.patchBackup(backup, backupClone)
		if err != nil {
			ctrl.logger.Errorf("Unable to update finalizer for backup(%s)", backup.Name)
			return backup, errors.Wrap(err, "Unable to update finalizer")
		}
	}

	if backupClone.Status.Stage == "" {
		backupClone.Status.Stage = kahuapi.BackupStageInitial
	}
	if backup.Status.StartTimestamp == nil {
		time := metav1.Now()
		backupClone.Status.StartTimestamp = &time
	}
	return ctrl.updateBackupStatus(backupClone, backupClone.Status)
}

func (ctrl *controller) patchBackup(oldBackup, newBackup *kahuapi.Backup) (*kahuapi.Backup, error) {
	origBytes, err := json.Marshal(oldBackup)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling original backup")
	}

	updatedBytes, err := json.Marshal(newBackup)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling updated backup")
	}

	patchBytes, err := jsonpatch.CreateMergePatch(origBytes, updatedBytes)
	if err != nil {
		return nil, errors.Wrap(err, "error creating json merge patch for backup")
	}

	updatedBackup, err := ctrl.backupClient.Patch(context.TODO(),
		oldBackup.Name,
		types.MergePatchType,
		patchBytes,
		metav1.PatchOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error patching backup")
	}

	return updatedBackup, nil
}

func (ctrl *controller) updateBackupStatus(
	backup *kahuapi.Backup,
	status kahuapi.BackupStatus) (*kahuapi.Backup, error) {
	var err error

	backupClone := backup.DeepCopy()
	dirty := false
	// update Phase
	if status.Stage != "" && toIota(backup.Status.Stage) < toIota(status.Stage) {
		backupClone.Status.Stage = status.Stage
		ctrl.logger.Infof("DEBUG: stage less :%+v -> %+v", backup.Status.State, status.Stage)
		dirty = true
	}

	if status.State != "" && backup.Status.State != status.State {
		backupClone.Status.State = status.State
		ctrl.logger.Infof("DEBUG: stage not nil :%+v -> %+v", backup.Status.State, status.Stage)
		dirty = true
	}

	// update Validation error
	if len(status.ValidationErrors) > 0 {
		backupClone.Status.ValidationErrors = append(backupClone.Status.ValidationErrors,
			status.ValidationErrors...)
		ctrl.logger.Infof("DEBUG: val errors :%+v", status.Stage)
		dirty = true
	}

	// update Start time
	if backup.Status.StartTimestamp == nil {
		backupClone.Status.StartTimestamp = status.StartTimestamp
		ctrl.logger.Infof("DEBUG: ts not nil :%+v", status.Stage)
		dirty = true
	}

	if backup.Status.LastBackup == nil &&
		status.LastBackup != nil {
		backupClone.Status.LastBackup = status.LastBackup
		ctrl.logger.Infof("DEBUG: last backup is nil :%+v", status.Stage)
		dirty = true
	}

	if len(backupClone.Status.Resources) == 0 {
		backupClone.Status.Resources = append(backupClone.Status.Resources,
			status.Resources...)
		dirty = true
		ctrl.logger.Infof("DEBUG: res len is nil :%+v", status.Stage)
	}

	if dirty {
		backupClone, err = ctrl.backupClient.UpdateStatus(context.TODO(), backupClone, metav1.UpdateOptions{})
		if err != nil {
			ctrl.logger.Errorf("updating backup(%s) status: update status failed %s", backup.Name, err)
		}
	}

	return backupClone, err
}

func (ctrl *controller) updateBackupStatusWithEvent(
	backup *kahuapi.Backup,
	status kahuapi.BackupStatus,
	eventType, reason, message string) (*kahuapi.Backup, error) {

	newBackup, err := ctrl.updateBackupStatus(backup, status)
	if err != nil {
		return newBackup, err
	}

	if newBackup.ResourceVersion != backup.ResourceVersion {
		ctrl.logger.Infof("backup %s changed phase to %q: %s", backup.Name, status.Stage, message)
		ctrl.eventRecorder.Event(newBackup, eventType, reason, message)
	}
	return newBackup, err
}

func (ctrl *controller) updateStatus(bkp *kahuapi.Backup, client kahuv1client.BackupInterface, status kahuapi.BackupStatus) {
	backup, err := client.Get(context.Background(), bkp.Name, metav1.GetOptions{})
	if err != nil {
		ctrl.logger.Errorf("failed to get backup for updating status :%+s", err)
		return
	}

	if backup.Status.Stage == kahuapi.BackupStageFinished {
		// no need to update as backup completed
		return
	}

	if status.State != "" && status.State != backup.Status.State {
		backup.Status.State = status.State
	}

	if status.Stage != "" && status.Stage != backup.Status.Stage {
		backup.Status.Stage = status.Stage
	}

	if len(status.ValidationErrors) > 0 {
		backup.Status.ValidationErrors = status.ValidationErrors
	}

	if backup.Status.StartTimestamp == nil && status.StartTimestamp != nil {
		backup.Status.StartTimestamp = status.StartTimestamp
	}

	if backup.Status.LastBackup == nil && status.LastBackup != nil {
		backup.Status.LastBackup = status.LastBackup
	}

	_, err = client.UpdateStatus(context.Background(), backup, metav1.UpdateOptions{})
	if err != nil {
		ctrl.logger.Errorf("failed to update backup status :%+s", err)
	}

	return
}

// addTypeInformationToObject adds TypeMeta information to a runtime.Object based upon the loaded scheme.Scheme
// inspired by: https://github.com/kubernetes/cli-runtime/blob/v0.19.2/pkg/printers/typesetter.go#L41
func addTypeInformationToObject(obj runtime.Object) (schema.GroupVersionKind, error) {
	gvks, _, err := scheme.Scheme.ObjectKinds(obj)
	if err != nil {
		return schema.GroupVersionKind{}, fmt.Errorf("missing apiVersion or kind and cannot assign it; %w", err)
	}

	for _, gvk := range gvks {
		if len(gvk.Kind) == 0 {
			continue
		}
		if len(gvk.Version) == 0 || gvk.Version == runtime.APIVersionInternal {
			continue
		}

		obj.GetObjectKind().SetGroupVersionKind(gvk)
		return gvk, nil
	}

	return schema.GroupVersionKind{}, err
}
