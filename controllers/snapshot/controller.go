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

package snapshot

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/util/sets"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	grm "k8s.io/kubernetes/pkg/util/goroutinemap"

	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	"github.com/soda-cdm/kahu/client/clientset/versioned"
	kahuscheme "github.com/soda-cdm/kahu/client/clientset/versioned/scheme"
	kahuinformer "github.com/soda-cdm/kahu/client/informers/externalversions"
	kahulister "github.com/soda-cdm/kahu/client/listers/kahu/v1beta1"
	"github.com/soda-cdm/kahu/controllers"
	"github.com/soda-cdm/kahu/controllers/snapshot/classsyncer"
	"github.com/soda-cdm/kahu/controllers/snapshot/csi"
	"github.com/soda-cdm/kahu/discovery"
	"github.com/soda-cdm/kahu/utils"
)

const (
	controllerName           = "snapshot-controller"
	snapshotFinalizer        = "kahu.io/snapshot-protection"
	defaultReconcileTimeLoop = 5 * time.Second
	defaultReSyncTimeLoop    = 30 * time.Minute
)

var snapshotSupport = sets.NewString("local.csi.openebs.io")

type controller struct {
	ctx                    context.Context
	logger                 log.FieldLogger
	genericController      controllers.Controller
	kubeClient             kubernetes.Interface
	kahuClient             versioned.Interface
	snapshotLister         kahulister.SnapshotLister
	dynamicClient          dynamic.Interface
	eventRecorder          record.EventRecorder
	discoveryHelper        discovery.DiscoveryHelper
	processedSnapshot      utils.Store
	volSnapshotClassSyncer classsyncer.Interface
	csiSnapshotter         csi.Snapshoter
	csiSnapshotHandler     grm.GoRoutineMap
}

func NewController(
	ctx context.Context,
	kubeClient kubernetes.Interface,
	kahuClient versioned.Interface,
	dynamicClient dynamic.Interface,
	informer kahuinformer.SharedInformerFactory,
	eventBroadcaster record.EventBroadcaster,
	discoveryHelper discovery.DiscoveryHelper,
	volSnapshotClassSyncer classsyncer.Interface,
	csiSnapshotter csi.Snapshoter) (controllers.Controller, error) {

	logger := log.WithField("controller", controllerName)
	processedCache := utils.NewStore(utils.DeletionHandlingMetaNamespaceKeyFunc)

	snapshotController := &controller{
		ctx:                    ctx,
		logger:                 logger,
		kahuClient:             kahuClient,
		kubeClient:             kubeClient,
		dynamicClient:          dynamicClient,
		discoveryHelper:        discoveryHelper,
		processedSnapshot:      processedCache,
		volSnapshotClassSyncer: volSnapshotClassSyncer,
		csiSnapshotter:         csiSnapshotter,
		snapshotLister:         informer.Kahu().V1beta1().Snapshots().Lister(),
		csiSnapshotHandler:     grm.NewGoRoutineMap(false),
	}

	// construct controller interface to process worker queue
	genericController, err := controllers.NewControllerBuilder(controllerName).
		SetLogger(logger).
		SetHandler(snapshotController.processQueue).
		SetReSyncHandler(snapshotController.reSync).
		SetReSyncPeriod(defaultReSyncTimeLoop).
		Build()
	if err != nil {
		return nil, err
	}

	// register to informer to receive events and push events to worker queue
	informer.Kahu().
		V1beta1().
		Snapshots().
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
	snapshotController.eventRecorder = eventRecorder

	// reference back
	snapshotController.genericController = genericController

	return genericController, err
}

func (ctrl *controller) reSync() {
	ctrl.logger.Info("Running soft reconciliation for snapshots")
	snapshots, err := ctrl.snapshotLister.List(labels.Everything())
	if err != nil {
		// re enqueue for processing
		ctrl.logger.Errorf("Unable to get snapshot list for re sync. %s", err)
		return
	}

	// enqueue all snapshot for soft reconciliation
	for _, snapshot := range snapshots {
		ctrl.genericController.Enqueue(snapshot)
	}
}

func (ctrl *controller) processQueue(key string) error {
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		ctrl.logger.Errorf("splitting key into namespace and name, error %s", err)
		return err
	}

	ctrl.logger.Infof("Processing snapshot(%s) request", name)
	snapshot, err := ctrl.snapshotLister.Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			ctrl.logger.Infof("Snapshot %s already deleted", name)
			return nil
		}
		// re enqueue for processing
		return errors.Wrap(err, fmt.Sprintf("error getting snapshot %s from lister", name))
	}

	newObj, err := utils.StoreRevisionUpdate(ctrl.processedSnapshot, snapshot, "Snapshot")
	if err != nil {
		ctrl.logger.Errorf("%s", err)
	}
	if !newObj {
		return nil
	}

	if readyToUse(snapshot.Status.ReadyToUse) {
		ctrl.logger.Infof("Snapshot %s is ready to use", snapshot.Name)
		return nil
	}

	// Identify volumes for snapshot
	snapshot, err = ctrl.syncSnapshotVolumes(snapshot)
	if err != nil {
		return err
	}

	// check volume snapshot support for CSI
	if supportCSISnapshot(snapshot) {
		return ctrl.handleCSISnapshot(snapshot)
	}

	return ctrl.handleSnapshot(snapshot)
}

func readyToUse(status *bool) bool {
	return status != nil && *status == true
}

func (ctrl *controller) syncSnapshotVolumes(snapshot *kahuapi.Snapshot) (*kahuapi.Snapshot, error) {
	// update volume info from Spec
	volumes := snapshot.Spec.List
	snapshotState := make([]kahuapi.SnapshotState, 0)
	for _, pvc := range volumes {
		snapshotState = append(snapshotState, kahuapi.SnapshotState{
			PVC: pvc,
		})
	}

	snapshot.Status.SnapshotStates = snapshotState

	return ctrl.kahuClient.KahuV1beta1().Snapshots().UpdateStatus(context.TODO(), snapshot, metav1.UpdateOptions{})
}

func supportCSISnapshot(snapshot *kahuapi.Snapshot) bool {
	// currently only support CSI volumes
	// TODO: Device a way to identify Snapshot support by Volume Provider
	if snapshot.Spec.SnapshotProvider == nil {
		return false
	}
	if snapshotSupport.Has(*snapshot.Spec.SnapshotProvider) {
		return true
	}
	return false
}

func (ctrl *controller) handleCSISnapshot(snapshot *kahuapi.Snapshot) error {
	csiSnapshotHandler := func(snapshot *kahuapi.Snapshot) error {
		err := ctrl.csiSnapshotHandler.Run(snapshot.Name, func() error {
			return ctrl.csiSnapshotter.Handle(snapshot)
		})
		if grm.IsAlreadyExists(err) {
			ctrl.logger.Info("CSI Snapshotting %s already getting handled ", snapshot.Name)
			return nil
		}

		readyToUse := true
		if err != nil {
			readyToUse = false
		}

		kahuVolSnapshot, err := ctrl.kahuClient.KahuV1beta1().
			Snapshots().
			Get(context.TODO(), snapshot.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		kahuVolSnapshot.Status.ReadyToUse = &readyToUse
		_, err = ctrl.kahuClient.KahuV1beta1().
			Snapshots().
			UpdateStatus(context.TODO(), kahuVolSnapshot, metav1.UpdateOptions{})
		return err
	}

	return csiSnapshotHandler(snapshot)
}

func (ctrl *controller) handleSnapshot(snapshot *kahuapi.Snapshot) error {
	ctrl.logger.Info("Waiting for external snapshot controller to handle volume snapshot")
	ctrl.eventRecorder.Event(snapshot, v1.EventTypeNormal, "ExternalSnapshotHandling",
		"Waiting for external snapshot controller to handle volume snapshot")
	return nil
}
