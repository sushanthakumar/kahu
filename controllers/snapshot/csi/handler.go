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

package csi

import (
	"context"
	"time"

	snapshotapiv1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	snapshotclientset "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned/typed/volumesnapshot/v1"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	clientset "github.com/soda-cdm/kahu/client/clientset/versioned"
	"github.com/soda-cdm/kahu/controllers/snapshot/classsyncer"
)

const (
	csiSnapshotTimeout = 5 * time.Minute
)

type Snapshoter interface {
	Handle(snapshot *kahuapi.Snapshot) error
}

type snapshoter struct {
	logger                 log.FieldLogger
	kubeClient             kubernetes.Interface
	kahuClient             clientset.Interface
	snapshotClient         snapshotv1.SnapshotV1Interface
	volSnapshotClassSyncer classsyncer.Interface
}

func NewSnapshotter(restConfig *rest.Config,
	kubeClient kubernetes.Interface,
	kahuClient clientset.Interface,
	volSnapshotClassSyncer classsyncer.Interface) (Snapshoter, error) {
	client, err := snapshotclientset.NewForConfig(restConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &snapshoter{
		kubeClient:             kubeClient,
		kahuClient:             kahuClient,
		volSnapshotClassSyncer: volSnapshotClassSyncer,
		snapshotClient:         client.SnapshotV1(),
		logger:                 log.WithField("module", "csi-snapshotter"),
	}, nil
}

func (s *snapshoter) Handle(snapshot *kahuapi.Snapshot) error {
	// create snapshot for each snapshot volumes
	csiSnapshotClass, err := s.volSnapshotClassSyncer.SnapshotClassByProvider(*snapshot.Spec.SnapshotProvider)
	if err != nil {
		return err
	}

	for _, snapshotState := range snapshot.Status.SnapshotStates {
		// create CSI object
		if err := s.applySnapshot(snapshot.Name, csiSnapshotClass.Name, snapshotState.PVC); err != nil {
			return err
		}
	}

	return s.waitForSnapshotToReady(snapshot.Name, csiSnapshotTimeout)
}

func (s *snapshoter) applySnapshot(kahuVolSnapshotName string,
	snapshotClassName string,
	pvc kahuapi.ResourceObjectReference) error {
	csiSnapshot, err := s.snapshotClient.VolumeSnapshots(pvc.Namespace).Create(context.TODO(),
		&snapshotapiv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvc.Name + "-" + string(uuid.NewUUID()),
				Namespace: pvc.Namespace,
			},
			Spec: snapshotapiv1.VolumeSnapshotSpec{
				Source: snapshotapiv1.VolumeSnapshotSource{
					PersistentVolumeClaimName: &pvc.Name,
				},
				VolumeSnapshotClassName: &snapshotClassName,
			}}, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	kahuVolSnapshot, err := s.kahuClient.KahuV1beta1().Snapshots().Get(context.TODO(), kahuVolSnapshotName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	for i, snapshotState := range kahuVolSnapshot.Status.SnapshotStates {
		if snapshotState.PVC.Name == pvc.Name &&
			snapshotState.PVC.Namespace == pvc.Namespace {
			kahuVolSnapshot.Status.SnapshotStates[i].CSISnapshotRef = &kahuapi.ResourceObjectReference{
				Namespace: csiSnapshot.Namespace,
				Name:      csiSnapshot.Name,
			}
		}
	}

	_, err = s.kahuClient.KahuV1beta1().Snapshots().UpdateStatus(context.TODO(), kahuVolSnapshot, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	return nil
}

func (s *snapshoter) waitForSnapshotToReady(snapshotID string, timeout time.Duration) error {
	s.logger.Infof("Probing for Snapshot Status [id=%v]", snapshotID)

	verifyStatus := func() (bool, error) {
		snapshot, err := s.kahuClient.KahuV1beta1().Snapshots().Get(context.TODO(),
			snapshotID, metav1.GetOptions{})
		if err != nil {
			// Ignore "not found" errors in case the Snapshot was just created and hasn't yet made it into the lister.
			if !apierrors.IsNotFound(err) {
				s.logger.Errorf("unexpected error waiting for snapshot, %v", err)
				return false, err
			}

			// The Snapshot is not available yet and we will have to try again.
			return false, nil
		}

		successful, err := s.verifySnapshotStatus(snapshot)
		if err != nil {
			return false, err
		}
		return successful, nil
	}

	return s.waitForSnapshotStatus(snapshotID, timeout, verifyStatus, "Create")
}

func readyToUse(status *bool) bool {
	return status != nil && *status == true
}

func (s *snapshoter) verifySnapshotStatus(snapshot *kahuapi.Snapshot) (bool, error) {
	// if being deleted, fail fast
	if snapshot.GetDeletionTimestamp() != nil {
		s.logger.Errorf("Snapshot [%s] has deletion timestamp, will not continue to wait for volume snapshot",
			snapshot.UID)
		return false, errors.New("snapshot is being deleted")
	}

	// attachment OK
	if readyToUse(snapshot.Status.ReadyToUse) {
		return true, nil
	}

	// driver reports attach error
	failureMsg := snapshot.Status.FailureReason
	if len(failureMsg) != 0 {
		s.logger.Errorf("Snapshot for %v failed: %v", snapshot.UID, failureMsg)
		return false, errors.New(failureMsg)
	}

	// check CSI snapshot, if all CSI Snapshot Ready to use return success
	for _, snapshotState := range snapshot.Status.SnapshotStates {
		if snapshotState.CSISnapshotRef == nil {
			continue
		}

		if !s.checkCSISnapshotStatus(snapshotState.CSISnapshotRef) {
			return false, nil
		}
	}

	return true, nil
}

func (s *snapshoter) checkCSISnapshotStatus(csiSnapshot *kahuapi.ResourceObjectReference) bool {
	snapshot, err := s.snapshotClient.VolumeSnapshots(csiSnapshot.Namespace).Get(context.TODO(),
		csiSnapshot.Name, metav1.GetOptions{})
	if err != nil {
		return false
	}

	return readyToUse(snapshot.Status.ReadyToUse)
}

func (s *snapshoter) waitForSnapshotStatus(snapshotID string,
	timeout time.Duration,
	verifyStatus func() (bool, error),
	operation string) error {
	var (
		initBackoff = 500 * time.Millisecond
		// This is approximately the duration between consecutive ticks after two minutes (CSI timeout).
		maxBackoff    = 7 * time.Second
		resetDuration = time.Minute
		backoffFactor = 1.05
		jitter        = 0.1
		clock         = &clock.RealClock{}
	)
	backoffMgr := wait.NewExponentialBackoffManager(initBackoff,
		maxBackoff,
		resetDuration,
		backoffFactor,
		jitter,
		clock)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		t := backoffMgr.Backoff()
		select {
		case <-t.C():
			successful, err := verifyStatus()
			if err != nil {
				return err
			}
			if successful {
				return nil
			}
		case <-ctx.Done():
			t.Stop()
			s.logger.Errorf("%s timeout for snapshot after %v [snapshotID=%s]",
				operation, timeout, snapshotID)
			return errors.New("timed out waiting for external-snapshotting")
		}
	}
}
