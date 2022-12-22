///*
//Copyright 2022 The SODA Authors.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
//*/
//
package v1beta1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +genclient
// +genclient:nonNamespaced
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:printcolumn:name="VolumeProvider",type=string,JSONPath=`.spec.snapshotProvider`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

type Snapshot struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +optional
	Spec SnapshotSpec `json:"spec,omitempty" protobuf:"bytes,3,opt,name=spec"`

	// +optional
	Status SnapshotStatus `json:"status,omitempty" protobuf:"bytes,4,opt,name=status"`
}

type SnapshotSpec struct {
	BackupName *string `json:"backup"`

	SnapshotProvider *string `json:"snapshotProvider"`

	VolumeSource `json:",inline" protobuf:"bytes,2,opt,name=volumes"`

	// Supported Snapshot backup provider information
	// +optional
	Parameters map[string]string `json:"parameters,omitempty"`
}

// +kubebuilder:validation:Enum=New;InProgress;Completed;Failed;Deleting
type SnapshotPhase string

const (
	SnapshotPhaseInit       SnapshotPhase = "New"
	SnapshotPhaseInProgress SnapshotPhase = "InProgress"
	SnapshotPhaseCompleted  SnapshotPhase = "Completed"
	SnapshotPhaseFailed     SnapshotPhase = "Failed"
	SnapshotPhaseDeleting   SnapshotPhase = "Deleting"
)

type SnapshotState struct {
	PVC ResourceObjectReference `json:"volume,omitempty"`

	SnapshotSource `json:"snapshotSource,omitempty"`

	Completed bool `json:"progress,omitempty"`
}

type SnapshotSource struct {
	CSISnapshotRef *ResourceObjectReference `json:"csiSnapshotRef,omitempty"`
}

// SnapshotStatus defines the observed state of Snapshot
type SnapshotStatus struct {
	// +optional
	Phase SnapshotPhase `json:"phase,omitempty"`

	ReadyToUse *bool `json:"readyToUse,omitempty"`
	// +optional
	FailureReason string `json:"failureReason,omitempty"`

	// +optional
	SnapshotStates []SnapshotState `json:"snapshotStates,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SnapshotList contains a list of Snapshot
type SnapshotList struct {
	metav1.TypeMeta `json:",inline"`

	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Snapshot `json:"items"`
}
