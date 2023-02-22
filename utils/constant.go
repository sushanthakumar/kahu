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

package utils

const (
	BackupLocationServiceAnnotation = "kahu.io/provider-service"
	NamespaceAnnotation             = "kahu.io/provider-namespace"
	InsecureAnnotation              = "kahu.io/provider-insecure-grpc-connection"
	NamespaceEnv                    = "NAMESPACE"
)

const (
	Pod                = "Pod"
	Service            = "Service"
	Deployment         = "Deployment"
	Replicaset         = "ReplicaSet"
	StatefulSet        = "StatefulSet"
	DaemonSet          = "DaemonSet"
	Configmap          = "ConfigMap"
	Secret             = "Secret"
	PVC                = "PersistentVolumeClaim"
	PV                 = "PersistentVolume"
	Endpoint           = "Endpoint"
	SC                 = "StorageClass"
	Backup             = "Backup"
	VBC                = "VolumeBackupContent"
	KahuSnapshot       = "VolumeSnapshot"
	BackupLocation     = "BackupLocation"
	Node               = "Node"
	Event              = "Event"
	VolumeSnapshot     = "VolumeSnapshot"
	ClusterRole        = "ClusterRole"
	ClusterRoleBinding = "ClusterRoleBinding"
	Role               = "Role"
	RoleBinding        = "RoleBinding"
	ServiceAccount     = "ServiceAccount"
	EndpointSlice      = "EndpointSlice"

	AnnBackupLocationParam     = "kahu.io/backup-location-parameter"
	AnnProviderRegistrationUID = "uid.provider.registration.kahu.io"
	AnnLegacyService           = "kahu.io/provider-service"
)

var SupportedResourceList = []string{Pod, Service, Deployment, Replicaset, StatefulSet,
	DaemonSet, Configmap, Secret, PVC, PV, Endpoint, SC,
}

var SupportedCsiDrivers = []string{""}
