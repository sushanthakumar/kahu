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

	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	metaservice "github.com/soda-cdm/kahu/providerframework/metaservice/lib/go"
)

func (c *controller) GetDeploymentAndBackup(gvr GroupResouceVersion, name, namespace string,
	backupClient metaservice.MetaService_BackupClient) error {
	k8sClinet, err := kubernetes.NewForConfig(c.restClientconfig)
	if err != nil {
		c.logger.Errorf("Unable to get k8sclient %s", err)
		return err
	}

	deployment, err := k8sClinet.AppsV1().Deployments(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	resourceData, err := json.Marshal(deployment)
	if err != nil {
		c.logger.Errorf("Unable to get resource content of deployment:%s: %s", name, err)
		return err
	}

	err = c.backupSend(gvr, resourceData, deployment.Name, backupClient)
	if err != nil {
		return err
	}
	return nil

}

func (c *controller) deploymentBackup(gvr GroupResouceVersion, namespace string,
	backup *PrepareBackup, backupClient metaservice.MetaService_BackupClient) error {

	k8sClinet, err := kubernetes.NewForConfig(c.restClientconfig)
	if err != nil {
		c.logger.Errorf("Unable to get k8sclient %s", err)
		return err
	}

	var labelSelectors map[string]string
	if backup.Spec.Label != nil {
		labelSelectors = backup.Spec.Label.MatchLabels
	}

	selectors := labels.Set(labelSelectors).String()
	dList, err := k8sClinet.AppsV1().Deployments(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: selectors,
	})
	if err != nil {
		return err
	}

	for _, deployment := range dList.Items {
		err = c.GetDeploymentAndBackup(gvr, deployment.Name, deployment.Namespace, backupClient)
		if err != nil {
			return err
		}
		err = c.GetConfigMapUsedInDeployment(gvr, deployment, backupClient)
		if err != nil {
			return err
		}
		err = c.GetServiceAccountUsedInDeployment(gvr, deployment, backupClient)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *controller) GetConfigMapUsedInDeployment(gvr GroupResouceVersion, deployment v1.Deployment,
	backupClient metaservice.MetaService_BackupClient) error {
	for _, v := range deployment.Spec.Template.Spec.Volumes {
		if v.ConfigMap != nil {
			configMap, err := c.GetConfigMap(deployment.Namespace, v.ConfigMap.Name)
			if err != nil {
				c.logger.Errorf("unable to get configmap for name: %s", v.ConfigMap.Name)
			}
			resourceData, err := json.Marshal(configMap)
			if err != nil {
				c.logger.Errorf("Unable to get resource content of pod: %s", err)
				return err
			}
			c.logger.Debug(resourceData)
			gvr.group = ""
			gvr.resourceName = "configs"
			gvr.version = "v1"

			c.backupSend(gvr, resourceData, deployment.Name, backupClient)
		}
	}
	return nil

}

func (c *controller) GetServiceAccountUsedInDeployment(gvr GroupResouceVersion, deployment v1.Deployment,
	backupClient metaservice.MetaService_BackupClient) error {
	saName := deployment.Spec.Template.Spec.ServiceAccountName

	sa, err := c.GetServiceAccount(deployment.Namespace, saName)
	if err != nil {
		c.logger.Errorf("unable to get configmap for name: %s", saName)
	}
	resourceData, err := json.Marshal(sa)
	if err != nil {
		c.logger.Errorf("Unable to get resource content of Service Account: %s", err)
		return err
	}
	c.logger.Debug(resourceData)
	gvr.group = ""
	gvr.resourceName = "serviceaccounts"
	gvr.version = "v1"

	c.backupSend(gvr, resourceData, deployment.Name, backupClient)

	return nil
}

func (c *controller) GetServiceAccount(namespace, name string) (*corev1.ServiceAccount, error) {

	k8sClinet, err := kubernetes.NewForConfig(c.restClientconfig)
	if err != nil {
		c.logger.Errorf("Unable to get k8sclient %s", err)
		return nil, err
	}

	sa, err := k8sClinet.CoreV1().ServiceAccounts(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return sa, err
}

func (c *controller) getConfigMapS(gvr GroupResouceVersion, namespace string, backup *PrepareBackup,
	backupClient metaservice.MetaService_BackupClient) error {

	k8sClinet, err := kubernetes.NewForConfig(c.restClientconfig)
	if err != nil {
		c.logger.Errorf("Unable to get k8sclient %s", err)
		return err
	}

	var labelSelectors map[string]string
	if backup.Spec.Label != nil {
		labelSelectors = backup.Spec.Label.MatchLabels
	}

	selectors := labels.Set(labelSelectors).String()
	configList, err := k8sClinet.CoreV1().ConfigMaps(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: selectors,
	})
	if err != nil {
		return err
	}

	for _, configMap := range configList.Items {
		config_data, err := c.GetConfigMap(namespace, configMap.Name)

		resourceData, err := json.Marshal(config_data)
		if err != nil {
			c.logger.Errorf("Unable to get resource content of configmaps: %s", err)
			return err
		}
		c.logger.Debug(resourceData)

		err = c.backupSend(gvr, resourceData, config_data.Name, backupClient)
		if err != nil {
			return err
		}

	}
	return nil
}

func (c *controller) GetConfigMap(namespace, name string) (*corev1.ConfigMap, error) {

	k8sClinet, err := kubernetes.NewForConfig(c.restClientconfig)
	if err != nil {
		c.logger.Errorf("Unable to get k8sclient %s", err)
		return nil, err
	}

	configmap, err := k8sClinet.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return configmap, err
}

func (c *controller) ListNamespaces(backup *PrepareBackup) ([]string, error) {
	k8sClinet, err := kubernetes.NewForConfig(c.restClientconfig)
	if err != nil {
		c.logger.Errorf("Unable to get k8sclient %s", err)
		return nil, err
	}

	var namespaceList []string
	namespaces, err := k8sClinet.CoreV1().Namespaces().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return namespaceList, err
	}
	for _, ns := range namespaces.Items {
		namespaceList = append(namespaceList, ns.Name)

	}
	return namespaceList, nil
}

func (c *controller) getPersistentVolumeClaims(gvr GroupResouceVersion, namespace string, backup *PrepareBackup,
	backupClient metaservice.MetaService_BackupClient) error {

	c.logger.Infoln("starting collecting persistentvolumeclaims")
	k8sClinet, err := kubernetes.NewForConfig(c.restClientconfig)
	if err != nil {
		c.logger.Errorf("Unable to get k8sclient %s", err)
		return err
	}

	var labelSelectors map[string]string
	if backup.Spec.Label != nil {
		labelSelectors = backup.Spec.Label.MatchLabels
	}

	selectors := labels.Set(labelSelectors).String()
	allPVC, err := k8sClinet.CoreV1().PersistentVolumeClaims(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: selectors,
	})
	if err != nil {
		return err
	}

	c.logger.Infof("the all pvc:%+v", allPVC)
	for _, pvc := range allPVC.Items {
		pvcData, err := c.GetPVC(namespace, pvc.Name)

		resourceData, err := json.Marshal(pvcData)
		if err != nil {
			c.logger.Errorf("Unable to get resource content of pvc: %s", err)
			return err
		}
		c.logger.Debug(resourceData)
		gvr := GroupResouceVersion{
			group:        "",
			resourceName: "persistentvolumeclaims",
			version:      "v1",
		}

		err = c.backupSend(gvr, resourceData, pvcData.Name, backupClient)
		if err != nil {
			return err
		}

	}
	return nil
}

func (c *controller) GetPVC(namespace, name string) (*corev1.PersistentVolumeClaim, error) {

	k8sClinet, err := kubernetes.NewForConfig(c.restClientconfig)
	if err != nil {
		c.logger.Errorf("Unable to get k8sclient %s", err)
		return nil, err
	}

	pvc, err := k8sClinet.CoreV1().PersistentVolumeClaims(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return pvc, err
}

func (c *controller) getStorageClass(gvr GroupResouceVersion, backup *PrepareBackup,
	backupClient metaservice.MetaService_BackupClient) error {

	k8sClinet, err := kubernetes.NewForConfig(c.restClientconfig)
	if err != nil {
		c.logger.Errorf("Unable to get k8sclient %s", err)
		return err
	}

	var labelSelectors map[string]string
	if backup.Spec.Label != nil {
		labelSelectors = backup.Spec.Label.MatchLabels
	}

	selectors := labels.Set(labelSelectors).String()
	allSC, err := k8sClinet.StorageV1().StorageClasses().List(context.TODO(), metav1.ListOptions{
		LabelSelector: selectors,
	})
	if err != nil {
		return err
	}

	for _, pvc := range allSC.Items {
		scData, err := c.GetSC(pvc.Name)

		resourceData, err := json.Marshal(scData)
		if err != nil {
			c.logger.Errorf("Unable to get resource content of pvc: %s", err)
			return err
		}
		c.logger.Debug(resourceData)

		err = c.backupSend(gvr, resourceData, scData.Name, backupClient)
		if err != nil {
			return err
		}

	}
	return nil
}

func (c *controller) GetSC(name string) (*storagev1.StorageClass, error) {

	k8sClinet, err := kubernetes.NewForConfig(c.restClientconfig)
	if err != nil {
		c.logger.Errorf("Unable to get k8sclient %s", err)
		return nil, err
	}

	sc, err := k8sClinet.StorageV1().StorageClasses().Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return sc, err
}
