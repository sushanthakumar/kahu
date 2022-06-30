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

package identity

import (
	"context"

	log "github.com/sirupsen/logrus"

	"google.golang.org/grpc"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1beta1 "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	kahuClient "github.com/soda-cdm/kahu/client"
)

const (
	// MetaService component name
	agentBaseName = "kahu-provider-identity"
)

// registerProvider creates CRD entry on behalf of the provider getting added.
func registerProvider(ctx context.Context, conn *grpc.ClientConnInterface, providerType apiv1beta1.ProviderType) error {
	providerInfo, err := GetProviderInfo(ctx, conn)
	if err != nil {
		return err
	}

	log.Info("Starting Server providerInfo", providerInfo)

	providerCapabilities, err := GetProviderCapabilities(ctx, conn)
	if err != nil {
		return err
	}

	log.Info("Starting Server GetProviderCapabilities", providerCapabilities)

	err = createProviderCR(providerInfo, providerType, providerCapabilities)
	return err

}

// createProviderCR creates CRD entry on behalf of the provider getting added.
func createProviderCR(providerInfo ProviderInfo, providerType apiv1beta1.ProviderType, providerCapabilities map[string]bool) error {
	cfg := kahuClient.NewFactoryConfig()
	log.Info("createProviderCR cfg", cfg)

	clientFactory := kahuClient.NewFactory(agentBaseName, cfg)
	log.Info("createProviderCR clientFactory", clientFactory)
	client, err := clientFactory.KahuClient()
	if err != nil {
		return err
	}

	log.Info("createProviderCR before Get, client", client)
	// Create provider CRD as it is not found and update the status
	_, err = client.KahuV1beta1().Providers().Get(context.TODO(), providerInfo.provider, metav1.GetOptions{})
	if err != nil {
		log.Info("createProviderCR Providers Get ", err)
		provider := &apiv1beta1.Provider{
			ObjectMeta: metav1.ObjectMeta{
				Name: providerInfo.provider,
			},
			Spec: apiv1beta1.ProviderSpec{
				Version:      providerInfo.version,
				Type:         providerType,
				Manifest:     providerInfo.manifest,
				Capabilities: providerCapabilities,
			},
		}

		provider, err = client.KahuV1beta1().Providers().Create(context.TODO(), provider, metav1.CreateOptions{})
		log.Info("createProviderCR Providers Create ", err)
		if err != nil {
			return err
		}

		provider.Status.State = apiv1beta1.ProviderStateAvailable

		provider, err = client.KahuV1beta1().Providers().UpdateStatus(context.TODO(), provider, metav1.UpdateOptions{})
		log.Info("createProviderCR Providers UpdateStatus ", provider, err)
		if err != nil {
			return err
		}
	}

	return nil
}
