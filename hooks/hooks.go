/*
Copyright 2022 The SODA Authors.
Copyright 2020 the Velero contributors.

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

// Package hooks implement pre and post hook execution
package hooks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1"
	"github.com/soda-cdm/kahu/utils"
)

const (
	defaultTimeout = 5 * time.Minute
	PreHookPhase   = "pre"
	PostHookPhase  = "post"
	hookResource   = utils.Pod
)

const (
	podHookContainerAnnotationKey = "hook.kahu.io/container"
	podHookCommandAnnotationKey   = "hook.kahu.io/command"
	podHookOnErrorAnnotationKey   = "hook.kahu.io/on-error"
	podHookTimeoutAnnotationKey   = "hook.kahu.io/timeout"
)


type Hooks interface {
	ExecuteHook(hookSpec *kahuapi.HookSpec, phase string) error
}

type hooksHandler struct {
	logger  			log.FieldLogger
	restConfig 			*restclient.Config
	kubeClient 			kubernetes.Interface
}

// NewHooks creates hooks exection handler
func NewHooks(kubeClient kubernetes.Interface, restConfig *restclient.Config) (Hooks, error) {
	logger := log.WithField("hooks", "create")

	h := &hooksHandler{
		logger: logger,
		restConfig: restConfig,
		kubeClient: kubeClient,
	}

	h.logger.Info("initialization of hooks complete")
	return h, nil
}

// ExecuteHook will handle executions of backup hooks
func (h *hooksHandler) ExecuteHook(hookSpec *kahuapi.HookSpec, phase string) error {
	h.logger = log.WithField("hook-phase", phase)
		
	if !h.IsHooksSpecified(hookSpec.Resources, phase) {
		h.logger.Infof("No hooks specified %+v", hookSpec.Resources)
		// no hooks to handle
		return nil
	}

	namespaces, err := h.kubeClient.CoreV1().Namespaces().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		h.logger.Errorf("unable to list namespaces for hooks %s", err.Error())
		return err
	}
	allNamespaces := sets.NewString()
	for _, namespace := range namespaces.Items {
		allNamespaces.Insert(namespace.Name)
	}

	// For each hook
	for _, resources := range hookSpec.Resources {
		filteredHookNamespaces := filterHookNamespaces(allNamespaces, resources)
			for _, namespace := range filteredHookNamespaces.UnsortedList() {
			if phase == PreHookPhase && len(resources.PreHooks) == 0 {
				continue
			}
			if phase == PostHookPhase && len(resources.PostHooks) == 0 {
				continue
			}

			// Get label selector
			var labelSelectors map[string]string
			if resources.LabelSelector != nil {
				labelSelectors = resources.LabelSelector.MatchLabels
			}
			pods, err := h.kubeClient.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
				LabelSelector: labels.Set(labelSelectors).String(),
			})
			if err != nil {
				h.logger.Errorf("unable to list pod for namespace %s", namespace)
				return err
			}

			// Filter pods for backup
			var allPods []string
			for _, pod := range pods.Items {
				allPods = append(allPods, pod.Name)
			}

			filteredPods := utils.FindMatchedStrings(utils.Pod,
				allPods,
				resources.IncludeResources,
				resources.ExcludeResources)
			for _, pod := range filteredPods {
				err := h.executeHook(resources, namespace, pod, phase)
				if err != nil {
					h.logger.Errorf("failed to execute hook on pod %s, err %s", pod, err.Error())
					return err
				}
			}
		}
	}
	h.logger.Infof("%s hook execution is success!", phase)
	return nil
}

func filterHookNamespaces(allNamespaces sets.String , resourceHook kahuapi.ResourceHookSpec) sets.String {
	// Filter namespaces for hook
	hooksNsIncludes := sets.NewString()
	hooksNsExcludes := sets.NewString()
	hooksNsIncludes.Insert(resourceHook.IncludeNamespaces...)
	hooksNsExcludes.Insert(resourceHook.ExcludeNamespaces...)
	filteredHookNs := filterIncludesExcludes(allNamespaces,
		hooksNsIncludes.UnsortedList(),
		hooksNsExcludes.UnsortedList())
	return filteredHookNs
}

func filterIncludesExcludes(rawItems sets.String, includes, excludes[]string) sets.String {
	// perform include/exclude item on cache
	excludeItems := sets.NewString()

	// process include item
	includeItems := sets.NewString(includes...)
	if len(includeItems) != 0 {
		for _, item := range rawItems.UnsortedList() {
			// if available item are not included exclude them
			if !includeItems.Has(item) {
				excludeItems.Insert(item)
			}
		}
		for _, item := range includeItems.UnsortedList() {
			if !rawItems.Has(item) {
				excludeItems.Insert(item)
			}
		}
	} else {
		// if not specified include all namespaces
		includeItems = rawItems
	}
	excludeItems.Insert(excludes...)

	for _, excludeItem := range excludeItems.UnsortedList() {
		includeItems.Delete(excludeItem)
	}
	return includeItems
}

// isHooksSpecified is helper for early exit
func (h *hooksHandler) IsHooksSpecified(resources []kahuapi.ResourceHookSpec, phase string) bool {
	if phase == PreHookPhase {
		for _, resourceHook := range resources {
			if len(resourceHook.PreHooks) > 0 {
				return true
			} 
		}
	}
	if phase == PostHookPhase {
		for _, resourceHook := range resources {
			if len(resourceHook.PostHooks) > 0 {
				return true
			} 
		}
	}
	return false
}

// ExecuteHook executes the hooks in a container
func (h *hooksHandler) executeHook(
	resourceHook kahuapi.ResourceHookSpec,
	namespace string,
	name string,
	stage string,
) error {
	pod, err := h.kubeClient.CoreV1().Pods(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		h.logger.Errorf("Unable to get pod object for name %s", pod)
		return err
	}

	// Handle hooks from annotations
	hookExec := getHooksSpecFromAnnotations(pod.GetAnnotations(), stage)
	if hookExec != nil {
		err := h.executePodCommand(pod, namespace, name, "annotationHook", hookExec)
		if err != nil {
			h.logger.Errorf("error %s, while executing annotation hook", err.Error())
			if hookExec.OnError == kahuapi.HookErrorModeFail {
				return err
			}
		}
		// Skip normal hooks
		return nil
	}

	labels := labels.Set(pod.GetLabels())
	if !h.validateHook(resourceHook, name, namespace, labels) {
		h.logger.Infof("validation for hook %s failed for pod (%s)", resourceHook.Name, pod.Name)
		// continue
		return nil
	}

	hooks := resourceHook.PreHooks
	if stage == PostHookPhase {
		hooks = resourceHook.PostHooks
	}
	for _, hook := range hooks {
		if hook.Exec != nil {
			err := h.executePodCommand(pod, namespace, name, resourceHook.Name, hook.Exec)
			if err != nil {
				h.logger.Errorf("hook failed on %s (%s) with %s", pod.Name, resourceHook.Name, err.Error())
				if hook.Exec.OnError == kahuapi.HookErrorModeFail {
					return err
				}
			}
		}
	}

	return nil
}

func getHooksSpecFromAnnotations(annotations map[string]string, stage string) *kahuapi.ExecHook {
	commands := annotations[fmt.Sprintf("%v.%v", stage, podHookCommandAnnotationKey)]
	if commands == "" {
		return nil
	}
	var command []string

	if commands[0] == '[' {
		if err := json.Unmarshal([]byte(commands), &command); err != nil {
			command = []string{commands}
		}
	} else {
		command = append(command, commands)
	}
	container := annotations[fmt.Sprintf("%v.%v", stage, podHookContainerAnnotationKey)]
	onError := kahuapi.HookErrorMode(annotations[fmt.Sprintf("%v.%v", stage, podHookOnErrorAnnotationKey)])
	if onError != kahuapi.HookErrorModeContinue && onError != kahuapi.HookErrorModeFail {
		onError = ""
	}
	timeout := annotations[fmt.Sprintf("%v.%v", stage, podHookTimeoutAnnotationKey)]
	var duration time.Duration
	if timeout != "" {
		if temp, err := time.ParseDuration(timeout); err == nil {
			duration = temp
		} else {
			log.Warnf("Unable to parse provided timeout %s, using default", timeout)
		}
	}
	execSpec := kahuapi.ExecHook{
		Command:   command,
		Container: container,
		OnError:   onError,
		Timeout:   metav1.Duration{Duration: duration},
	}

	return &execSpec
}



func (h *hooksHandler) executePodCommand(pod *v1.Pod, namespace, name, hookName string, hook *kahuapi.ExecHook) error {
	if pod == nil {
		return errors.New("pod is required")
	}
	if namespace == "" {
		return errors.New("namespace is required")
	}
	if name == "" {
		return errors.New("name is required")
	}
	if hookName == "" {
		return errors.New("hookName is required")
	}
	if hook == nil {
		return errors.New("hook is required")
	}

	localHook := *hook
	if localHook.Container == "" {
		if err := setDefaultHookContainer(pod, &localHook); err != nil {
			return err
		}
	} else if err := ensureContainerExists(pod, localHook.Container); err != nil {
		// no container skipping hook execution
		log.Warnf("unable to find container %s, in pod %s", localHook.Container, pod.Name)
		return err
	}

	if len(localHook.Command) == 0 {
		return errors.New("command is required")
	}

	switch localHook.OnError {
	case kahuapi.HookErrorModeFail, kahuapi.HookErrorModeContinue:
		// use the specified value
	default:
		// default to fail
		localHook.OnError = kahuapi.HookErrorModeFail
	}

	if localHook.Timeout.Duration == 0 {
		localHook.Timeout.Duration = defaultTimeout
	}

	log := h.logger.WithFields(
		log.Fields{
			"hookName":      hookName,
			"hookNamespace": namespace,
			"hookContainer": localHook.Container,
			"hookCommand":   localHook.Command,
			"hookOnError":   localHook.OnError,
			"hookTimeout":   localHook.Timeout,
		},
	)

	log.Info("running exec hook")
	executor := hookContainerExecuter{
		config: h.restConfig,
		kubeClient: h.kubeClient,
	}

	err := executor.ContainerExecute(
		localHook.Container,
		localHook.Command,
		namespace,
		name,
		localHook.Timeout,
	)

	return err
}

func (h *hooksHandler) checkInclude(in []string, ex []string, key string) bool {
	setIn := sets.NewString().Insert(in...)
	setEx := sets.NewString().Insert(ex...)
	if setEx.Has(key) {
		return false
	}
	return len(in) == 0 || setIn.Has(key)
}

func (h *hooksHandler) validateHook(hookSpec kahuapi.ResourceHookSpec, name, namespace string, slabels labels.Set) bool {
	// Check namespace
	namespacesIn := hookSpec.IncludeNamespaces
	namespacesEx := hookSpec.ExcludeNamespaces

	if !h.checkInclude(namespacesIn, namespacesEx, namespace) {
		h.logger.Infof("invalid namespace (%s), skipping  hook execution", namespace)
		return false
	}
	// Check resource
	resourcesIn := hookSpec.IncludeResources
	resourcesEx := hookSpec.ExcludeResources

	var resourceNames []string
	resourceNames = append(resourceNames, name)
	names := utils.FindMatchedStrings(hookResource, resourceNames, resourcesIn, resourcesEx)
	setNames := sets.NewString().Insert(names...)
	if !setNames.Has(name) {
		h.logger.Infof("invalid resource (%s), skipping hook execution", name)
		return false
	}
	// Check label
	var selector labels.Selector
	if hookSpec.LabelSelector != nil {
		labelSelector, err := metav1.LabelSelectorAsSelector(hookSpec.LabelSelector)
		if err != nil {
			h.logger.Infof("error getting label selector(%s), skipping hook execution", err.Error())
			return false
		}
		selector = labelSelector
	}
	if selector != nil && !selector.Matches(slabels) {
		h.logger.Infof("invalid label for hook execution, skipping hook execution")
		return false
	}
	return true
}

func ensureContainerExists(pod *v1.Pod, container string) error {
	for _, c := range pod.Spec.Containers {
		if c.Name == container {
			return nil
		}
	}
	return errors.New("no such container")
}

func setDefaultHookContainer(pod *v1.Pod, hook *kahuapi.ExecHook) error {
	if len(pod.Spec.Containers) < 1 {
		return errors.New("need at least 1 container")
	}

	hook.Container = pod.Spec.Containers[0].Name
	return nil
}

// ContainerExecuter defines interface for executor in container
type ContainerExecuter interface {
	ContainerExecute(containerName string,
		commands []string,
		namespace string,
		podName string,
		timeout metav1.Duration)
}

// HookContainerExecuter implements ContainerExecute interface
type hookContainerExecuter struct{
	kubeClient 			kubernetes.Interface
	config              *restclient.Config
}

// ContainerExecute implements execution in a named container
func (ce *hookContainerExecuter) ContainerExecute(
	containerName string,
	commands []string,
	namespace string,
	podName string,
	timeout metav1.Duration,
) error {
	log := log.WithField("container", containerName)

	req := ce.kubeClient.CoreV1().RESTClient().Post().Resource("pods").Name(podName).Namespace(namespace).SubResource("exec")
	scheme := runtime.NewScheme()
	if err := v1.AddToScheme(scheme); err != nil {
		log.Errorf("failed to add functions to scheme")
		return err
	}
	parameterCodec := runtime.NewParameterCodec(scheme)
	req.VersionedParams(&v1.PodExecOptions{
		Stdin:     false,
		Stdout:    true,
		Stderr:    true,
		TTY:       false,
		Container: containerName,
		Command:   commands,
	}, parameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(ce.config, "POST", req.URL())
	if err != nil {
		log.Errorf("failed to create remove executor")
		return err
	}

	errCh := make(chan error)
	var stdout, stderr bytes.Buffer

	go func() {
		err := exec.Stream(remotecommand.StreamOptions{
			Stdin:  nil,
			Stdout: &stdout,
			Stderr: &stderr,
			Tty:    false,
		})
		errCh <- err
	}()
	var timeoutCh <-chan time.Time
	if timeout.Duration > 0 {
		timer := time.NewTimer(timeout.Duration)
		defer timer.Stop()
		timeoutCh = timer.C
	}

	select {
	case err = <-errCh:
	case <-timeoutCh:
		return errors.New("timed out while executing hook")
	}

	log.Infof("STDOUT from container: %s", stdout.String())
	log.Infof("STDERR from container: %s", stderr.String())
	return err
}
