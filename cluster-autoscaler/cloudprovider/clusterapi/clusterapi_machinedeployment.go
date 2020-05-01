/*
Copyright 2020 The Kubernetes Authors.

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

package clusterapi

import (
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/pointer"
)

type machineDeploymentScalableResource struct {
	controller        *machineController
	machineDeployment *MachineDeployment
	lock              sync.RWMutex
	maxSize           int
	minSize           int
}

var _ scalableResource = (*machineDeploymentScalableResource)(nil)

func (r *machineDeploymentScalableResource) ID() string {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return path.Join(r.Namespace(), r.Name())
}

func (r *machineDeploymentScalableResource) MaxSize() int {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.maxSize
}

func (r *machineDeploymentScalableResource) MinSize() int {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.minSize
}

func (r *machineDeploymentScalableResource) Name() string {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.machineDeployment.Name
}

func (r *machineDeploymentScalableResource) Namespace() string {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.machineDeployment.Namespace
}

func (r *machineDeploymentScalableResource) Nodes() ([]string, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	var result []string

	if err := r.controller.filterAllMachineSets(func(machineSet *MachineSet) error {
		if machineSetIsOwnedByMachineDeployment(machineSet, r.machineDeployment) {
			providerIDs, err := r.controller.machineSetProviderIDs(machineSet)
			if err != nil {
				return err
			}
			result = append(result, providerIDs...)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return result, nil
}

func (r *machineDeploymentScalableResource) Replicas() int32 {
	r.lock.RLock()
	defer r.lock.RUnlock()

	if r.machineDeployment.Spec.Replicas == nil {
		klog.Warningf("MachineDeployment %q has nil spec.replicas. This is unsupported behaviour. Falling back to status.replicas.", r.machineDeployment.Name)
	}
	// If no value for replicas on the MachineSet spec, fallback to the status
	// TODO: Remove this fallback once defaulting is implemented for MachineSet Replicas
	return pointer.Int32PtrDerefOr(r.machineDeployment.Spec.Replicas, r.machineDeployment.Status.Replicas)
}

func (r *machineDeploymentScalableResource) SetSize(nreplicas int32) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	u, err := r.controller.dynamicclient.Resource(*r.controller.machineDeploymentResource).Namespace(r.machineDeployment.Namespace).Get(context.TODO(), r.machineDeployment.Name, metav1.GetOptions{})

	if err != nil {
		return err
	}

	if u == nil {
		return fmt.Errorf("unknown machineDeployment %s", r.machineDeployment.Name)
	}

	u = u.DeepCopy()
	if err := unstructured.SetNestedField(u.Object, int64(nreplicas), "spec", "replicas"); err != nil {
		return fmt.Errorf("failed to set replica value: %v", err)
	}

	_, updateErr := r.controller.dynamicclient.Resource(*r.controller.machineDeploymentResource).Namespace(u.GetNamespace()).Update(context.TODO(), u, metav1.UpdateOptions{})
	if updateErr != nil {
		return updateErr
	}

	r.machineDeployment = newMachineDeploymentFromUnstructured(u)
	return nil
}

func (r *machineDeploymentScalableResource) MarkMachineForDeletion(machine *Machine) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	u, err := r.controller.dynamicclient.Resource(*r.controller.machineResource).Namespace(machine.Namespace).Get(context.TODO(), machine.Name, metav1.GetOptions{})

	if err != nil {
		return err
	}
	if u == nil {
		return fmt.Errorf("unknown machine %s", machine.Name)
	}

	u = u.DeepCopy()

	annotations := u.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[machineDeleteAnnotationKey] = time.Now().String()
	u.SetAnnotations(annotations)

	_, updateErr := r.controller.dynamicclient.Resource(*r.controller.machineResource).Namespace(u.GetNamespace()).Update(context.TODO(), u, metav1.UpdateOptions{})
	if updateErr != nil {
		return updateErr
	}

	r.machineDeployment = newMachineDeploymentFromUnstructured(u)
	return nil
}

func newMachineDeploymentScalableResource(controller *machineController, machineDeployment *MachineDeployment) (*machineDeploymentScalableResource, error) {
	minSize, maxSize, err := parseScalingBounds(machineDeployment.Annotations)
	if err != nil {
		return nil, fmt.Errorf("error validating min/max annotations: %v", err)
	}

	return &machineDeploymentScalableResource{
		controller:        controller,
		machineDeployment: machineDeployment,
		maxSize:           maxSize,
		minSize:           minSize,
	}, nil
}

func (r *machineDeploymentScalableResource) Taints() []apiv1.Taint {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.machineDeployment.Spec.Template.Spec.Taints
}

func (r *machineDeploymentScalableResource) Labels() map[string]string {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.machineDeployment.Spec.Template.Spec.Labels
}

func (r *machineDeploymentScalableResource) CanScaleFromZero() bool {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return scaleFromZeroEnabled(r.machineDeployment.Annotations)
}

func (r *machineDeploymentScalableResource) InstanceCPUCapacity() (resource.Quantity, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return parseCPUCapacity(r.machineDeployment.Annotations)
}

func (r *machineDeploymentScalableResource) InstanceMemoryCapacity() (resource.Quantity, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return parseMemoryCapacity(r.machineDeployment.Annotations)
}

func (r *machineDeploymentScalableResource) InstanceGPUCapacity() (resource.Quantity, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return parseGPUCapacity(r.machineDeployment.Annotations)
}

func (r *machineDeploymentScalableResource) InstanceMaxPodsCapacity() (resource.Quantity, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return parseMaxPodsCapacity(r.machineDeployment.Annotations)
}
