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
	"k8s.io/klog"
	"k8s.io/utils/pointer"
)

type machineSetScalableResource struct {
	controller *machineController
	machineSet *MachineSet
	lock       sync.RWMutex
	maxSize    int
	minSize    int
}

var _ scalableResource = (*machineSetScalableResource)(nil)

func (r *machineSetScalableResource) ID() string {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return path.Join(r.Namespace(), r.Name())
}

func (r *machineSetScalableResource) MaxSize() int {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.maxSize
}

func (r *machineSetScalableResource) MinSize() int {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.minSize
}

func (r *machineSetScalableResource) Name() string {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.machineSet.Name
}

func (r *machineSetScalableResource) Namespace() string {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.machineSet.Namespace
}

func (r *machineSetScalableResource) Nodes() ([]string, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.controller.machineSetProviderIDs(r.machineSet)
}

func (r machineSetScalableResource) Replicas() int32 {
	r.lock.RLock()
	defer r.lock.RUnlock()

	if r.machineSet.Spec.Replicas == nil {
		klog.Warningf("MachineSet %q has nil spec.replicas. This is unsupported behaviour. Falling back to status.replicas.", r.machineSet.Name)
	}

	// If no value for replicas on the MachineSet spec, fallback to the status
	// TODO: Remove this fallback once defaulting is implemented for MachineSet Replicas
	return pointer.Int32PtrDerefOr(r.machineSet.Spec.Replicas, r.machineSet.Status.Replicas)
}

func (r *machineSetScalableResource) SetSize(nreplicas int32) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	u, err := r.controller.dynamicclient.Resource(*r.controller.machineSetResource).Namespace(r.machineSet.Namespace).Get(context.TODO(), r.machineSet.Name, metav1.GetOptions{})

	if err != nil {
		return err
	}

	if u == nil {
		return fmt.Errorf("unknown machineSet %s", r.machineSet.Name)
	}

	u = u.DeepCopy()
	if err := unstructured.SetNestedField(u.Object, int64(nreplicas), "spec", "replicas"); err != nil {
		return fmt.Errorf("failed to set replica value: %v", err)
	}

	_, updateErr := r.controller.dynamicclient.Resource(*r.controller.machineSetResource).Namespace(u.GetNamespace()).Update(context.TODO(), u, metav1.UpdateOptions{})
	if updateErr != nil {
		return updateErr
	}

	r.machineSet = newMachineSetFromUnstructured(u)
	return nil
}

func (r *machineSetScalableResource) MarkMachineForDeletion(machine *Machine) error {
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
	return updateErr
}

func (r *machineSetScalableResource) UnmarkMachineForDeletion(machine *Machine) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	return unmarkMachineForDeletion(r.controller, machine)
}

func newMachineSetScalableResource(controller *machineController, machineSet *MachineSet) (*machineSetScalableResource, error) {
	minSize, maxSize, err := parseScalingBounds(machineSet.Annotations)
	if err != nil {
		return nil, fmt.Errorf("error validating min/max annotations: %v", err)
	}

	return &machineSetScalableResource{
		controller: controller,
		machineSet: machineSet,
		maxSize:    maxSize,
		minSize:    minSize,
	}, nil
}

func (r *machineSetScalableResource) Labels() map[string]string {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.machineSet.Spec.Template.Spec.Labels
}

func (r *machineSetScalableResource) Taints() []apiv1.Taint {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.machineSet.Spec.Template.Spec.Taints
}

func (r *machineSetScalableResource) CanScaleFromZero() bool {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return scaleFromZeroEnabled(r.machineSet.Annotations)
}

func (r *machineSetScalableResource) InstanceCPUCapacity() (resource.Quantity, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return parseCPUCapacity(r.machineSet.Annotations)
}

func (r *machineSetScalableResource) InstanceMemoryCapacity() (resource.Quantity, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return parseMemoryCapacity(r.machineSet.Annotations)
}

func (r *machineSetScalableResource) InstanceGPUCapacity() (resource.Quantity, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return parseGPUCapacity(r.machineSet.Annotations)
}

func (r *machineSetScalableResource) InstanceMaxPodsCapacity() (resource.Quantity, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return parseMaxPodsCapacity(r.machineSet.Annotations)
}
