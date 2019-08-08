package osd

import (
	"fmt"

	rookalpha "github.com/rook/rook/pkg/apis/rook.io/v1alpha2"
	opspec "github.com/rook/rook/pkg/operator/ceph/spec"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (c *Cluster) prepareStorageClassDeviceSets(config *provisionConfig) []rookalpha.VolumeSource {
	volumeSources := []rookalpha.VolumeSource{}
	for _, storageClassDeviceSet := range c.DesiredStorage.StorageClassDeviceSets {
		if err := opspec.CheckPodMemory(storageClassDeviceSet.Resources, cephOsdPodMinimumMemory); err != nil {
			config.addError("cannot use storageClassDeviceSet %s for creating osds %v", storageClassDeviceSet.Name, err)
			continue
		}
		for setIndex := 0; setIndex < storageClassDeviceSet.Count; setIndex++ {
			pvc, err := c.createStorageClassDeviceSetPVC(storageClassDeviceSet, setIndex)
			if err != nil {
				config.addError("%+v", err)
				config.addError("OSD creation for storageClassDeviceSet %v failed for count %v", storageClassDeviceSet.Name, setIndex)
				continue
			}
			volumeSources = append(volumeSources, rookalpha.VolumeSource{
				Name:      storageClassDeviceSet.Name,
				Resources: storageClassDeviceSet.Resources,
				Placement: storageClassDeviceSet.Placement,
				Config:    storageClassDeviceSet.Config,
				PersistentVolumeClaimSource: v1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvc.GetName(),
					ReadOnly:  false,
				},
			})
			logger.Infof("successfully provisioned osd for storageClassDeviceSet %s of set %v", storageClassDeviceSet.Name, setIndex)
		}
	}
	return volumeSources
}

func (c *Cluster) createStorageClassDeviceSetPVC(storageClassDeviceSet rookalpha.StorageClassDeviceSet, setIndex int) (*v1.PersistentVolumeClaim, error) {
	if len(storageClassDeviceSet.VolumeClaimTemplates) == 0 {
		return nil, fmt.Errorf("No PVC available for storageClassDeviceSet %s", storageClassDeviceSet.Name)
	}
	deployedPVCs := []v1.PersistentVolumeClaim{}
	pvcStorageClassDeviceSetPVCId, pvcStorageClassDeviceSetPVCIdLabelSelector := makeStorageClassDeviceSetPVCID(storageClassDeviceSet.Name, setIndex, 0)

	pvc := makeStorageClassDeviceSetPVC(storageClassDeviceSet.Name, pvcStorageClassDeviceSetPVCId, 0, setIndex, storageClassDeviceSet.VolumeClaimTemplates[0])
	// Check if a PVC already exists with same StorageClassDeviceSet label
	presentPVCs, err := c.context.Clientset.CoreV1().PersistentVolumeClaims(c.Namespace).List(metav1.ListOptions{LabelSelector: pvcStorageClassDeviceSetPVCIdLabelSelector})
	if err != nil {
		return nil, fmt.Errorf("failed to create pvc %v for storageClassDeviceSet %v, err %+v", pvc.GetGenerateName(), storageClassDeviceSet.Name, err)
	}
	if len(presentPVCs.Items) == 0 { // No PVC found, creating a new one
		deployedPVC, err := c.context.Clientset.CoreV1().PersistentVolumeClaims(c.Namespace).Create(pvc)
		if err != nil {
			return nil, fmt.Errorf("failed to create pvc %v for storageClassDeviceSet %v, err %+v", pvc.GetGenerateName(), storageClassDeviceSet.Name, err)
		}
		deployedPVCs = append(deployedPVCs, *deployedPVC)
	} else if len(presentPVCs.Items) == 1 { // The PVC is already present.
		deployedPVCs = append(deployedPVCs, presentPVCs.Items...)
	} else { // More than one PVC exists with same labelSelector
		return nil, fmt.Errorf("more than one PVCs exists with label %v, pvcs %+v", pvcStorageClassDeviceSetPVCIdLabelSelector, presentPVCs)
	}
	return &deployedPVCs[0], nil
}
