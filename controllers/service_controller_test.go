package controllers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetNodeIPv4ReturnsIPv4(t *testing.T) {
	addresses := []corev1.NodeAddress{
		{Type: corev1.NodeInternalIP, Address: "192.168.1.10"},
	}
	assert.Equal(t, "192.168.1.10", getNodeIPv4(addresses))
}

func TestGetNodeIPv4IgnoresIPv6(t *testing.T) {
	addresses := []corev1.NodeAddress{
		{Type: corev1.NodeInternalIP, Address: "fd00::1"},
	}
	assert.Equal(t, "", getNodeIPv4(addresses))
}

func TestGetNodeIPv4ReturnsIPv4FromMixedAddresses(t *testing.T) {
	addresses := []corev1.NodeAddress{
		{Type: corev1.NodeInternalIP, Address: "fd00::1"},
		{Type: corev1.NodeInternalIP, Address: "192.168.1.10"},
	}
	assert.Equal(t, "192.168.1.10", getNodeIPv4(addresses))
}

func TestGetNodeIPv4IgnoresNonInternalAddresses(t *testing.T) {
	addresses := []corev1.NodeAddress{
		{Type: corev1.NodeExternalIP, Address: "10.0.0.1"},
	}
	assert.Equal(t, "", getNodeIPv4(addresses))
}


func TestGetHolepunchPortMapping(t *testing.T) {
	portMapping, err := getHolepunchPortMapping(corev1.Service{
		ObjectMeta: v1.ObjectMeta{
			Name:        "my-service",
			Namespace:   "default",
			Annotations: map[string]string{
				holepunchAnnotationName: "true",
				holepunchPortMapAnnotationPrefix + "80": "3000",
				holepunchPortMapAnnotationPrefix + "443": "4000",
			},
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, portMapping, map[uint16]uint16{
		80: 3000,
		443: 4000,
	})
}

func TestGetHolepunchPortMappingNonNumericErrors(t *testing.T) {
	portMapping, err := getHolepunchPortMapping(corev1.Service{
		ObjectMeta: v1.ObjectMeta{
			Name:        "my-service",
			Namespace:   "default",
			Annotations: map[string]string{
				holepunchAnnotationName: "true",
				holepunchPortMapAnnotationPrefix + "80": "some-non-numeric-value",
			},
		},
	})
	assert.Error(t, err)
	assert.Nil(t, portMapping)
}

func TestGetHolepunchPortMappingInvalidPortNumberErrors(t *testing.T) {
	portMapping, err := getHolepunchPortMapping(corev1.Service{
		ObjectMeta: v1.ObjectMeta{
			Name:        "my-service",
			Namespace:   "default",
			Annotations: map[string]string{
				holepunchAnnotationName: "true",
				// 70,000 is too high for a port number (on Linux)
				holepunchPortMapAnnotationPrefix + "80": "70000",
			},
		},
	})
	assert.Error(t, err)
	assert.Nil(t, portMapping)
}
