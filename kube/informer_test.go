package kube

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestNewConfigMapController_SingleNs(t *testing.T) {
	var logger *zap.Logger
	ctx := t.Context()

	configMap1 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-first-manual-variables",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   "mdaihub-first",
			},
		},
		Data: map[string]string{
			"first_manual_variable": "boolean",
		},
	}
	configMap2 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-second-manual-variables",
			Namespace: "second",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   "mdaihub-second",
			},
		},
		Data: map[string]string{
			"second_manual_variable": "string",
		},
	}
	configMap3 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-third-manual-variables",
			Namespace: "second",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   "mdaihub-third",
			},
		},
		Data: map[string]string{
			"third_manual_variable": "string",
		},
	}

	clientset := fake.NewClientset(configMap1, configMap2, configMap3)

	cmController, err := NewConfigMapController(ManualEnvConfigMapType, "second", clientset, logger)
	if err != nil {
		logger.Fatal("failed to create ConfigMap controller", zap.Error(err))
	}

	stop := make(chan struct{})
	defer close(stop)
	err = cmController.Run(stop)
	if err != nil {
		logger.Fatal("failed to run ConfigMap controller", zap.Error(err))
	}

	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap1, metav1.CreateOptions{})
	_, _ = clientset.CoreV1().ConfigMaps("second").Create(ctx, configMap2, metav1.CreateOptions{})
	_, _ = clientset.CoreV1().ConfigMaps("second").Create(ctx, configMap3, metav1.CreateOptions{})
	time.Sleep(100 * time.Millisecond)

	// List ConfigMaps
	configMaps, err := clientset.CoreV1().ConfigMaps("second").List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	assert.Len(t, configMaps.Items, 2)

	cmController.Lock.RLock()
	defer cmController.Lock.RUnlock()

	indexer := cmController.CmInformer.Informer().GetIndexer()
	hubNames := indexer.ListIndexFuncValues(ByHub)
	assert.ElementsMatch(t, hubNames, []string{"mdaihub-second", "mdaihub-third"})

	hubMap := make(map[string]*corev1.ConfigMap)
	for _, hubName := range hubNames {
		objs, err := indexer.ByIndex(ByHub, hubName)
		if err != nil {
			continue
		}
		for _, obj := range objs {
			cm := obj.(*corev1.ConfigMap)
			hubMap[hubName] = cm
		}
	}
	assert.Equal(t, hubMap["mdaihub-second"], configMap2)
	assert.Equal(t, hubMap["mdaihub-third"], configMap3)

}
func TestNewConfigMapController_MultipleNs(t *testing.T) {
	var logger *zap.Logger
	ctx := t.Context()

	configMap1 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-first-manual-variables",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   "mdaihub-first",
			},
		},
		Data: map[string]string{
			"first_manual_variable": "boolean",
		},
	}
	configMap2 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-second-manual-variables",
			Namespace: "second",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   "mdaihub-second",
			},
		},
		Data: map[string]string{
			"second_manual_variable": "string",
		},
	}
	configMap3 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-third-manual-variables",
			Namespace: "second",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   "mdaihub-third",
			},
		},
		Data: map[string]string{
			"third_manual_variable": "string",
		},
	}

	clientset := fake.NewClientset(configMap1, configMap2, configMap3)

	cmController, err := NewConfigMapController(ManualEnvConfigMapType, corev1.NamespaceAll, clientset, logger)
	require.NoError(t, err)

	stop := make(chan struct{})
	defer close(stop)
	err = cmController.Run(stop)
	require.NoError(t, err)

	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap1, metav1.CreateOptions{})
	_, _ = clientset.CoreV1().ConfigMaps("second").Create(ctx, configMap2, metav1.CreateOptions{})
	_, _ = clientset.CoreV1().ConfigMaps("second").Create(ctx, configMap3, metav1.CreateOptions{})
	time.Sleep(100 * time.Millisecond)

	// List ConfigMaps
	configMaps, err := clientset.CoreV1().ConfigMaps(corev1.NamespaceAll).List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	assert.Len(t, configMaps.Items, 3)

	cmController.Lock.RLock()
	defer cmController.Lock.RUnlock()

	indexer := cmController.CmInformer.Informer().GetIndexer()
	hubNames := indexer.ListIndexFuncValues(ByHub)
	assert.ElementsMatch(t, hubNames, []string{"mdaihub-first", "mdaihub-second", "mdaihub-third"})

	hubMap := make(map[string]*corev1.ConfigMap)
	for _, hubName := range hubNames {
		objs, err := indexer.ByIndex(ByHub, hubName)
		if err != nil {
			continue
		}
		for _, obj := range objs {
			cm := obj.(*corev1.ConfigMap)
			hubMap[hubName] = cm
		}
	}
	assert.Equal(t, hubMap["mdaihub-first"], configMap1)
	assert.Equal(t, hubMap["mdaihub-second"], configMap2)
	assert.Equal(t, hubMap["mdaihub-third"], configMap3)

}
