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
	logger := zap.NewNop()
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
	require.NoError(t, err)

	err = cmController.Run()
	require.NoError(t, err)
	defer cmController.Stop()

	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap1, metav1.CreateOptions{})
	_, _ = clientset.CoreV1().ConfigMaps("second").Create(ctx, configMap2, metav1.CreateOptions{})
	_, _ = clientset.CoreV1().ConfigMaps("second").Create(ctx, configMap3, metav1.CreateOptions{})
	time.Sleep(100 * time.Millisecond)

	// List ConfigMaps
	configMaps, err := clientset.CoreV1().ConfigMaps("second").List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	assert.Len(t, configMaps.Items, 2)

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
	var logger = zap.NewNop()
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

	err = cmController.Run()
	defer cmController.Stop()
	require.NoError(t, err)

	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap1, metav1.CreateOptions{})
	_, _ = clientset.CoreV1().ConfigMaps("second").Create(ctx, configMap2, metav1.CreateOptions{})
	_, _ = clientset.CoreV1().ConfigMaps("second").Create(ctx, configMap3, metav1.CreateOptions{})
	time.Sleep(100 * time.Millisecond)

	// List ConfigMaps
	configMaps, err := clientset.CoreV1().ConfigMaps(corev1.NamespaceAll).List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	assert.Len(t, configMaps.Items, 3)

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
func TestNewConfigMapController_NonExistentCmType(t *testing.T) {
	var logger = zap.NewNop()

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

	clientset := fake.NewClientset(configMap1)

	cmController, err := NewConfigMapController("hub-nonexistent-cm-type", "second", clientset, logger)
	require.Nil(t, cmController)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported ConfigMap type")
}

func TestGetHubData(t *testing.T) {
	var logger = zap.NewNop()
	ctx := t.Context()
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-first-manual-variables",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   "mdaihub-first",
			},
		},
		Data: map[string]string{
			"first_manual_variable":  "boolean",
			"second_manual_variable": "string",
		},
	}
	clientset := fake.NewClientset(configMap)

	cmController, err := NewConfigMapController(ManualEnvConfigMapType, "first", clientset, logger)
	require.NoError(t, err)

	err = cmController.Run()
	require.NoError(t, err)
	defer cmController.Stop()

	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap, metav1.CreateOptions{})
	time.Sleep(100 * time.Millisecond)

	expectedHubData := []map[string]string{
		{
			"first_manual_variable":  "boolean",
			"second_manual_variable": "string",
		},
	}

	assert.Eventually(t, func() bool {
		hubData, err := cmController.GetHubData("mdaihub-first")
		if err != nil {
			return false
		}
		return assert.ObjectsAreEqual(expectedHubData, hubData)
	}, time.Second, 100*time.Millisecond, "Expected hub data to eventually match")

	assert.Eventually(t, func() bool {
		cm, err := cmController.GetConfigMapByHubName("mdaihub-first")
		if err != nil {
			return false
		}
		return assert.ObjectsAreEqual(configMap, cm)
	}, time.Second, 100*time.Millisecond, "Expected hub data to eventually match")
}

func TestGetAllHubsToDataMap(t *testing.T) {
	var logger = zap.NewNop()
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
	require.NoError(t, err)

	err = cmController.Run()
	require.NoError(t, err)
	defer cmController.Stop()

	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap1, metav1.CreateOptions{})
	_, _ = clientset.CoreV1().ConfigMaps("second").Create(ctx, configMap2, metav1.CreateOptions{})
	_, _ = clientset.CoreV1().ConfigMaps("second").Create(ctx, configMap3, metav1.CreateOptions{})
	time.Sleep(100 * time.Millisecond)

	expectedHubData := map[string]map[string]string{
		"mdaihub-second": {
			"second_manual_variable": "string",
		},
		"mdaihub-third": {
			"third_manual_variable": "string",
		},
	}

	assert.Eventually(t, func() bool {
		hubData, err := cmController.GetAllHubsToDataMap()
		if err != nil {
			return false
		}
		return assert.ObjectsAreEqual(expectedHubData, hubData)
	}, time.Second, 100*time.Millisecond, "Expected hub data to eventually match")
}

func TestGetConfigMapByHubName_NotFound(t *testing.T) {
	var logger = zap.NewNop()
	clientset := fake.NewClientset()

	cmController, err := NewConfigMapController(ManualEnvConfigMapType, "first", clientset, logger)
	require.NoError(t, err)

	err = cmController.Run()
	require.NoError(t, err)
	defer cmController.Stop()

	assert.Eventually(t, func() bool {
		cm, err := cmController.GetConfigMapByHubName("non-existent-hub")
		if cm != nil {
			return false
		}
		if err == nil {
			return false
		}
		return assert.Contains(t, err.Error(), "no ConfigMap hub-manual-variables found for hub: non-existent-hub")
	}, time.Second, 100*time.Millisecond, "Expected error for non-existent hub")
}

func TestGetConfigMapByHubName_MultipleFound(t *testing.T) {
	var logger = zap.NewNop()
	ctx := t.Context()
	const hubName = "shared-hub"

	configMap1 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-first-manual-variables",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   hubName,
			},
		},
	}
	configMap2 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-second-manual-variables",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   hubName,
			},
		},
	}

	clientset := fake.NewClientset(configMap1, configMap2)

	cmController, err := NewConfigMapController(ManualEnvConfigMapType, "first", clientset, logger)
	require.NoError(t, err)

	err = cmController.Run()
	require.NoError(t, err)
	defer cmController.Stop()

	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap1, metav1.CreateOptions{})
	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap2, metav1.CreateOptions{})
	time.Sleep(100 * time.Millisecond)

	assert.Eventually(t, func() bool {
		cm, err := cmController.GetConfigMapByHubName(hubName)
		if cm != nil {
			return false
		}
		if err == nil {
			return false
		}
		return assert.Contains(t, err.Error(), "multiple ConfigMaps hub-manual-variables found for the same hub: shared-hub")
	}, time.Second, 100*time.Millisecond, "Expected error for multiple config maps")
}
