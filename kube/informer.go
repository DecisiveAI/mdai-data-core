package kube

import (
	"fmt"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	ByHub                      = "IndexByHub"
	ManagedByMdaiOperatorLabel = "app.kubernetes.io/managed-by=mdai-operator"
	EnvConfigMapType           = "hub-variables"
	ManualEnvConfigMapType     = "hub-manual-variables"
	AutomationConfigMapType    = "hub-automation"
	LabelMdaiHubName           = "mydecisive.ai/hub-name"
	ConfigMapTypeLabel         = "mydecisive.ai/configmap-type"
)

type ConfigMapController struct {
	InformerFactory informers.SharedInformerFactory
	CmInformer      coreinformers.ConfigMapInformer
	lock            sync.RWMutex
	namespace       string
	configMapType   string
	Logger          *zap.Logger
	stopCh          chan struct{}
}

func (cmc *ConfigMapController) Lock() {
	cmc.lock.Lock()
}

func (cmc *ConfigMapController) Unlock() {
	cmc.lock.Unlock()
}

func (cmc *ConfigMapController) RLock() {
	cmc.lock.RLock()
}

func (cmc *ConfigMapController) RUnlock() {
	cmc.lock.RUnlock()
}

func (cmc *ConfigMapController) Run() error {
	resultCh := make(chan error)
	cmc.stopCh = make(chan struct{})

	go func() {
		cmc.InformerFactory.Start(cmc.stopCh)
		if !cache.WaitForCacheSync(cmc.stopCh, cmc.CmInformer.Informer().HasSynced) {
			resultCh <- fmt.Errorf("failed to sync")
		} else {
			resultCh <- nil
		}
	}()
	return <-resultCh
}

func (cmc *ConfigMapController) Stop() {
	close(cmc.stopCh)
}

func NewConfigMapController(configMapType string, namespace string, clientset kubernetes.Interface, logger *zap.Logger) (*ConfigMapController, error) {

	defaultResyncTime := time.Hour * 24

	var informerFactory informers.SharedInformerFactory
	switch configMapType {
	case EnvConfigMapType, ManualEnvConfigMapType, AutomationConfigMapType:
		informerFactory = informers.NewSharedInformerFactoryWithOptions(
			clientset,
			defaultResyncTime,
			informers.WithNamespace(namespace),
			informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
				opts.LabelSelector = fmt.Sprintf("%s=%s", ConfigMapTypeLabel, configMapType)
			}),
		)
	default:
		return nil, fmt.Errorf("unsupported ConfigMap type")
	}

	cmInformer := informerFactory.Core().V1().ConfigMaps()

	if err := cmInformer.Informer().AddIndexers(map[string]cache.IndexFunc{
		ByHub: func(obj interface{}) ([]string, error) {
			var hubNames []string
			hubName, err := getHubName(obj.(*v1.ConfigMap))
			if err != nil {
				logger.Error("failed to get hub name for ConfigMap", zap.String("ConfigMap name", obj.(*v1.ConfigMap).Name))
				return nil, err
			}
			hubNames = append(hubNames, hubName)
			return hubNames, nil
		},
	}); err != nil {
		logger.Error("failed to add index", zap.Error(err))
		return nil, err
	}

	c := &ConfigMapController{
		namespace:       namespace,
		configMapType:   configMapType,
		InformerFactory: informerFactory,
		CmInformer:      cmInformer,
		Logger:          logger,
	}

	return c, nil
}

func getHubName(configMap *v1.ConfigMap) (string, error) {
	if hubName, ok := configMap.Labels[LabelMdaiHubName]; ok {
		return hubName, nil
	}
	return "", fmt.Errorf("ConfigMap does not have hub name label")
}

func NewK8sClient(logger *zap.Logger) (kubernetes.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		kubeconfig, err := os.UserHomeDir()
		if err != nil {
			logger.Error("Failed to load k8s config", zap.Error(err))
			return nil, err
		}

		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig+"/.kube/config")
		if err != nil {
			logger.Error("Failed to build k8s config", zap.Error(err))
			return nil, err
		}
	}
	return kubernetes.NewForConfig(config)
}

func (cmc *ConfigMapController) GetAllHubsToDataMap() (map[string]any, error) {
	cmc.RLock()
	defer cmc.RUnlock()

	hubMap := make(map[string]any)

	indexer := cmc.CmInformer.Informer().GetIndexer()
	hubNames := indexer.ListIndexFuncValues(ByHub)
	for _, hubName := range hubNames {
		objs, err := indexer.ByIndex(ByHub, hubName)
		if err != nil {
			continue
		}
		for _, obj := range objs {
			cm := obj.(*v1.ConfigMap)
			hubMap[hubName] = cm.Data
		}
	}
	return hubMap, nil
}

func (cmc *ConfigMapController) GetHubData(hubName string) ([]map[string]string, error) {
	cmc.RLock()
	defer cmc.RUnlock()

	indexer := cmc.CmInformer.Informer().GetIndexer()
	objs, err := indexer.ByIndex(ByHub, hubName)
	if err != nil {
		return nil, err
	}
	result := make([]map[string]string, 0, len(objs))
	for _, obj := range objs {
		cm := obj.(*v1.ConfigMap)
		result = append(result, cm.Data)
	}
	return result, nil
}
