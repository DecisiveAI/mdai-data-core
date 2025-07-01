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
	Lock            sync.RWMutex
	Namespace       string
	ConfigMapType   string
	Logger          *zap.Logger
}

func (c *ConfigMapController) Run(stopCh chan struct{}) error {
	c.InformerFactory.Start(stopCh)

	if !cache.WaitForCacheSync(stopCh, c.CmInformer.Informer().HasSynced) {
		return fmt.Errorf("failed to sync")
	}
	return nil
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
		Namespace:       namespace,
		ConfigMapType:   configMapType,
		InformerFactory: informerFactory,
		CmInformer:      cmInformer,
		Logger:          logger,
	}

	return c, nil
}

func getHubName(configMap *v1.ConfigMap) (string, error) {
	if len(configMap.Labels) > 0 {
		hubName := configMap.Labels[LabelMdaiHubName]
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
