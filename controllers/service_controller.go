package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/huin/goupnp/dcps/internetgateway2"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	holepunchAnnotationName          = "holepunch/punch-external"
	holepunchPortMapAnnotationPrefix = "holepunch.port/"
)

// ServiceReconciler reconciles a Service object
type ServiceReconciler struct {
	client.Client
	ClientSet                *kubernetes.Clientset
	Log                      logr.Logger
	Scheme                   *runtime.Scheme
	NodeAnnotationExternalIp string
	ForceNodePort            bool
	LeaseDurationSeconds     uint32
}

type patchStringValue struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value"`
}

// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=services/status,verbs=get
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get,patch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list

func (r *ServiceReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	log := r.Log.WithValues("service", req.NamespacedName)

	// Get current node name and node object
	nodeName := os.Getenv("KUBERNETES_NODENAME")
	node, err := r.ClientSet.CoreV1().Nodes().Get(nodeName, metav1.GetOptions{})
	if err != nil {
		log.Error(err, "Failed to obtain node name")
		return ctrl.Result{}, err
	}

	// Get the service
	var service corev1.Service
	if err := r.Get(ctx, req.NamespacedName, &service); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// We only care about services that have our annotation on them
	if !hasHolepunchAnnotation(service) {
		// Nothing to be done
		return ctrl.Result{}, nil
	}

	// We only care about LoadBalancer/NodePort services. We need a real internal IP to map to!
	if !(service.Spec.Type == corev1.ServiceTypeLoadBalancer || service.Spec.Type == corev1.ServiceTypeNodePort) {
		// This means we've put the annotation on a service that isn't a Loadbalancer/NodePort.
		log.Error(nil, "Holepunch requires a NodePort or LoadBalancer type service")
		// TODO emit event onto the service
		return ctrl.Result{}, nil
	}

	if r.ForceNodePort && service.Spec.Type == corev1.ServiceTypeLoadBalancer {
		// We're going to spoof that the svc type is NodePort
		service.Spec.Type = corev1.ServiceTypeNodePort
	}

	// Ensure that current node hosts pod if NodePort and ExternalTrafficPolicy is Local or skip processing
	if service.Spec.Type == corev1.ServiceTypeNodePort && service.Spec.ExternalTrafficPolicy == corev1.ServiceExternalTrafficPolicyTypeLocal {
		podList, err := r.ClientSet.CoreV1().Pods(service.Namespace).List(metav1.ListOptions{LabelSelector: labels.Set(service.Spec.Selector).AsSelectorPreValidated().String()})
		//pods, err := podInformer.Lister().Pods(service.Namespace).List(selector)
		if err != nil {
			log.Error(err, "Current node can't be used to expose service")
			return ctrl.Result{}, err
		}
		pods := podList.Items

		runsOnLocalNode := false

		for _, v := range pods {
			log.Info("Checking pod", "Pod", v.Name, "Node", v.Spec.NodeName)
			if v.Status.Phase == corev1.PodRunning {
				if strings.EqualFold(v.Spec.NodeName, nodeName) {
					log.Info("Found pod that runs on this node", "Pod", v.Name)
					runsOnLocalNode = true
					break
				}
			}
		}

		if !runsOnLocalNode {
			err := errors.New("ExternalTrafficPolicy is set to Local and no pods are running on this node")
			log.Error(err, "Current node can't be used to expose service")
			return ctrl.Result{}, err
		}
	}

	// Get the port mapping, if one exists. This instructs us to setup the UPnP mappings to use a *different* external
	// and internal port. Some routers may not support this feature.
	portMapping, err := getHolepunchPortMapping(service)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Find a router to configure
	router, err := PickRouterClient(ctx)
	if err != nil {
		log.Error(err, "Failed to find router to configure")
		return ctrl.Result{}, err
	}

	// Ask that router for *it's* external IP.
	// This is where the term "external" gets weird. There's the underlying pods in the K8s cluster which have IPs, then
	// the service has an IP inside the cluster, but it also has an "external" IP which is really an IP on the user's
	// home network (usually), and when we ask the *router* for "external" we really do mean public internet IP.
	externalIP, err := router.GetExternalIPAddress()
	if err != nil {
		log.Error(err, "Failed to resolve external IP address")
		return ctrl.Result{}, err
	}
	log = log.WithValues("external-ip", externalIP)

	// Cool part, update the current node's external-ip label
	// This will only process node that runs the controller.
	// If not running with leader election mode, you may need to ensure you only run one instance for all nodes belonging to a single router.
	// For example, apply pod anti-affinity based on the region/zone node label
	if r.NodeAnnotationExternalIp != "" {
		annotations := node.GetAnnotations()
		nodeAnnotationExternalIp := strings.Trim(r.NodeAnnotationExternalIp, "\"")

		if val, present := annotations[nodeAnnotationExternalIp]; present && val == externalIP {
			log.Info("Node annotation is up to date", nodeAnnotationExternalIp, externalIP)
		} else {
			nodeAnnotationExternalIpEscaped := strings.Replace(nodeAnnotationExternalIp, "/", "~1", 1)
			payload := []patchStringValue{{
				Op:    "replace",
				Path:  "/metadata/annotations/" + nodeAnnotationExternalIpEscaped,
				Value: externalIP,
			}}
			payloadBytes, _ := json.Marshal(payload)
			_, err = r.ClientSet.CoreV1().Nodes().Patch(nodeName, types.JSONPatchType, payloadBytes)
			if err != nil {
				log.Error(err, "Failed to update node annotation")
			} else {
				log.Info("Node annotation updated", nodeAnnotationExternalIp, externalIP)
			}
		}
	}

	var ipToMap string
	// Find the service's IP, that we're hoping is a local network IP from the perspective of the router.
	// This will be the current node IP on nodePort type of service
	if service.Spec.Type == corev1.ServiceTypeLoadBalancer {
		serviceIP, err := getServiceIP(service)
		if err != nil {
			log.Error(err, "Failed to get IP for service (has it not been allocated yet?)")
			return ctrl.Result{}, err
		}
		ipToMap = serviceIP
	} else if service.Spec.Type == corev1.ServiceTypeNodePort {
		for _, na := range node.Status.Addresses {
			if na.Type == corev1.NodeInternalIP {
				ipToMap = na.Address
			}
		}
	}

	log = log.WithValues("ip-to-map", ipToMap)

	description := fmt.Sprintf("Mapping for %s/%s", service.Name, service.Namespace)

	// Try to forward every port
	for _, servicePort := range service.Spec.Ports {
		// For some reason the Kubernetes Service API thinks a port can be an int32. On Linux at least it'll *always*
		// be a uint16 so this is a safe cast.
		var portNumber uint16
		if service.Spec.Type == corev1.ServiceTypeLoadBalancer {
			portNumber = uint16(servicePort.Port)
		} else if service.Spec.Type == corev1.ServiceTypeNodePort {
			portNumber = uint16(servicePort.NodePort)
		}

		protocol, err := toUPnPProtocol(servicePort.Protocol)
		if err != nil {
			log.Error(err, "Unable to resolve protocol to use")
			return ctrl.Result{}, err
		}

		// Figure out if we want to map the port
		externalPort, ok := portMapping[portNumber]
		if !ok {
			// We didn't have a mapping for this port.
			externalPort = portNumber
		}

		// Log out
		portLogger := log.WithValues("forwarding-port", portNumber,
			"external-port", externalPort,
			"upnp-description", description,
			"lease-duration", r.LeaseDurationSeconds)
		portLogger.Info("Attempting to forward port from router with UPnP")

		if err = router.AddPortMapping(
			"",
			// External port number to expose to Internet:
			externalPort,
			// Forward TCP (this could be "UDP" if we wanted that instead).
			protocol,
			// Internal port number on the LAN to forward to.
			// Some routers might not support this being different to the external
			// port number.
			portNumber,
			// Internal address on the LAN we want to forward to.
			ipToMap,
			// Enabled:
			true,
			// Informational description for the client requesting the port forwarding.
			description,
			// How long should the port forward last for in seconds.
			// If you want to keep it open for longer and potentially across router
			// resets, you might want to periodically request before this elapses.
			r.LeaseDurationSeconds,
		); err != nil {
			// When trying to map a port that is already mapped to another node/application, error 718 is usually returned.
			// This error would be expected when operating without leader election, with leader election NodePort mapping won't work well as only the node running as leader will be used to map the service externally - this will make all traffic flow through that node and if ExternalTrafficPolicy is set to Local and the pod doesn't run on the leader node, the service will simply not map as it wouldn't work anyway.
			// miniupnpd has something called secure mode, this will prevent a device from mapping ports to IPs other than it's own. PFsense for example hard-codes secure mode to be enabled, breaking LoadBalancer type mapping completely. This is why NodePort option was added to work around that issue.
			portLogger.Error(err, "Failed to configure UPnP port-forwarding")
			return ctrl.Result{}, err
		}
	}

	// Even on a "success" we need to come back before our lease is up to redo it.
	log.Info("Success, ports forwarded.", "reschedule-seconds", r.LeaseDurationSeconds-30)
	return ctrl.Result{RequeueAfter: time.Duration((r.LeaseDurationSeconds - 30) * uint32(time.Second))}, nil
}

func getHolepunchPortMapping(service corev1.Service) (map[uint16]uint16, error) {
	portMapping := make(map[uint16]uint16)
	for annotationName, annotationValue := range service.Annotations {
		if strings.HasPrefix(annotationName, holepunchPortMapAnnotationPrefix) {
			// The term "internal port" is "port on our local network", or "port with the Kubenretes service" exposes.
			// Meanwhile "external port" is "port exposed by the router on the open internet". If we have the annotaiton
			// "holepunch.port/80: 3000" that means that our "internal port" is 80 and our "external port" is 3000.
			internalPortStr := strings.TrimPrefix(annotationName, holepunchPortMapAnnotationPrefix)
			internalPort, err := strconv.ParseUint(internalPortStr, 10, 16)
			if err != nil {
				return nil, err
			}
			externalPortStr := annotationValue
			externalPort, err := strconv.ParseUint(externalPortStr, 10, 16)
			if err != nil {
				return nil, err
			}
			// These casts to uint16 (from uint64) are safe because we told strconv.ParseUint earlier to confine to 16
			// bits only.
			portMapping[uint16(internalPort)] = uint16(externalPort)
		}
	}
	return portMapping, nil
}

func hasHolepunchAnnotation(service corev1.Service) bool {
	for name, value := range service.Annotations {
		if name == holepunchAnnotationName {
			return value == "true"
		}
	}
	return false
}

func toUPnPProtocol(serviceProtocol corev1.Protocol) (string, error) {
	if serviceProtocol == corev1.ProtocolTCP {
		return "TCP", nil
	} else if serviceProtocol == corev1.ProtocolUDP {
		return "UDP", nil
	} else {
		// This could happen, for example with corev1.ProtocolSTCP
		return "", errors.New(fmt.Sprintf("protocol type %s not supported", serviceProtocol))
	}
}

func getServiceIP(service corev1.Service) (string, error) {
	for _, ingress := range service.Status.LoadBalancer.Ingress {
		if ingress.IP != "" {
			// TODO don't just take the first
			return ingress.IP, nil
		}
	}
	return "", errors.New("no IP available for LoadBalancer (not yet allocated?)")
}

type RouterClient interface {
	AddPortMapping(
		NewRemoteHost string,
		NewExternalPort uint16,
		NewProtocol string,
		NewInternalPort uint16,
		NewInternalClient string,
		NewEnabled bool,
		NewPortMappingDescription string,
		NewLeaseDuration uint32,
	) (err error)

	GetExternalIPAddress() (
		NewExternalIPAddress string,
		err error,
	)
}

func PickRouterClient(ctx context.Context) (RouterClient, error) {
	tasks, _ := errgroup.WithContext(ctx)
	// Request each type of client in parallel, and return what is found.
	var ip1Clients []*internetgateway2.WANIPConnection1
	tasks.Go(func() error {
		var err error
		ip1Clients, _, err = internetgateway2.NewWANIPConnection1Clients()
		return err
	})
	var ip2Clients []*internetgateway2.WANIPConnection2
	tasks.Go(func() error {
		var err error
		ip2Clients, _, err = internetgateway2.NewWANIPConnection2Clients()
		return err
	})
	var ppp1Clients []*internetgateway2.WANPPPConnection1
	tasks.Go(func() error {
		var err error
		ppp1Clients, _, err = internetgateway2.NewWANPPPConnection1Clients()
		return err
	})

	if err := tasks.Wait(); err != nil {
		return nil, err
	}

	// Trivial handling for where we find exactly one device to talk to, you
	// might want to provide more flexible handling than this if multiple
	// devices are found.
	switch {
	case len(ip2Clients) > 0:
		return ip2Clients[0], nil
	case len(ip1Clients) > 0:
		return ip1Clients[0], nil
	case len(ppp1Clients) > 0:
		return ppp1Clients[0], nil
	default:
		return nil, errors.New("No services found")
	}
}

func (r *ServiceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Service{}).
		Complete(r)
}
