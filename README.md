# Holepunch

Configure UPnP routers to port-forward to Kubernetes services on your local network.

## Use Case

If you run a Kubernetes cluster behind a NAT router (e.g., on a home network) you might use a service such as [MetalLB](https://metallb.universe.tf/) to provide local-network IP addresses to your services.
But you still need to configure your router's "port forward" feature to forward traffic from the open internet (assuming you either have a static IP or dynamic DNS of some kind) to that local service IP.

This typically requires you to cordinate IP addresses, set `spec.loadBalancerIP` and hope that no other service used it first, and then configure your router manually.
Holepunch automates this process, and configures your router using UPnP to whatever the local network IP is.

## Usage

Deploy Holepunch into your cluster.
A container image is available at `ghcr.io/jameslaverack/holepunch`.
You can use the provided Makefile to produce the YAML and deploy to your current kube config.

For example, to deploy version `v0.1.0`:
```bash
export IMG='ghcr.io/jameslaverack/holepunch:v0.1.0'
make deploy
```

Holepunch requires the `KUBERNETES_NODENAME` environment variable to be set to the name of the node it is running on.
This is used to determine the node's IP address for NodePort services and to update node annotations.
You can set this in your pod spec using the Downward API:
```yaml
env:
  - name: KUBERNETES_NODENAME
    valueFrom:
      fieldRef:
        fieldPath: spec.nodeName
```

Once Holepunch is deployed, annotate services of type `LoadBalancer` or `NodePort` with `holepunch/punch-external: "true"`.
Holepunch will then configure your router over UPnP to forward the service's ports to the service's IP (for `LoadBalancer` services) or the node's IP and node port (for `NodePort` services).

### Using Different External Ports

If you want to expose a different port on your router than the Kubernetes service port, you can map this with an annotation.
Holepunch looks for annotations with the prefix `holepunch.port/`, followed by the service's port number.
The value of this annotation is the desired external port.
Note that annotations must have string YAML values, so the external port number must be templated as a string.

For example, if a service exposes port 80, the annotation `holepunch.port/80: "3000"` could be used.
This would cause Holepunch to make a UPnP mapping from an external port 3000 to port 80 on the local network.

### NodePort Services

Holepunch supports both `LoadBalancer` and `NodePort` service types.
For `NodePort` services, Holepunch maps the node's IP address and the assigned node port to the router, rather than the service's cluster IP.

If `spec.externalTrafficPolicy` is set to `Local` on a `NodePort` service, Holepunch will only configure the UPnP mapping if a pod selected by the service is running on the same node as the Holepunch controller.
This ensures traffic is only forwarded to nodes that can serve it locally without an extra hop.

### Force NodePort Mode

Some routers (e.g., pfSense) operate in "secure mode", which prevents a device from mapping ports to IP addresses other than its own.
This breaks `LoadBalancer` type mappings completely, since the service IP is different from the Holepunch pod's IP.

To work around this, you can enable force NodePort mode with the `--force-nodeport` flag.
When enabled, Holepunch will treat `LoadBalancer` services as if they were `NodePort` services, mapping the node's IP and node port instead of the service's external IP.

### Configuration Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--metrics-addr` | `:8080` | The address the metrics endpoint binds to. |
| `--enable-leader-election` | `false` | Enable leader election to ensure only one active controller manager. |
| `--external-ip-annotation` | `""` | If set, the controller will annotate the current node with the external IP discovered from the router, using this annotation key. |
| `--force-nodeport` | `false` | Treat `LoadBalancer` services as `NodePort` services, mapping the node's IP and node port. Useful for routers with secure mode enabled. |
| `--lease-duration-seconds` | `3600` | The duration in seconds for which the UPnP port mapping is created. The controller will re-create the mapping before it expires. |

### Annotating Nodes with External IP

Holepunch can automatically annotate the Kubernetes node it runs on with the external IP address discovered from the router.
This is useful for integrating with other tools that need to know the node's public IP.

To enable this, set the `--external-ip-annotation` flag to the annotation key you want to use:
```bash
--external-ip-annotation=example.com/external-ip
```

Holepunch will then keep the annotation on the node up to date with the router's external IP address.

## Limitations

- Only `LoadBalancer` and `NodePort` services are supported.
- Some routers won't allow some ports (such as 80 and 443) to be configured over UPnP.
- Holepunch can't handle more than one router on your network.
- To work inside your Kubernetes cluster, the holepunch Pod must bind to the host network and expose some UDP ports.
  This means that no more than one holepunch pod can run at once, and no other UPnP services can work at the same time on the same cluster.
- When using leader election with NodePort mode, only the node running the leader pod will be used for port mapping.
  If `externalTrafficPolicy` is set to `Local` and no pods run on the leader node, the service will not be mapped.
  Consider using pod anti-affinity rules based on node region/zone labels to control scheduling when not using leader election.

