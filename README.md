# pg-idle-operator

Built to demonstrate the intersection of Go operator patterns and PostgreSQL internals.
## Description
A Kubernetes operator that auto-pauses idle PostgreSQL instances by scaling their StatefulSets to zero replicas — retaining PVC data while eliminating compute cost for inactive tenants.

## How it works

```
PostgresInstance CR created
         ↓
Operator creates StatefulSet + headless Service (with owner references)
         ↓
Every 60s: queries pg_stat_activity for active connection count
         ↓
Updates status.phase, status.activeConnections, status.lastActiveTime
         ↓
If idle for >= idleTimeoutMinutes → patches StatefulSet replicas to 0
         ↓
PVC retained — data survives. Pod terminated — compute freed.
```
## Getting Started

### Prerequisites
- go version v1.24.6+
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.

### To Deploy on the cluster
**Build your image to the location specified by `IMG`:**

```sh
# Build and load the image into the cluster
make docker-build IMG=pg-idle-operator:dev
```

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=pg-idle-operator:dev
```

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin
privileges or be logged in as admin.

**Create instances of your solution**
You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -k config/samples/tenant-db.yaml
```

>**NOTE**: Ensure that the samples has default values to test it out.

### To Uninstall
**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/tenant-db.yaml
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**UnDeploy the controller from the cluster:**

```sh
make undeploy
```

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `spec.version` | string | required | Postgres major version (`"16"`) |
| `spec.storage` | string | required | PVC size (`"10Gi"`) |
| `spec.idleTimeoutMinutes` | int | `10` | Minutes of zero connections before pausing |
| `spec.paused` | bool | `false` | Manually freeze reconciliation |


## To run the project locally

### Create the tenants namespace


```sh
# create a namespace
kubectl create namespace tenants
```

**Add the lib/pq Postgres driver dependency:**

```sh
go get github.com/lib/pq@v1.10.9
go mod tidy
```


**Install the CRDs into the cluster:**

```sh
make install
```

**check the CRD into your cluster:**

```sh
 kubectl get crds | grep odilon
```

**Create instances of your solution**

You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -k config/samples/tenant-db.yaml
```
Run the operator locally (kubebuilder's make target)
**Terminal 1**

```sh
make run
```

**make sure the service name is portforwarded:**
**Terminal 2**
```sh
kubectl port-forward pod/tenant-acme-0 5432:5432 -n tenants
```

### To Uninstall
**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/tenant-db.yaml
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

## Observing state

```bash
# Current phase + connection count
kubectl get postgresinstances -n tenants

# Full status
kubectl describe postgresinstance tenant-acme -n tenants

# Operator logs
kubectl logs -l control-plane=pg-idle-operator -f
```


## Contributing
any one is free to contribute

**NOTE:** Run `make help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Author

**Irambona Odilon Vaillant**
[github.com/odilon-cloud](https://github.com/odilon-cloud)
