import asyncio
import base64
import collections
import time
from http import HTTPStatus
import uuid
from typing import Optional, List, Union

import httpx
from fastapi import FastAPI, Depends, Query

from importlib.metadata import version as package_version

from kubernetes_asyncio.client import V1Secret, V1ObjectMeta
from pydantic import BaseModel
from starlette.requests import Request
from starlette.responses import Response

from . import k8s, scheduler_coms
from .cluster import KubernetesCluster
from .models import ClusterOptions, ClusterState, ClusterResponse

app = FastAPI()
k8s.setup_k8s_client(app)
scheduler_coms.setup_http_client(app)


@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/api/version")
def version():
    return {"version": package_version("dask-gateway-eks")}


class ClusterOptionsRequest(BaseModel):
    cluster_options: ClusterOptions


@app.post("/api/v1/clusters/")
async def create_cluster(
    cluster_options: ClusterOptionsRequest, client: k8s.ApiClient = Depends(k8s.client)
):
    return {"name": await k8s.create_cluster(client, cluster_options.cluster_options, owner="tom")}


@app.delete("/api/v1/clusters/{cluster_name}")
async def stop_cluster(cluster_name: str, client: k8s.ApiClient = Depends(k8s.client)):
    await k8s.stop_cluster(client, cluster_name)
    return Response(status_code=HTTPStatus.NO_CONTENT)


@app.post("/api/v1/clusters/{cluster_name}/scale")
async def scale_cluster(
    cluster_name: str, count: int, client: k8s.ApiClient = Depends(k8s.client)
):
    return await k8s.set_cluster_replicas(client, cluster_name, count)
    # cluster = await k8s.get_cluster(client, cluster_name)
    # await cluster_coms.scale_cluster(cluster, count)


def _get_worker(worker_definition: dict, name: Optional[str] = None):
    metadata = worker_definition["metadata"].copy()
    if not name:
        cluster_name = metadata["labels"]["dask/cluster"]
        name = f"worker-{cluster_name}-{uuid.uuid4().hex[:6]}"
    metadata["name"] = name
    return {**worker_definition, "metadata": metadata}


@app.post("/sync")
async def sync(
    parent: dict, children: dict, client: k8s.ApiClient = Depends(k8s.client)
):
    cluster_state = KubernetesCluster(parent, children, client)
    return cluster_state.progress()


@app.post("/sync2")
async def sync2(
    parent: dict, children: dict, client: k8s.ApiClient = Depends(k8s.client)
):
    # If we have no clusterState in the parent, we are a brand new cluster. Set
    # the state to pending.
    if "clusterState" not in parent.get("status", {}):
        return {"status": {"clusterState": ClusterState.PENDING}}

    cluster_state = ClusterState(parent["status"]["clusterState"])
    print(f"{cluster_state=}")
    # If we're a failed or stopped cluster, do nothing
    if cluster_state in (ClusterState.FAILED, ClusterState.STOPPED):
        return {
            "status": {"clusterState": cluster_state},
            "children": [],
            "resyncAfterSeconds": 0,
        }

    pods = children["Pod.v1"].values()
    schedulers = [
        pod for pod in pods if pod["metadata"]["labels"]["dask/role"] == "scheduler"
    ]
    # We should never have more than one scheduler.
    scheduler_pod = None if not schedulers else schedulers[0]
    worker_pods = [
        pod for pod in pods if pod["metadata"]["labels"]["dask/role"] == "worker"
    ]

    workers_by_status = collections.defaultdict(list)
    for pod in worker_pods:
        workers_by_status[pod["status"]["phase"]].append(pod)

    worker_definition = parent["spec"]["worker_definition"]
    scheduler_definition = parent["spec"]["scheduler_definition"]
    scheduler_service_definition = parent["spec"]["scheduler_service_definition"]

    cluster_name = parent["metadata"]["name"]

    child_secrets = children["Secret.v1"]
    if not child_secrets:
        from .certificates import new_keypair

        cert_bytes, key_bytes = new_keypair(f"daskgateway-{cluster_name}")
        secret = client.sanitize_for_serialization(
            V1Secret(
                api_version="v1",
                kind="Secret",
                metadata=V1ObjectMeta(name=f"dask-cluster-{cluster_name}"),
                data={
                    "api-token": base64.b64encode(uuid.uuid4().hex.encode()).decode(),
                    "tls.ca": base64.b64encode(cert_bytes).decode(),
                    "tls.crt": base64.b64encode(cert_bytes).decode(),
                    "tls.key": base64.b64encode(key_bytes).decode(),
                },
            )
        )
    else:
        secret = list(child_secrets.values())[0]

    tls_options = children["TLSOption.traefik.containo.us/v1alpha1"]
    if not tls_options:
        tls_options = {
            "apiVersion": "traefik.containo.us/v1alpha1",
            "kind": "TLSOption",
            "metadata": {"name": f"dask-cluster-{cluster_name}"},
            "spec": {
                "clientAuth": {
                    "secretNames": [secret["metadata"]["name"]],
                    "clientAuthType": "RequireAndVerifyClientCert"
                    # "clientAuthType": "NoClientCert"
                },
                "sniStrict": True,
            },
        }
    else:
        tls_options = list(tls_options.values())[0]

    traefik_ingresses = []

    ingresses = children["IngressRouteTCP.traefik.containo.us/v1alpha1"]
    if not ingresses:
        traefik_ingresses.append(
            {
                "apiVersion": "traefik.containo.us/v1alpha1",
                "kind": "IngressRouteTCP",
                "metadata": {"name": f"dask-cluster-{cluster_name}"},
                "spec": {
                    "routes": [
                        {
                            "match": f"HostSNI(`daskgateway-{cluster_name}`)",
                            "services": [
                                {
                                    "name": scheduler_service_definition["metadata"][
                                        "name"
                                    ],
                                    "port": "dask",
                                }
                            ],
                        },
                    ],
                    "tls": {
                        "options": {"name": tls_options["metadata"]["name"]},
                        "secretName": tls_options["metadata"]["name"],
                        "domains": [{"main": f"daskgateway-{cluster_name}"}],
                    },
                },
            }
        )
    else:
        traefik_ingresses = list(ingresses.values())

    dev_ingresses = children["IngressRoute.traefik.containo.us/v1alpha1"]
    if not dev_ingresses:
        traefik_ingresses.append(
            {
                "apiVersion": "traefik.containo.us/v1alpha1",
                "kind": "IngressRoute",
                "metadata": {"name": f"dask-cluster-{cluster_name}"},
                "spec": {
                    "routes": [
                        {
                            "kind": "Rule",
                            "match": f"PathPrefix(`/gateway-api/{cluster_name}/`)",
                            "middlewares": [{"name": "strip-dask-prefix"}],
                            "services": [
                                {
                                    "name": scheduler_service_definition["metadata"][
                                        "name"
                                    ],
                                    "port": "gateway",
                                }
                            ],
                        },
                        {
                            "kind": "Rule",
                            "match": f"PathPrefix(`/dashboard/{cluster_name}/`)",
                            "middlewares": [{"name": "strip-dask-prefix"}],
                            "services": [
                                {
                                    "name": scheduler_service_definition["metadata"][
                                        "name"
                                    ],
                                    "port": "dashboard",
                                }
                            ],
                        },
                    ]
                },
            }
        )
    else:
        traefik_ingresses.extend(dev_ingresses.values())

    auth_secret_name = secret["metadata"]["name"]

    for definition in (scheduler_definition, worker_definition):
        mounts = definition["spec"]["containers"][0].setdefault("volumeMounts", [])
        mounts.append(
            {
                "name": "dask-credentials",
                "mountPath": "/etc/dask-credentials/",
                "readOnly": True,
            }
        )
        volumes = definition["spec"].setdefault("volumes", [])
        volumes.append(
            {"name": "dask-credentials", "secret": {"secretName": auth_secret_name}}
        )

    # Pending clusters progress to RUNNING or FAILED.
    if cluster_state == ClusterState.PENDING:
        # To progress to RUNNING we need a Scheduler pod alive. If we don't yet have a scheduler, lets create one
        if not scheduler_pod:
            print("Creating Scheduler pod")
            return {
                "status": {"clusterState": ClusterState.PENDING},
                "children": [
                    secret,
                    scheduler_definition,
                    scheduler_service_definition,
                    tls_options,
                    *traefik_ingresses,
                ],
            }

        # Is our scheduler pod running? If so, our cluster is running!
        if scheduler_pod["status"]["phase"] == "Running":
            print("Cluster scheduler running")
            return {
                "status": {
                    "clusterState": ClusterState.RUNNING,
                    "authSecretName": auth_secret_name,
                    "localSchedulerPorts": scheduler_k8s_service_ports,
                },
                "children": [
                    secret,
                    scheduler_definition,
                    scheduler_service_definition,
                    tls_options,
                    *traefik_ingresses,
                ],
            }
        elif scheduler_pod["status"]["phase"] == "Pending":
            # If it's pending, then the cluster is still pending
            print("Cluster scheduler pending")
            return {
                "status": {"clusterState": ClusterState.PENDING},
                "children": [
                    secret,
                    scheduler_definition,
                    scheduler_service_definition,
                    tls_options,
                    *traefik_ingresses,
                ],
            }
        else:
            # The scheduler pod has failed to start, therefore the cluster has failed
            print("Cluster scheduler failed")
            return {"status": {"clusterState": ClusterState.FAILED}, "children": []}

    # STOPPING clusters progress to STOPPED only if we have no workers or schedulers running
    if cluster_state == ClusterState.STOPPING:
        if not pods:
            cluster_state = ClusterState.STOPPED
        return {"status": {"clusterState": cluster_state}, "children": []}

    # Running clusters progress to STOPPING or FAILED, depending on the status of the workers + scheduler
    if cluster_state == ClusterState.RUNNING:
        if not (scheduler_pod and scheduler_pod["status"]["phase"] == "Running"):
            # The scheduler has failed!
            return {"status": {"clusterState": ClusterState.FAILED}, "children": []}

        # Otherwise just create that many workers
        desired_workers = parent["spec"]["desiredWorkers"]
        print(f"Desired workers:", desired_workers)
        # We need a concrete "name" for each worker that must be unique.
        running_worker_definitions = [
            _get_worker(worker_definition, worker["metadata"]["name"])
            for worker in workers_by_status["Running"]
        ]
        pending_worker_definitions = [
            _get_worker(worker_definition, worker["metadata"]["name"])
            for worker in workers_by_status["Pending"]
        ]
        new_worker_definitions = []
        total_workers = len(pending_worker_definitions) + len(
            running_worker_definitions
        )
        print("total workers", total_workers)
        if desired_workers > total_workers:
            # We need to add workers
            print("Adding workers")
            new_worker_definitions.extend(
                [
                    _get_worker(worker_definition)
                    for _ in range(desired_workers - len(new_worker_definitions))
                ]
            )
        elif desired_workers < total_workers:
            difference = total_workers - desired_workers
            print("Removing workers", difference)
            # Attempt to reduce pending pods
            if difference > len(pending_worker_definitions):
                pending_worker_definitions = []
            else:
                pending_worker_definitions = pending_worker_definitions[
                    : len(pending_worker_definitions) - difference
                ]
            print("After removal ", len(pending_worker_definitions))
            # pending_worker_definitions =
            # print("pending workers", len(pending_worker_definitions))
        print(workers_by_status.keys())
        all_worker_definitions = [
            *new_worker_definitions,
            *running_worker_definitions,
            *pending_worker_definitions,
        ]
        return {
            "status": {
                "clusterState": ClusterState.RUNNING,
                "localSchedulerPorts": scheduler_k8s_service_ports,
                "authSecretName": auth_secret_name,
            },
            "children": [
                secret,
                scheduler_definition,
                scheduler_service_definition,
                tls_options,
                *traefik_ingresses,
                *all_worker_definitions,
            ],
        }

    raise RuntimeError(f"Unknown status {cluster_state}")


#
@app.get("/api/v1/clusters/{cluster_name}", response_model=ClusterResponse)
async def get_cluster(
    cluster_name: str,
    wait: Union[bool, str] = False,
    client: k8s.ApiClient = Depends(k8s.client),
) -> ClusterResponse:
    if wait == "":
        wait = True

    if wait:
        try:
            await asyncio.wait_for(
                k8s.wait_for_cluster(client, cluster_name), timeout=20
            )
        except asyncio.TimeoutError:
            raise Exception("timeout!")

    cluster = await k8s.get_cluster(client, cluster_name)

    return ClusterResponse(
        name=cluster.name,
        dashboard_route=f"/dashboard/{cluster_name}/status",
        status=cluster.status,
        start_time=time.time(),
        end_time=None,
        options=cluster.options,
        tls_key=cluster.tls_key,
        tls_cert=cluster.tls_cert,
    )


class AdaptCluster(BaseModel):
    minimum: int
    maximum: int
    active: bool


@app.post("/api/v1/clusters/{cluster_name}/adapt")
async def adapt_cluster(
    cluster_name: str,
    adapt: AdaptCluster,
    k8s_client: k8s.ApiClient = Depends(k8s.client),
    http_client: httpx.AsyncClient = Depends(scheduler_coms.client),
):
    cluster = await k8s.get_cluster(k8s_client, cluster_name)
    await scheduler_coms.send_to_scheduler(
        cluster,
        http_client,
        {
            "op": "adapt",
            "minimum": adapt.minimum,
            "maximum": adapt.maximum,
            "active": adapt.active,
        },
    )
    return {"ok": True}


class SchedulerHeartbeat(BaseModel):
    count: int
    active_workers: List[str]
    closing_workers: List[str]
    closed_workers: List[str]


@app.post("/api/v1/clusters/{cluster_name}/heartbeat")
async def cluster_heartbeat(
    request: Request,
    cluster_name: str,
    heartbeat: SchedulerHeartbeat,
    # count: int,
    # active_workers: int,
    # closing_workers: int,
    # closed_workers: int,
    client: k8s.ApiClient = Depends(k8s.client),
):
    # cluster = await k8s.get_cluster(client, cluster_name)
    count = heartbeat.count
    # await k8s.set_cluster_workers(client, cluster_name, )
    await k8s.set_cluster_replicas(client, cluster_name, count)
    return {}
