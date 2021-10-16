import collections
from http import HTTPStatus
import uuid
from collections import defaultdict
from typing import Optional

from fastapi import FastAPI, Depends

from importlib.metadata import version as package_version

from starlette.requests import Request
from starlette.responses import Response

from . import k8s
from .models import ClusterOptions, Cluster, ClusterState

app = FastAPI()
k8s.setup_k8s_client(app)


@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/api/version")
def version():
    return {"version": package_version("dask-gateway-eks")}


@app.post("/api/v1/clusters/")
async def create_cluster(
    cluster_options: ClusterOptions, client: k8s.ApiClient = Depends(k8s.client)
):
    return await k8s.create_cluster(client, cluster_options, owner="tom")


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
        name = f'worker-{cluster_name}-{uuid.uuid4().hex[:6]}'
    metadata["name"] = name
    return {
        **worker_definition,
        "metadata": metadata
    }


@app.post("/sync")
async def sync(parent: dict, children: dict):
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

    # Pending clusters progress to RUNNING or FAILED.
    if cluster_state == ClusterState.PENDING:
        # To progress to RUNNING we need a Scheduler pod alive. If we don't yet have a scheduler, lets create one
        if not scheduler_pod:
            print("Creating Scheduler pod")
            return {
                "status": {"clusterState": ClusterState.PENDING},
                "children": [scheduler_definition, scheduler_service_definition],
            }

        # Is our scheduler pod running? If so, our cluster is running!
        if scheduler_pod["status"]["phase"] == "Running":
            print("Cluster scheduler running")
            return {
                "status": {"clusterState": ClusterState.RUNNING},
                "children": [scheduler_definition, scheduler_service_definition],
            }
        elif scheduler_pod["status"]["phase"] == "Pending":
            # If it's pending, then the cluster is still pending
            print("Cluster scheduler pending")
            return {
                "status": {"clusterState": ClusterState.PENDING},
                "children": [scheduler_definition, scheduler_service_definition],
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
        new_worker_definitions = [_get_worker(worker_definition, worker["metadata"]["name"]) for worker in worker_pods]
        if desired_workers > len(new_worker_definitions):
            # We need to add workers
            print("Adding workers")
            new_worker_definitions.extend([_get_worker(worker_definition)
                                           for _ in range(desired_workers - len(new_worker_definitions))])
        else:
            print("Removing workers")
            new_worker_definitions = new_worker_definitions[:desired_workers]
        print(len(new_worker_definitions))
        return {
            "status": {
                "clusterState": ClusterState.RUNNING,
                "desiredWorkers": desired_workers,
            },
            "children": [scheduler_definition, scheduler_service_definition] + new_worker_definitions,
        }

    raise RuntimeError(f"Unknown status {cluster_state}")


#
# @app.get("/api/v1/clusters/{cluster_name}")
# async def get_cluster(
#     cluster_name: str, client: k8s.ApiClient = Depends(k8s.client)
# ) -> Cluster:
#     return await k8s.get_cluster(client, cluster_name)
#
#


#

#
# @app.post("/api/v1/clusters/{cluster_name}/adapt")
# async def adapt_cluster(
#     cluster_name: str,
#     minimum: int,
#     maximum: int,
#     active: bool,
#     client: k8s.ApiClient = Depends(k8s.client),
# ):
#     cluster = await k8s.get_cluster(client, cluster_name)
#     await cluster_coms.adapt_cluster(cluster, minimum, maximum, active)
#
#


@app.post("/api/v1/clusters/{cluster_name}/heartbeat")
async def cluster_heartbeat(
    cluster_name: str,
    count: int,
    active_workers: int,
    closing_workers: int,
    closed_workers: int,
    client: k8s.ApiClient = Depends(k8s.client),
):
    # cluster = await k8s.get_cluster(client, cluster_name)
    await k8s.set_cluster_replicas(client, cluster_name, count)
