import asyncio
import time
import uuid
from http import HTTPStatus
from importlib.metadata import version as package_version
from typing import List, Optional, Union

import aiohttp
from fastapi import Depends, FastAPI
from pydantic import BaseModel
from starlette.requests import Request
from starlette.responses import Response

from . import k8s, scheduler_coms
from .cluster import KubernetesCluster
from .models import ClusterOptions, ClusterResponse

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
    return {
        "name": await k8s.create_cluster(
            client, cluster_options.cluster_options, owner="tom"
        )
    }


@app.delete("/api/v1/clusters/{cluster_name}")
async def stop_cluster(cluster_name: str, client: k8s.ApiClient = Depends(k8s.client)):
    await k8s.stop_cluster(client, cluster_name)
    return Response(status_code=HTTPStatus.NO_CONTENT)


@app.post("/api/v1/clusters/{cluster_name}/scale")
async def scale_cluster(
    cluster_name: str, count: int, k8s_client: k8s.ApiClient = Depends(k8s.client),
    http_client: aiohttp.ClientSession = Depends(scheduler_coms.client),
):
    # To-Do: This should obey the `max-worker` configuration option
    cluster = await k8s.get_cluster(k8s_client, cluster_name)
    await scheduler_coms.send_to_scheduler(cluster, http_client, {
        "op": "scale",
        "count": count
    })
    return {
        "ok": True,
        "msg": "to-do"
    }


@app.post("/sync")
async def sync(
    parent: dict, children: dict, client: k8s.ApiClient = Depends(k8s.client)
):
    cluster_state = KubernetesCluster(parent, children, client)
    return cluster_state.progress()


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
    http_client: aiohttp.ClientSession = Depends(scheduler_coms.client),
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
    cluster_name: str,
    heartbeat: SchedulerHeartbeat,
    client: k8s.ApiClient = Depends(k8s.client),
):
    count = heartbeat.count
    await k8s.set_cluster_replicas(client, cluster_name, count)
    return {}
