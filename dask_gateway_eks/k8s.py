import asyncio
import base64
import enum
import uuid
from typing import Dict, Optional, TypeVar

from fastapi import FastAPI, Request
from kubernetes_asyncio import config as k8s_config
from kubernetes_asyncio.client import (
    CoreV1Api,
    CustomObjectsApi,
    V1ContainerPort,
    V1ObjectMeta,
    V1Pod,
    V1Secret,
    V1Service,
    V1ServicePort,
    V1ServiceSpec,
)
from kubernetes_asyncio.client.api_client import ApiClient

from .config_utils import create_env_var
from .models import Cluster, ClusterOptions, ClusterState

GROUP = "dask-gateway-eks.github.com"
NAMESPACE = "default"

T = TypeVar("T")


class WatchEvent(enum.Enum):
    ADDED = "ADDED"
    MODIFIED = "MODIFIED"
    DELETED = "DELETED"


def setup_k8s_client(app: FastAPI):
    @app.on_event("startup")
    async def setup_client():
        app.extra["k8s_client"] = await create_eks_client()

    @app.on_event("shutdown")
    async def shutdown_client():
        await app.extra["k8s_client"].__aexit__(None, None, None)


def client(request: Request) -> ApiClient:
    return request.app.extra["k8s_client"]


async def create_eks_client() -> ApiClient:
    await k8s_config.load_kube_config()
    return ApiClient()


def _set_labels(definition: V1Pod, labels: Dict[str, str], name: Optional[str] = None):
    if definition.metadata is None:
        definition.metadata = V1ObjectMeta()
    if definition.metadata.labels is None:
        definition.metadata.labels = {}
    definition.metadata.labels.update(labels)
    if name:
        definition.metadata.name = name


def cluster_options_to_k8s_resource(
    k8s_client: ApiClient, name: str, owner: str, options: ClusterOptions
) -> dict:
    worker_definition = options.get_worker_pod_definition()

    worker_definition.spec.restart_policy = "OnFailure"

    scheduler_definition = options.get_scheduler_pod_definition()

    # Add required definitions
    _set_labels(
        scheduler_definition,
        labels={"dask/cluster": name, "dask/role": "scheduler"},
        name=f"scheduler-{name}",
    )
    _set_labels(worker_definition, labels={"dask/cluster": name, "dask/role": "worker"})

    worker_env = worker_definition.spec.containers[0].env
    scheduler_env = scheduler_definition.spec.containers[0].env
    if worker_env is None:
        worker_env = worker_definition.spec.containers[0].env = []
    if scheduler_env is None:
        scheduler_env = scheduler_definition.spec.containers[0].env = []
    scheduler_env.append(create_env_var("DASK_GATEWAY_CLUSTER_NAME", name))
    scheduler_env.append(
        create_env_var("DASK_GATEWAY_API_URL", "http://host.docker.internal:8000/api")
    )
    scheduler_env.append(
        create_env_var("DASK_GATEWAY_API_TOKEN", "/etc/dask-credentials/api-token")
    )
    scheduler_env.append(create_env_var("DASK_LOGGING__DISTRIBUTED", "info"))
    worker_env.append(create_env_var("DASK_LOGGING__DISTRIBUTED", "info"))

    if scheduler_definition.spec.containers[0].ports is None:
        scheduler_definition.spec.containers[0].ports = []

    scheduler_definition.spec.containers[0].ports.extend([
        V1ContainerPort(container_port=8787, name="dashboard"),
        V1ContainerPort(container_port=8786, name="dask"),
        V1ContainerPort(container_port=8788, name="gateway")
    ])

    scheduler_definition.spec.containers[0].args = [
        "dask-scheduler",
        "--host",
        "",
        "--port",
        "8786",
        "--dashboard-address",
        ":8787",
        "--dg-api-address",
        ":8788",
        "--preload",
        "dask_gateway.scheduler_preload",
        "--dg-heartbeat-period",
        "0",
        "--dg-adaptive-period",
        "3",
    ]

    worker_definition.spec.containers[0].args = [
        "dask-worker",
        f"tcp://scheduler-service-{name}:8786",
    ]

    return {
        "apiVersion": f"{GROUP}/v1alpha1",
        "kind": "DaskCluster",
        "metadata": {
            "name": name,
            "labels": {
                "gateway.dask.org/cluster": name,
                "gateway.dask.org/owner": owner,
            },
            "annotations": {},
        },
        "spec": {
            "scheduler_definition": k8s_client.sanitize_for_serialization(
                scheduler_definition
            ),
            "worker_definition": k8s_client.sanitize_for_serialization(
                worker_definition
            ),
            "options": options.dict(),
            "desiredWorkers": 0,
        },
    }


async def create_cluster(k8s_client: ApiClient, options: ClusterOptions, owner: str):
    cluster_name = uuid.uuid4().hex
    definition = cluster_options_to_k8s_resource(
        k8s_client, cluster_name, owner, options
    )
    custom_client = CustomObjectsApi(api_client=k8s_client)
    await custom_client.create_namespaced_custom_object(
        GROUP, "v1alpha1", NAMESPACE, "daskclusters", definition
    )
    return cluster_name


async def wait_for_cluster(k8s_client: ApiClient, name: str):
    while True:
        custom_client = CustomObjectsApi(api_client=k8s_client)
        response = await custom_client.get_namespaced_custom_object(
            GROUP, "v1alpha1", NAMESPACE, "daskclusters", name
        )
        if "status" in response:
            return
        await asyncio.sleep(2)


async def get_cluster(k8s_client: ApiClient, name: str) -> Cluster:
    core_client = CoreV1Api(api_client=k8s_client)
    custom_client = CustomObjectsApi(api_client=k8s_client)
    response = await custom_client.get_namespaced_custom_object(
        GROUP, "v1alpha1", NAMESPACE, "daskclusters", name
    )
    api_token, tls_cert, tls_key = None, None, None

    if response["status"].get("authSecretName"):
        # Get auth secret value
        secret_data: V1Secret = await core_client.read_namespaced_secret(
            name=response["status"]["authSecretName"], namespace=NAMESPACE
        )
        api_token = base64.b64decode(secret_data.data["api-token"])
        tls_cert = base64.b64decode(secret_data.data["ca.crt"])
        tls_key = base64.b64decode(secret_data.data["tls.key"])

    return Cluster(
        name=name,
        owner=response["metadata"]["labels"]["gateway.dask.org/owner"],
        status=ClusterState(response["status"]["clusterState"]),
        options=response["spec"]["options"],
        api_token=api_token,
        tls_cert=tls_cert,
        tls_key=tls_key,
    )


async def stop_cluster(k8s_client: ApiClient, name: str):
    custom_client = CustomObjectsApi(api_client=k8s_client)
    await custom_client.patch_namespaced_custom_object_status(
        GROUP,
        "v1alpha1",
        NAMESPACE,
        "daskclusters",
        name,
        [
            {
                "op": "add",
                "path": "/status/clusterState",
                "value": ClusterState.STOPPING.value,
            }
        ],
    )


async def set_cluster_replicas(k8s_client: ApiClient, name: str, count: int):
    custom_client = CustomObjectsApi(api_client=k8s_client)
    await custom_client.patch_namespaced_custom_object(
        GROUP,
        "v1alpha1",
        NAMESPACE,
        "daskclusters",
        name,
        [{"op": "add", "path": "/spec/desiredWorkers", "value": count}],
    )
