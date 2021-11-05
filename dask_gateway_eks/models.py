from enum import Enum
from typing import Dict, Optional, Any
from . import config_utils

from kubernetes_asyncio.client import V1PodSpec, V1Pod
from pydantic import BaseModel, Field


class ClusterState(str, Enum):
    PENDING = "Pending"
    RUNNING = "Running"
    STOPPING = "Stopping"
    STOPPED = "Stopped"
    FAILED = "Failed"


class Resources(BaseModel):
    memory: Optional[int]
    cores: Optional[int]
    gpus: Optional[int]

    def to_requests(self) -> Dict[str, str]:
        result = {}
        if self.memory:
            result["memory"] = str(self.memory)
        if self.cores:
            result["cpu"] = str(self.cores)
        if self.gpus:
            result["nvidia.com/gpu"] = str(self.gpus)
        return result


class ClusterOptions(BaseModel):
    image: str = "dask-eks"

    min_workers: Optional[int]
    max_workers: Optional[int]

    environment: Optional[Dict[str, str]]

    worker_resources: Optional[Resources]
    scheduler_resources: Optional[Resources]

    def get_scheduler_pod_definition(self) -> V1Pod:
        return config_utils.create_pod_definition(
            image=self.image,
            resource_requests=self.scheduler_resources.to_requests()
            if self.scheduler_resources
            else None,
            env=self.environment or {},
        )

    def get_worker_pod_definition(self) -> V1Pod:
        return config_utils.create_pod_definition(
            image=self.image,
            resource_requests=self.worker_resources.to_requests()
            if self.scheduler_resources
            else None,
            env=self.environment or {},
        )


class LocalPorts(BaseModel):
    dask: int
    dashboard: int
    gateway: int


class Cluster(BaseModel):
    name: str
    owner: str
    status: ClusterState
    options: Dict[str, Any]
    local_ports: Optional[LocalPorts]

    api_token: Optional[str]
    tls_cert: Optional[str]
    tls_key: Optional[str]


class ClusterResponse(BaseModel):
    name: str
    dashboard_route: str
    status: ClusterState
    start_time: Optional[int]
    stop_time: Optional[int]
    options: Dict[str, Any]

    # token: Optional[str]
    tls_cert: Optional[str]
    tls_key: Optional[str]
