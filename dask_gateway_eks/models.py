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
    image: str = "dask"

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


class Cluster(BaseModel):
    name: str
    owner: str
    uid: str

    status: ClusterState

    start_time: Optional[str]
    stop_time: Optional[str]

    options: Dict[str, Any]
    scheduler_definition: V1Pod
    worker_definition: V1Pod

    class Config:
        arbitrary_types_allowed = True
