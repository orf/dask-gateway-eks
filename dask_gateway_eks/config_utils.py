from typing import Dict, Union, Optional, Any

from kubernetes_asyncio.client import (
    V1Pod,
    V1PodSpec,
    V1Container,
    V1EnvVar,
    V1EnvVarSource,
    V1ResourceRequirements,
    V1ObjectMeta,
)


def create_pod_definition(
    *,
    image: str,
    env: Optional[Dict[str, Union[str, V1EnvVarSource]]] = None,
    resource_requests: Optional[Dict[str, str]] = None,
    resource_limits: Optional[Dict[str, str]] = None,
    annotations: Optional[Dict[str, str]] = None,
    labels: Optional[Dict[str, str]] = None,
) -> V1Pod:
    container = V1Container(name="dask-eks", image=image, image_pull_policy="Never")
    if env:
        container.env = [create_env_var(key, value) for key, value in env.items()]
    if resource_requests or resource_limits:
        container.resources = V1ResourceRequirements(
            limits=resource_limits, requests=resource_requests
        )
    spec = V1PodSpec(containers=[container])
    return V1Pod(
        api_version="v1",
        kind="Pod",
        metadata=V1ObjectMeta(annotations=annotations, labels=labels),
        spec=spec,
    )


def create_env_var(key: str, value: Union[str, V1EnvVarSource]) -> V1EnvVar:
    if isinstance(value, str):
        return V1EnvVar(name=key, value=value)
    else:
        return V1EnvVar(name=key, value_from=value)