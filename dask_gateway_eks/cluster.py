import base64
import enum
import logging
import uuid
from functools import cached_property
from typing import Dict, Iterable, List, Optional, TypedDict

from kubernetes_asyncio.client import (
    V1ObjectMeta,
    V1Secret,
    V1Service,
    V1ServicePort,
    V1ServiceSpec,
)

from .certificates import new_keypair
from .k8s import ApiClient
from .models import ClusterState

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


class ClusterCreationState(str, enum.Enum):
    CREATING_SECRETS = "CreatingSecrets"
    CREATING_TLS_CONFIG = "CreatingTLSConfig"
    CREATING_SCHEDULER = "CreatingScheduler"
    CREATING_INGRESSES = "CreatingIngresses"


class CRDStatus(TypedDict, total=False):
    clusterState: ClusterState
    clusterCreationState: Optional[ClusterCreationState]
    authSecretName: Optional[str]


class NewState(TypedDict):
    status: CRDStatus
    children: List[dict]


class KubernetesCluster:
    cluster_definition: dict
    children: Dict[str, Dict[str, dict]]
    state: ClusterState
    creation_state: Optional[ClusterCreationState]
    k8s_client: ApiClient

    def __init__(
        self,
        cluster_definition: dict,
        children: Dict[str, Dict[str, dict]],
        k8s_client: ApiClient,
    ):
        self.cluster_definition = cluster_definition
        self.children = children
        self.k8s_client = k8s_client

        self.cluster_name = cluster_definition["metadata"]["name"]
        # Brand new clusters do not have a state yet. Set it as PENDING
        if "clusterState" not in cluster_definition.get("status", {}):
            self.state = ClusterState.PENDING
        else:
            self.state = ClusterState(cluster_definition["status"]["clusterState"])

        # Clusters that are CREATING have a sub status depending on which resource is being created
        if cluster_definition.get("status", {}).get("clusterCreationState"):
            self.creation_state = ClusterCreationState(
                cluster_definition["status"]["clusterCreationState"]
            )
        else:
            self.creation_state = None

    def progress(self) -> NewState:
        new_state = self._progress()
        logger.info("Old state: %s, new state: %s", self.state, new_state["status"])
        return new_state

    def _progress(self) -> NewState:
        state = self.state
        # If the cluster has failed or stopped, then we don't want any children.
        # If the cluster is currently stopping, we progress to STOPPED if we have no children.
        if self.state in (
            ClusterState.FAILED,
            ClusterState.STOPPED,
            ClusterState.STOPPING,
        ):
            has_any_children = any(self.children.values())
            if not has_any_children and self.state == ClusterState.STOPPING:
                state = ClusterState.STOPPED
            return NewState(status=CRDStatus(clusterState=state), children=[])

        secret_definition = self.get_single_minimal_definition("Secret.v1")
        tls_definition = self.get_single_minimal_definition(
            "TLSOption.traefik.containo.us/v1alpha1"
        )
        service_definition = self.get_single_minimal_definition("Service.v1")
        scheduler_definition = (
            get_minimal_definition(self.scheduler_pod) if self.scheduler_pod else None
        )
        tcp_ingress_definition = self.get_single_minimal_definition(
            "IngressRouteTCP.traefik.containo.us/v1alpha1"
        )
        http_ingress_definition = self.get_single_minimal_definition(
            "IngressRoute.traefik.containo.us/v1alpha1"
        )

        if self.state == ClusterState.PENDING:
            # A cluster is pending until we have set up the required infrastructure for the scheduler to run and
            # for external clients to connect to it. This is done in sequence.
            # First we create our cluster secret:
            if secret_definition is None:
                return NewState(
                    status=CRDStatus(
                        clusterState=ClusterState.PENDING,
                        clusterCreationState=ClusterCreationState.CREATING_SECRETS,
                    ),
                    children=[
                        get_secret_definition(self.cluster_name, self.k8s_client)
                    ],
                )

            # Then we create our TLS definition using the secret we created above
            if tls_definition is None:
                tls_definition = get_tls_options(
                    self.cluster_name, secret_definition["metadata"]["name"]
                )
                return NewState(
                    status=CRDStatus(
                        clusterState=ClusterState.PENDING,
                        clusterCreationState=ClusterCreationState.CREATING_TLS_CONFIG,
                        authSecretName=secret_definition["metadata"]["name"],
                    ),
                    children=[secret_definition, tls_definition],
                )

            # Next we create our scheduler pod and service, and wait until the scheduler is not pending
            if (
                service_definition is None
                or self.scheduler_pod is None
                or pod_status(self.scheduler_pod) == "Pending"
            ):
                scheduler_definition = self.create_scheduler_definition(
                    secret_definition["metadata"]["name"]
                )
                service_definition = get_dask_service_definition(
                    self.cluster_name, self.k8s_client
                )
                return NewState(
                    status=CRDStatus(
                        clusterState=ClusterState.PENDING,
                        clusterCreationState=ClusterCreationState.CREATING_SCHEDULER,
                        authSecretName=secret_definition["metadata"]["name"],
                    ),
                    children=[
                        secret_definition,
                        tls_definition,
                        service_definition,
                        scheduler_definition,
                    ],
                )

            # Then finally we create our ingresses
            if not tcp_ingress_definition or not http_ingress_definition:
                assert self.scheduler_pod_name is not None

                tcp_ingress_definition = get_traefik_tcp_ingress_route(
                    self.cluster_name,
                    self.scheduler_pod_name,
                    tls_definition["metadata"]["name"],
                    secret_definition["metadata"]["name"],
                )
                http_ingress_definition = get_traefik_http_ingress_routes(
                    self.cluster_name, self.scheduler_pod_name
                )
                assert scheduler_definition is not None

                return NewState(
                    status=CRDStatus(
                        clusterState=ClusterState.PENDING,
                        clusterCreationState=ClusterCreationState.CREATING_INGRESSES,
                        authSecretName=secret_definition["metadata"]["name"],
                    ),
                    children=[
                        secret_definition,
                        scheduler_definition,
                        service_definition,
                        tls_definition,
                        tcp_ingress_definition,
                        http_ingress_definition,
                    ],
                )

            assert scheduler_definition is not None

            # If we have completed all of the above, our cluster is running!
            return NewState(
                status=CRDStatus(
                    clusterState=ClusterState.RUNNING,
                    authSecretName=secret_definition["metadata"]["name"],
                ),
                children=[
                    secret_definition,
                    scheduler_definition,
                    service_definition,
                    tls_definition,
                    tcp_ingress_definition,
                    http_ingress_definition,
                ],
            )

        if state == ClusterState.RUNNING:
            # If our scheduler pod is not running, then the cluster has failed
            if not self.scheduler_pod or pod_status(self.scheduler_pod) != "Running":
                return NewState(
                    status=CRDStatus(clusterState=ClusterState.FAILED), children=[]
                )

            # MyPy thinks (quite rightly) that these definitions could be None.
            assert secret_definition is not None
            assert scheduler_definition is not None
            assert service_definition is not None
            assert tls_definition is not None
            assert tcp_ingress_definition is not None
            assert http_ingress_definition is not None

            # Let's scale!
            desired_workers = self.cluster_definition["spec"]["desiredWorkers"]
            print("Desired workers", desired_workers)

            worker_definitions = [get_minimal_definition(w) for w in self.worker_pods]
            running_or_pending_pod_count = len(
                [
                    pod
                    for pod in self.worker_pods
                    if pod["status"]["phase"] in ("Pending", "Running")
                ]
            )
            new_workers_required = desired_workers - running_or_pending_pod_count
            if new_workers_required > 0:
                # Scale up!
                secret_name = secret_definition["metadata"]["name"]
                worker_definitions.extend(
                    self.create_worker_definitions(secret_name, new_workers_required)
                )

            return NewState(
                status=CRDStatus(
                    clusterState=ClusterState.RUNNING,
                    authSecretName=secret_definition["metadata"]["name"],
                ),
                children=[
                    secret_definition,
                    scheduler_definition,
                    service_definition,
                    tls_definition,
                    tcp_ingress_definition,
                    http_ingress_definition,
                    *worker_definitions,
                ],
            )

        raise RuntimeError(f"Unknown cluster state: {state}")

    def get_single_minimal_definition(self, resource_type: str) -> Optional[dict]:
        if not self.children.get(resource_type):
            return None
        children = self.children[resource_type]
        if len(children) > 1:
            raise RuntimeError(
                f"Resource type {resource_type} has {len(children)} children. Expected 1."
            )
        return get_minimal_definition(list(children.values())[0])

    @cached_property
    def scheduler_pod(self) -> Optional[dict]:
        for pod in self.children.get("Pod.v1", {}).values():
            if pod["metadata"]["labels"]["dask/role"] == "scheduler":
                return pod
        return None

    @cached_property
    def worker_pods(self) -> List[dict]:
        return [
            pod
            for pod in self.children.get("Pod.v1", {}).values()
            if pod["metadata"]["labels"]["dask/role"] == "worker"
        ]

    @cached_property
    def scheduler_pod_name(self) -> Optional[str]:
        if self.scheduler_pod:
            return self.scheduler_pod["metadata"]["name"]
        return None

    def create_scheduler_definition(self, auth_secret_name: str) -> dict:
        scheduler_definition = self.cluster_definition["spec"]["scheduler_definition"]
        return add_credentials_to_pod_definition(
            scheduler_definition, self.cluster_name, auth_secret_name
        )

    def create_worker_definitions(
        self, auth_secret_name: str, count: int
    ) -> Iterable[dict]:
        worker_definition = self.cluster_definition["spec"]["worker_definition"]
        worker_definition_with_secrets = add_credentials_to_pod_definition(
            worker_definition, self.cluster_name, auth_secret_name
        )
        assert self.scheduler_pod is not None
        worker_definition_with_secrets["spec"]["containers"][0]["args"] = [
            "dask-worker",
            f"tcp://{self.scheduler_pod['status']['podIP']}:8786",
        ]
        for _ in range(count):
            new_metadata = worker_definition_with_secrets["metadata"].copy()
            new_metadata["name"] = f"worker-{self.cluster_name}-{uuid.uuid4().hex[:10]}"
            yield {**worker_definition, "metadata": new_metadata}


def pod_status(definition: dict) -> str:
    return definition["status"]["phase"]


def add_credentials_to_pod_definition(
    definition: dict, cluster_name: str, auth_secret_name: str
) -> dict:
    """
    Updates a given definition with some values that should always be present.
    Specifically, it adds:
    1. A dask credentials volume, mounted from the secret created in get_tls_secret_definition()
    2. Various DASK_GATEWAY environment variables
    """
    volumes = definition["spec"].setdefault("volumes", [])
    volumes.append(
        {"name": "dask-credentials", "secret": {"secretName": auth_secret_name}}
    )
    volume_mounts = definition["spec"]["containers"][0].setdefault("volumeMounts", [])
    volume_mounts.append(
        {
            "name": "dask-credentials",
            "mountPath": "/etc/dask-credentials/",
            "readOnly": True,
        }
    )
    definition["spec"]["containers"][0]["env"].extend(
        [
            {
                "name": "DASK_GATEWAY_API_TOKEN",
                "value": "/etc/dask-credentials/api-token",
            },
            {"name": "DASK_GATEWAY_CLUSTER_NAME", "value": cluster_name},
            # To-Do: Should be different when running in the cluster
            {
                "name": "DASK_GATEWAY_API_URL",
                "value": "http://host.docker.internal:8000/api",
            },
        ]
    )
    return definition


def get_dask_service_definition(cluster_name: str, client: ApiClient) -> dict:
    """
    Returns the Kubernetes service that exposes various ports on the Scheduler pod. Specifically a scheduler has:
    1. A "dask" port, which uses the Dask protocol to exchange messages with any remote clients
    2. A dashboard port, which serves the dashboard over HTTP
    3. A gateway port, which serves the scheduler dask-gateway API over HTTP. This is used by the dask gateway to send
       messages to the scheduler in response to scaling requests from the user.
    """
    return client.sanitize_for_serialization(
        V1Service(
            api_version="v1",
            kind="Service",
            metadata=V1ObjectMeta(name=f"scheduler-{cluster_name}"),
            spec=V1ServiceSpec(
                # type="NodePort",
                selector={"dask/role": "scheduler", "dask/cluster": cluster_name},
                ports=[
                    V1ServicePort(port=8786, target_port="dask", name="dask"),
                    V1ServicePort(port=8787, target_port="dashboard", name="dashboard"),
                    V1ServicePort(port=8788, target_port="gateway", name="gateway"),
                ],
            ),
        )
    )


def get_secret_definition(cluster_name: str, client: ApiClient) -> dict:
    """
    Create a definition for the cluster secrets. This includes the api-token used to communicate with
    dask-gateway, and the TLS certificates used for mutual-tls authentication.
    """
    cert_bytes, key_bytes = new_keypair(f"daskgateway-{cluster_name}")
    return client.sanitize_for_serialization(
        V1Secret(
            api_version="v1",
            kind="Secret",
            metadata=V1ObjectMeta(name=f"dask-cluster-{cluster_name}"),
            data={
                "api-token": base64.b64encode(uuid.uuid4().hex.encode()).decode(),
                # The per-cluster certificate authority
                "ca.crt": base64.b64encode(cert_bytes).decode(),
                "tls.crt": base64.b64encode(cert_bytes).decode(),
                # The per-cluster TLS key
                "tls.key": base64.b64encode(key_bytes).decode(),
            },
        )
    )


def get_tls_options(cluster_name: str, tls_secret_name: str) -> dict:
    """
    Get the TLS options definition used by Traefik to validate mutual-tls authentication.
    """
    return {
        "apiVersion": "traefik.containo.us/v1alpha1",
        "kind": "TLSOption",
        "metadata": {"name": f"dask-cluster-{cluster_name}"},
        "spec": {
            "clientAuth": {
                "secretNames": [tls_secret_name],
                "clientAuthType": "RequireAndVerifyClientCert",
            },
            "sniStrict": True,
        },
    }


def get_traefik_tcp_ingress_route(
    cluster_name: str, scheduler_name: str, tls_options_name: str, tls_secrets_name: str
) -> dict:
    """
    Get the TCP ingress route used to route external connections to the scheduler.
    This definition tells Traefik to authenticate the user with mtls, then pass the unencrypted data to the
    Dask scheduler pod.
    """
    return {
        "apiVersion": "traefik.containo.us/v1alpha1",
        "kind": "IngressRouteTCP",
        "metadata": {"name": f"dask-cluster-{cluster_name}"},
        "spec": {
            "routes": [
                {
                    "match": f"HostSNI(`daskgateway-{cluster_name}`)",
                    "services": [{"name": scheduler_name, "port": "dask"}],
                }
            ],
            "tls": {
                "options": {"name": tls_options_name},
                "secretName": tls_secrets_name,
                "domains": [{"main": f"daskgateway-{cluster_name}"}],
                "passthrough": False,
            },
        },
    }


def get_traefik_http_ingress_routes(cluster_name: str, scheduler_name: str) -> dict:
    """
    Get the HTTP ingress routes used to route requests for dashboards to the scheduler dashboard port.

    When running locally we also need to expose the schedulers `gateway-api` port to our local machine so that
    we can send requests to the scheduler when we need to tell it to scale up or down.
    """
    return {
        "apiVersion": "traefik.containo.us/v1alpha1",
        "kind": "IngressRoute",
        "metadata": {"name": f"dask-cluster-{cluster_name}"},
        "spec": {
            "routes": [
                # To-Do: This ingress rule should only be added when we are running locally.
                {
                    "kind": "Rule",
                    "match": f"PathPrefix(`/gateway-api/{cluster_name}/`)",
                    "middlewares": [{"name": "common-dask-middleware"}],
                    "services": [{"name": scheduler_name, "port": "gateway"}],
                },
                {
                    "kind": "Rule",
                    "match": f"PathPrefix(`/dashboard/{cluster_name}/`)",
                    "middlewares": [{"name": "common-dask-middleware"}],
                    "services": [{"name": scheduler_name, "port": "dashboard"}],
                },
            ]
        },
    }


def get_minimal_definition(resource: dict) -> dict:
    """
    Return an "empty" definition for a given resource type.
    """
    return {
        "apiVersion": resource["apiVersion"],
        "kind": resource["kind"],
        "metadata": resource["metadata"],
    }
