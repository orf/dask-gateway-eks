import httpx
from fastapi import FastAPI
from starlette.requests import Request

from dask_gateway_eks.models import Cluster


def setup_http_client(app: FastAPI):
    @app.on_event("startup")
    async def setup_client():
        httpx_client = httpx.AsyncClient()
        app.extra["httpx_client"] = await httpx_client.__aenter__()

    @app.on_event("shutdown")
    async def shutdown_client():
        await app.extra["httpx_client"].__aexit__(None, None, None)


def client(request: Request) -> httpx.AsyncClient:
    return request.app.extra["httpx_client"]


async def send_to_scheduler(
    cluster: Cluster, httpx_client: httpx.AsyncClient, message: dict
):
    response = await httpx_client.post(
        f"http://localhost/gateway-api/{cluster.name}/api/comm",
        json=message,
        headers={"Authorization": "token %s" % cluster.api_token},
    )
    response.raise_for_status()
