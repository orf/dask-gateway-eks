import aiohttp
import backoff
from aiohttp import ClientError
from aiohttp.http_exceptions import HttpProcessingError
from aiohttp.web_exceptions import HTTPException
from fastapi import FastAPI
from starlette.requests import Request

from dask_gateway_eks.models import Cluster


def setup_http_client(app: FastAPI):
    @app.on_event("startup")
    async def setup_client():
        http_client = aiohttp.ClientSession()
        app.extra["http_client"] = await http_client.__aenter__()

    @app.on_event("shutdown")
    async def shutdown_client():
        await app.extra["http_client"].__aexit__(None, None, None)


def client(request: Request) -> aiohttp.ClientSession:
    return request.app.extra["http_client"]


@backoff.on_exception(
    backoff.expo, (ClientError, HttpProcessingError, HTTPException), max_tries=5
)
async def send_to_scheduler(
    cluster: Cluster, http_client: aiohttp.ClientSession, message: dict
):
    response = await http_client.post(
        f"http://localhost/gateway-api/{cluster.name}/api/comm",
        json=message,
        headers={"Authorization": "token %s" % cluster.api_token},
    )
    response.raise_for_status()
