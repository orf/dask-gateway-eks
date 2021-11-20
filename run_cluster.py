import time

from dask.distributed import Client
from dask_gateway import GatewayCluster


def do_stuff(idx):
    print(f"Starting task {idx}")
    import random

    time.sleep(1)
    print("Done Task")
    return random.random()


with GatewayCluster(
    "http://localhost:8000", proxy_address="https://localhost:443", image="dask-eks"
) as gateway:
    gateway.adapt(0, 3)
    print(gateway.dashboard_link)
    client: Client = gateway.get_client()

    futures = client.map(do_stuff, range(100))
    print("Gathering...")
    print(client.gather(futures))

    input("Waiting...")
    futures = client.map(do_stuff, range(200))
    print("Gathering...")
    print(client.gather(futures))
