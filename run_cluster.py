from dask_gateway import GatewayCluster

with GatewayCluster("http://localhost:8000", proxy_address="https://localhost:443",
                    image="daskgateway/dask-gateway:0.9.0") as gateway:
    try:
        client = gateway.get_client()
    except Exception as e:
        print(e)
        input("Waiting...")
