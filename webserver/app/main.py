from functools import lru_cache

from fastapi import FastAPI
from cassandra.cluster import Cluster
from starlette import status
from starlette.responses import JSONResponse
from . import config


# Get settings from .env file
@lru_cache()
def get_settings():
    return config.Settings()


# Connect to Cassandra
cluster = Cluster(
    contact_points=[get_settings().cassandra_host],
    port=get_settings().cassandra_port
)
session = cluster.connect()

# FastAPI app
app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


# Get Cassandra cluster name and listen address
@app.get("/cluster")
def read_cluster():
    row = session.execute(
        "SELECT cluster_name, listen_address FROM system.local").one()
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"cluster_name": row.cluster_name,
                 "listen_address": row.listen_address},
    )
