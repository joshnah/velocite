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
@app.get("/all")
def read_cluster():
    rows = session.execute(
        "SELECT * FROM station.stations").all()
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=rows
    )
