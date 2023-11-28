from functools import lru_cache

from fastapi import FastAPI, Request
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
def read_root(request : Request):
    rows = session.execute(
        "SELECT DISTINCT city FROM station.stations")
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"cities": [{r.city: str(request.base_url) + r.city} for r in rows]})


# Get list of stations in a city
@app.get("/{city}/")
def read_city(city,  request : Request, page = 1):
    max_station_page = 100
    print(request.base_url)
    page = int(page)
    if page < 1:
        page = 1
    rows = session.execute(
        "SELECT station_id FROM station.stations WHERE city = '%s' LIMIT %s;" % (city, max_station_page)) # , (page - 1) * max_station_page
    rows = list(rows)
    more_station = len(rows) >= max_station_page
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"stations": [{r.station_id: str(request.base_url)+ '/' + city + '/' + r.station_id} for r in rows], "next_page": str(request.base_url) + '/' + city + '/' + "?page=" + str(page + 1) if more_station else None})
