from datetime import datetime, timedelta
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
    page = max(int(page),1)
    query = "SELECT station_id FROM station.stations WHERE city = ?;"
    prepared_query = session.prepare(query)
    statement = prepared_query.bind([city])
    results = session.execute(statement)
    results = list(results)
    # convert to int
    rows = [int(r.station_id) for r in results]
    # remove double
    rows = list(dict.fromkeys(rows))
    # sort it 
    rows.sort()
    rows = rows[(page-1)*max_station_page:page*max_station_page]
    more_stations = len(rows) == max_station_page
    next_page_url = None

    if more_stations:
        next_page_params = {"page": page + 1}
        next_page_url = str(request.base_url).rstrip('/') + '/' + city + '/' + "?" + "&".join(f"{k}={v}" for k, v in next_page_params.items())

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "stations": [{r: str(request.base_url) + city + '/' + str(r)} for r in rows],
            "next_page": next_page_url,
        }
    )



@app.get("/{city}/{station_id}")
def read_station(city, station_id, request: Request, startDate = None, endDate = None):
    if startDate is None or endDate is None:
        endDate = datetime.now()
        startDate = endDate - timedelta(days=7)
    query = "SELECT * FROM station.stations WHERE city = ? AND station_id = ? AND updated_at >= ? AND updated_at <= ?;"
    prepared_query = session.prepare(query)
    statement = prepared_query.bind([city, station_id, startDate, endDate])

    results = session.execute(statement)
    results = list(results)
    # convert unix timestamp to datetime
    updates = {}
    for r in results:
        date = r.updated_at.strftime("%Y-%m-%d %H:%M:%S")
        updates[date] = {"capacity": r.capacity, "bikes": r.bikes}

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "updates": updates
        }
    )    
# range parameter ?start=2020-01-01&end=2020-01-02 et default 7 derniers jours