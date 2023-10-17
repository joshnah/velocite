import json
from kafka import KafkaProducer
import requests
from datetime import datetime

SERVER_ADDRESS = '127.0.0.1:9092'



producer = KafkaProducer(
    bootstrap_servers=SERVER_ADDRESS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
list_apis = {"paris":"https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records",
             "lille":"https://opendata.lillemetropole.fr/api/explore/v2.1/catalog/datasets/vlille-realtime/records",
             "lyon":"https://transport.data.gouv.fr/gbfs/lyon/station_information.json",
             "strasbourg":"https://data.strasbourg.eu/api/explore/v2.1/catalog/datasets/stations-velhop/records",
             "toulouse":"https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/records",
             "bordeaux":"https://transport.data.gouv.fr/gbfs/vcub/station_information.json",
             "nancy":"https://transport.data.gouv.fr/gbfs/nancy/station_information.json",
             "amiens":"https://transport.data.gouv.fr/gbfs/amiens/station_information.json",
             "besancon":"https://transport.data.gouv.fr/gbfs/besancon/station_information.json"}

def extract_from_opendata(city):
    results = []
    offset = 0
    while True:
        url = list_apis[city] + "?limit=100&offset=" + str(offset)
        try:
            response = requests.get(url).json()
        except Exception as e:
            print(e)
            print("Error while calling API from opendata")
            exit(1)
        if(len(response["results"])>0):
            results = results + response["results"]
            offset += 100
        else:
            break
    match city:
        case "lille":
            for station in results:
                station["station_id"] = station["libelle"]
                station["name"] = station["nom"]
                station["capacity"] = station["nbplacesdispo"]
                station["lat"] = station["geo"]["lat"]
                station["lon"] = station["geo"]["lon"]
                station["num_bikes_available"] = station["nbvelosdispo"]
                station["last_reported"] = datetime.strptime(station["datemiseajour"][:-6], "%Y-%m-%dT%H:%M:%S").timestamp()
        case "strasbourg":
            for station in results:
                station["name"] = station["na"]
                station["capacity"] = station["to"]
                station["lat"] = station["la"]
                station["lon"] = station["lg"]
                station["num_bikes_available"] = station["av"]
                station["station_id"] = station["id"]
        case "toulouse":
            for station in results:
                station["station_id"] = station["number"]
                station["capacity"] = station["bike_stands"]
                station["lat"] = station["position"]["lat"]
                station["lon"] = station["position"]["lon"]
                station["num_bikes_available"] = station["available_bikes"]
                station["last_reported"] = datetime.strptime(station["last_update"][:-6], "%Y-%m-%dT%H:%M:%S").timestamp()
        case default:
            for station in results:
                station["lat"] = station["coordonnees_geo"]["lat"]
                station["lon"] = station["coordonnees_geo"]["lon"]
                station["num_bikes_available"] = station["numbikesavailable"]
                station["station_id"] = station["stationcode"]
                station["last_reported"] = datetime.strptime(station["duedate"][:-6], "%Y-%m-%dT%H:%M:%S").timestamp()
    return results

def extract_from_gouv(city):
    try:
        stations_informations = requests.get(list_apis[city]).json()
    except Exception as e:
        print(e)
        print("Error while calling API informations from gouv")
        exit(1)
    stations_informations = stations_informations["data"]["stations"]
    try:
        stations_status = requests.get(list_apis[city].replace("station_information","station_status")).json()
    except Exception as e:
        print(e)
        print("Error while calling API status from gouv")
        exit(1)
    stations_status = stations_status["data"]["stations"]
    stations_dict = {}
    for station in stations_informations:
        stations_dict[station["station_id"]] = station
    for station in stations_status:
        informations = stations_dict[station["station_id"]]
        station["name"] = informations["name"]
        station["lat"] = informations["lat"]
        station["lon"] = informations["lon"]
        station["capactiy"] = informations["capacity"]
    return stations_status

if __name__ == "__main__":
    """
       1. Calling API 
       2. Send the fake data to the consumer topic 
       3. Sleep 10 seconds and repeat the process utill press Crontrol C
    """

    try:
        for city,url in list_apis.items():
            if(url.find(".json") == -1):
                print("extract from " + city + " opendata")
                result = extract_from_opendata(city)
            else:
                print("extract from " + city + " gouv")
                result = extract_from_gouv(city)
            producer.send(city, result)
    except KeyboardInterrupt:
       print("Quit")