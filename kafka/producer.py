import json
from kafka import KafkaProducer
import requests

SERVER_ADDRESS = '127.0.0.1:9092'



producer = KafkaProducer(
    bootstrap_servers=SERVER_ADDRESS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
list_apis = {"paris":"https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records",
             "lille":"https://opendata.lillemetropole.fr/api/explore/v2.1/catalog/datasets/vlille-realtime/records",
             "lyon":"https://transport.data.gouv.fr/gbfs/lyon/station_status.json",
             "strasbourg":"https://data.strasbourg.eu/api/explore/v2.1/catalog/datasets/stations-velhop/records",
             "toulouse":"https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/records",
             "bordeaux":"https://transport.data.gouv.fr/gbfs/vcub/station_status.json",
             "nancy":"https://transport.data.gouv.fr/gbfs/nancy/station_status.json",
             "amiens":"https://transport.data.gouv.fr/gbfs/amiens/station_status.json",
             "besancon":"https://transport.data.gouv.fr/gbfs/besancon/station_status.json"}

def extract_from_opendata(city):
    #TODO get every pages
    results = []
    results = requests.get(list_apis[city])
    return results

def extract_from_gouv(city):
    #TODO get every pages
    results = []
    results = requests.get(list_apis[city])
    return results

if __name__ == "__main__":
    """
       1. Calling API 
       2. Send the fake data to the consumer topic 
       3. Sleep 10 seconds and repeat the process utill press Crontrol C
    """

    try:
        for city,url in list_apis.items():
            if(url.find("opendata") != -1):
                result = extract_from_opendata(city)
            else:
                result = extract_from_gouv(city)
            producer.send(city, result)
    except KeyboardInterrupt:
       print("Quit")