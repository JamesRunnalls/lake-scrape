import requests
import pandas as pd
from datetime import datetime
from sources.functions import ch1903_plus_to_latlng


def temperature(stations, filesystem, min_date):
    """
    River monitoring data from BAFU
    https://www.hydrodaten.admin.ch
    """
    features = []
    response = requests.get("https://www.hydrodaten.admin.ch/web-hydro-maps/hydro_sensor_temperature.geojson")
    if response.status_code == 200:
        for f in response.json()["features"]:
            lat, lng = ch1903_plus_to_latlng(f["geometry"]["coordinates"][0], f["geometry"]["coordinates"][1])
            date = datetime.strptime(f["properties"]["last_measured_at"], "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()
            lake = False
            if str(f["properties"]["key"]) in stations:
                lake = stations[str(f["properties"]["key"])]
            if date > min_date:
                features.append({
                    "type": "Feature",
                    "id": "bafu_" + f["properties"]["key"],
                    "properties": {
                        "label": f["properties"]["label"],
                        "last_time": date,
                        "last_value": float(f["properties"]["last_value"]),
                        "depth": False,
                        "url": "https://www.hydrodaten.admin.ch/en/seen-und-fluesse/stations/{}".format(
                            f["properties"]["key"]),
                        "source": "BAFU Hydrodaten",
                        "icon": "river",
                        "lake": lake
                    },
                    "geometry": {
                        "coordinates": [lng, lat],
                        "type": "Point"}})
    return features

def level(stations, filesystem, min_date):
    """
    River monitoring data from BAFU
    https://www.hydrodaten.admin.ch
    """
    features = []
    response = requests.get("https://www.hydrodaten.admin.ch/web-hydro-maps/hydro_sensor_pq.geojson")
    if response.status_code == 200:
        for f in response.json()["features"]:
            lat, lng = ch1903_plus_to_latlng(f["geometry"]["coordinates"][0], f["geometry"]["coordinates"][1])
            date = datetime.strptime(f["properties"]["last_measured_at"], "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()
            if f["properties"]["kind"] == "lake" and str(f["properties"]["key"]) in stations:
                if date > min_date:
                    features.append({
                        "type": "Feature",
                        "id": "bafu_" + f["properties"]["key"],
                        "properties": {
                            "label": f["properties"]["label"],
                            "unit": f["properties"]["unit"],
                            "last_time": date,
                            "last_value": float(f["properties"]["last_value"]),
                            "url": "https://www.hydrodaten.admin.ch/en/seen-und-fluesse/stations/{}".format(
                                f["properties"]["key"]),
                            "source": "BAFU Hydrodaten",
                            "icon": "lake",
                            "lake": stations[str(f["properties"]["key"])]
                        },
                        "geometry": {
                            "coordinates": [lng, lat],
                            "type": "Point"}})
    return features