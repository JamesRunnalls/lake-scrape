import requests
import pandas as pd
from datetime import datetime, timezone


def temperature(stations, filesystem, min_date):
    """
    Selected temperature datasets from Datalakes
    https://www.datalakes-eawag.ch/
    """
    features = []
    for station in stations:
        response = requests.get(
            "https://api.datalakes-eawag.ch/data/{}/{}".format(station["id"], station["parameters"]))
        if response.status_code == 200:
            data = response.json()
            response = requests.get("https://api.datalakes-eawag.ch/datasets/{}".format(station["id"]))
            if response.status_code == 200:
                metadata = response.json()
                date = datetime.strptime(data["time"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                    tzinfo=timezone.utc).timestamp()
                if date > min_date:
                    features.append({
                        "type": "Feature",
                        "id": "datalakes_{}".format(station["id"]),
                        "properties": {
                            "label": station["label"],
                            "last_time": date,
                            "last_value": data["value"],
                            "depth": station["depth"],
                            "url": "https://www.datalakes-eawag.ch/datadetail/{}".format(station["id"]),
                            "source": "Datalakes",
                            "icon": "lake",
                            "lake": station["lake"]
                        },
                        "geometry": {
                            "coordinates": [metadata["longitude"], metadata["latitude"]],
                            "type": "Point"}})
    return features