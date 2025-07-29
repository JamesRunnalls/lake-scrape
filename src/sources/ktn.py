import os
import pytz
import requests
import pandas as pd
from datetime import datetime
from sources.functions import parse_html_table, write_local_data


def temperature(stations, filesystem, min_date):
    """
    Water temperature data from Hydrographischer Dienst Land Kärnten
    https://hydrographie.ktn.gv.at/gewasser/seen-wasserstaende
    """
    features = []
    folder = os.path.join(filesystem, "media/lake-scrape/temperature")
    response = requests.get("https://hydrographie.ktn.gv.at/DE/repos/evoscripts/hydrografischer/getSeenWasserstand.es")
    if response.status_code == 200:
        for s in response.json()["data"]:
            s_id = str(s["stationsnummer"])
            if s_id in stations:
                value = float(s["metrics2"].replace(",", "."))
                tz = pytz.timezone("Europe/Rome")
                date = datetime.strptime(s["datum"], "%d.%m.%Y %H:%M")
                date = int(tz.localize(date).timestamp())
                df = pd.DataFrame({'time': [date], "value": [value]})
                key = "ktn_" + s_id
                write_local_data(os.path.join(folder, key), df)
                if date > min_date:
                    features.append({
                        "type": "Feature",
                        "id": key,
                        "properties": {
                            "label": s["station"],
                            "last_time": date,
                            "last_value": value,
                            "depth": "surface",
                            "url": "https://hydrographie.ktn.gv.at/gewasser/seen-wasserstaende",
                            "source": "Hydrographischer Dienst Land Kärnten",
                            "icon": "lake",
                            "lake": False
                        },
                        "geometry": {
                            "coordinates": stations[s_id]["coordinates"],
                            "type": "Point"}})
    return features

def level(stations, filesystem, min_date):
    """
    Water level data from Hydrographischer Dienst Land Kärnten
    https://hydrographie.ktn.gv.at/gewasser/seen-wasserstaende
    """
    features = []
    response = requests.get("https://hydrographie.ktn.gv.at/DE/repos/evoscripts/hydrografischer/getSeenWasserstand.es")
    if response.status_code == 200:
        for s in response.json()["data"]:
            s_id = str(s["stationsnummer"])
            if s_id in stations:
                value = float(s["pegelnullpunkt"]) + (float(s["metrics"])/100)
                tz = pytz.timezone("Europe/Rome")
                date = datetime.strptime(s["datum"], "%d.%m.%Y %H:%M")
                date = int(tz.localize(date).timestamp())
                key = "ktn_" + s_id
                if date > min_date:
                    features.append({
                        "type": "Feature",
                        "id": key,
                        "properties": {
                            "label": s["station"],
                            "last_time": date,
                            "last_value": value,
                            "url": "https://hydrographie.ktn.gv.at/gewasser/seen-wasserstaende",
                            "source": "Hydrographischer Dienst Land Kärnten",
                            "icon": "lake",
                            "lake": False
                        },
                        "geometry": {
                            "coordinates": stations[s_id]["coordinates"],
                            "type": "Point"}})
    return features