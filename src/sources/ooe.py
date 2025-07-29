import os
import pytz
import requests
import pandas as pd
from datetime import datetime
from urllib.parse import quote
from sources.functions import parse_html_table, write_local_data, cart_to_latlng


def temperature(stations, filesystem, min_date):
    """
    Water temperature data from Province of Upper Austria
    https://hydro.ooe.gv.at/#/overview/Wassertemperatur
    """
    features = []
    folder = os.path.join(filesystem, "media/lake-scrape/temperature")
    response = requests.get("https://hydro.ooe.gv.at/daten/internet/layers/5/index.json")
    if response.status_code != 200:
        raise ValueError("Failed to collect data")

    data = response.json()
    for station in data:
        if station["station_id"] in stations.keys():
            lat, lon = cart_to_latlng(station["station_carteasting"], station["station_cartnorthing"])
            response = requests.get(f"https://hydro.ooe.gv.at/daten/internet/stations/OG/{station['station_no']}/WT/week.json")
            key = f"ooe_{station['station_id']}"
            if response.status_code == 200:
                r = response.json()[0]
                df = pd.DataFrame(r["data"], columns=r["columns"].split(","))
                df["time"] = pd.to_datetime(df['Timestamp']).astype('int64') // 10**9
                df["value"] = df["Value"]
                df = df[["time", "value"]]
                df['value'] = pd.to_numeric(df['value'], errors='coerce')
                df = df.sort_values("time")
                write_local_data(os.path.join(folder, key), df)
            date = datetime.fromisoformat(station["timestamp"]).timestamp()
            value = float(station["ts_value"])
            if date > min_date:
                features.append({
                    "type": "Feature",
                    "id": key,
                    "properties": {
                        "label": station["station_name"],
                        "last_time": date,
                        "last_value": value,
                        "depth": "surface",
                        "url": f"https://hydro.ooe.gv.at/#/overview/Wasserstand/station/{station['station_id']}/{quote(station['station_name'])}/Wassertemperatur",
                        "source": "Land Oberösterreich",
                        "icon": stations[station["station_id"]]["icon"],
                        "lake": stations[station["station_id"]]["lake"]
                    },
                    "geometry": {
                        "coordinates": [lon, lat],
                        "type": "Point"}})

    return features

def level(stations, filesystem, min_date):
    """
    Water level data from Province of Upper Austria
    https://hydro.ooe.gv.at/#/overview/Wasserstand
    """
    features = []
    response = requests.get("https://hydro.ooe.gv.at/daten/internet/layers/1/index.json")
    if response.status_code != 200:
        raise ValueError("Failed to collect data")

    data = response.json()
    for station in data:
        if station["station_id"] in stations.keys():
            lat, lon = cart_to_latlng(station["station_carteasting"], station["station_cartnorthing"])
            key = f"ooe_{station['station_id']}"
            date = datetime.fromisoformat(station["timestamp"]).timestamp()
            value = stations[station["station_id"]]["zero"] + float(station["ts_value"]) / 100
            if date > min_date:
                features.append({
                    "type": "Feature",
                    "id": key,
                    "properties": {
                        "label": station["station_name"],
                        "last_time": date,
                        "last_value": value,
                        "url": f"https://hydro.ooe.gv.at/#/overview/Wasserstand/station/{station['station_id']}/{quote(station['station_name'])}/Wasserstand",
                        "source": "Land Oberösterreich",
                        "icon": "lake",
                        "lake": stations[station["station_id"]]["lake"]
                    },
                    "geometry": {
                        "coordinates": [lon, lat],
                        "type": "Point"}})

    return features