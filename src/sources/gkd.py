import os
import pytz
import requests
import pandas as pd
from datetime import datetime
from sources.functions import parse_html_table, write_local_data


def temperature(stations, filesystem, min_date):
    """
    Water temperature data from Gewässerkundlicher Dienst Bayern
    https://www.gkd.bayern.de/en/lakes/watertemperature
    """
    features = []
    folder = os.path.join(filesystem, "media/lake-scrape/temperature")
    for station in stations:
        response = requests.get(f"https://www.gkd.bayern.de/en/lakes/watertemperature/{station['area']}/{station['id']}/current-values/table")
        if response.status_code == 200:
            df = parse_html_table(response.text)
            df.columns = ["time", "value"]
            df['time'] = pd.to_datetime(df['time'], format='%d.%m.%Y %H:%M').dt.tz_localize('Europe/Berlin').astype(int) // 10 ** 9
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df.sort_values("time")
            key = "gkd_{}".format(station["id"])
            write_local_data(os.path.join(folder, key), df)
            df = df.dropna(subset=['value'])
            row = df.iloc[-1]
            date = row["time"]
            if date > min_date:
                features.append({
                    "type": "Feature",
                    "id": key,
                    "properties": {
                        "label": station["label"],
                        "last_time": date,
                        "last_value": row["value"],
                        "depth": "surface",
                        "url": f"https://www.gkd.bayern.de/en/lakes/watertemperature/{station['area']}/{station['id']}/current-values",
                        "source": "Gewässerkundlicher Dienst Bayern",
                        "icon": station["icon"],
                        "lake": station["lake"]
                    },
                    "geometry": {
                        "coordinates": station["coordinates"],
                        "type": "Point"}})
    return features

def level(stations, filesystem, min_date):
    """
    Water level data from Gewässerkundlicher Dienst Bayern
    https://www.gkd.bayern.de/en/lakes/waterlevel
    """
    features = []
    for station in stations:
        response = requests.get(f"https://www.gkd.bayern.de/en/lakes/waterlevel/{station['area']}/{station['id']}/current-values/table")
        if response.status_code == 200:
            df = parse_html_table(response.text)
            df.columns = ["time", "value"]
            df['time'] = pd.to_datetime(df['time'], format='%d.%m.%Y %H:%M').dt.tz_localize('Europe/Berlin').astype(int) // 10 ** 9
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df.sort_values("time")
            key = "gkd_{}".format(station["id"])
            df = df.dropna(subset=['value'])
            row = df.iloc[-1]
            date = row["time"]
            if date > min_date:
                features.append({
                    "type": "Feature",
                    "id": key,
                    "properties": {
                        "label": station["label"],
                        "last_time": date,
                        "last_value": row["value"],
                        "url": f"https://www.gkd.bayern.de/en/lakes/watertemperature/{station['area']}/{station['id']}/current-values",
                        "source": "Gewässerkundlicher Dienst Bayern",
                        "icon": station["icon"],
                        "lake": station["lake"]
                    },
                    "geometry": {
                        "coordinates": station["coordinates"],
                        "type": "Point"}})
    return features