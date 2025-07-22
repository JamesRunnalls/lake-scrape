import os
import pytz
import requests
import pandas as pd
from datetime import datetime
from sources.functions import parse_html_table, write_local_data


def temperature(stations, filesystem, min_date):
    """
    Water temperature data from ARSO
    https://www.arso.gov.si/vode/podatki/amp/
    """
    features = []
    folder = os.path.join(filesystem, "media/lake-scrape/temperature")
    for station in stations:
        response = requests.get(f"https://www.arso.gov.si/vode/podatki/amp/{station['id']}.html")
        if response.status_code == 200:
            df = parse_html_table(response.text).iloc[2:, :3]
            df.columns = ["time", "level", "value"]
            df = df[["time", "value"]]
            df['time'] = pd.to_datetime(df['time'], format='%d.%m.%Y %H:%M').dt.tz_localize('Europe/Ljubljana').astype(int) // 10 ** 9
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df.sort_values("time")
            key = "arso_{}".format(station["id"])
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
                        "depth": station["depth"],
                        "url": f"https://www.arso.gov.si/vode/podatki/amp/{station['id']}.html",
                        "source": "ARSO",
                        "icon": station["icon"],
                        "lake": station["lake"]
                    },
                    "geometry": {
                        "coordinates": station["coordinates"],
                        "type": "Point"}})
    return features
