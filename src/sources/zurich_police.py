import os
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from sources.functions import write_local_data


def temperature(stations, filesystem, min_date):
    """
    Water temperature data from Zurich Police
    https://tecdottir.herokuapp.com
    """
    features = []
    folder = os.path.join(filesystem, "media/lake-scrape/temperature")
    for station in stations:
        today = datetime.today()
        startDate = (today - timedelta(days=2)).strftime('%Y-%m-%d')
        endDate = (today + timedelta(days=1)).strftime('%Y-%m-%d')
        response = requests.get(
            "https://tecdottir.herokuapp.com/measurements/{}?startDate={}&endDate={}&sort=timestamp_cet%20desc&limit=1000&offset=0".format(
                station["id"], startDate, endDate))
        if response.status_code == 200:
            data = response.json()
            if data["ok"] != True:
                continue
            time = []
            values = []
            for d in data["result"]:
                time.append(d["timestamp"])
                values.append(float(d["values"]["water_temperature"]["value"]))

            df = pd.DataFrame({'time': time, "value": values})
            df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True).astype(
                int) / 10 ** 9
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df.sort_values("time")
            key = "zurich_police_{}".format(station["id"])
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
                        "depth": 1,
                        "url": "https://www.tecson-data.ch/zurich/{}/index.php".format(station["id"]),
                        "source": "Stadtpolizei Zürich",
                        "icon": "lake",
                        "lake": False
                    },
                    "geometry": {
                        "coordinates": station["coordinates"],
                        "type": "Point"}})
    return features
