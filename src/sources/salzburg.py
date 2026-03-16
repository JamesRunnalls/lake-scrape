import os
import pytz
import requests
import pandas as pd
from datetime import datetime
from sources.functions import write_local_data


def temperature(stations, filesystem, min_date):
    """
    Water temperature data from Land Salzburg
    https://www.salzburg.gv.at/wasser/hydro/#/Seen
    """
    features = []
    folder = os.path.join(filesystem, "media/lake-scrape/temperature")
    response = requests.get(f"https://www.salzburg.gv.at/wasser/hydro/grafiken/data.json")
    if response.status_code == 200:
        data = response.json()
        for station in stations:
            try:
                key = "salzburg_{}".format(station["id"])
                station_data = next((item for item in data if str(item["number"]) == str(station["id"])), None)
                time = round(station_data["values"]["WT"]["Cmd"]["dt"] / 1000)
                value = station_data["values"]["WT"]["Cmd"]["v"]
                df = pd.DataFrame({"time": [time], "value": [value]})
                write_local_data(os.path.join(folder, key), df)
                if time > min_date:
                    features.append({
                        "type": "Feature",
                        "id": key,
                        "properties": {
                            "label": station_data["name"].split(" (")[0],
                            "last_time": time,
                            "last_value": value,
                            "depth": "surface",
                            "url": f"https://www.salzburg.gv.at/wasser/hydro/#/Seen/list?station={station['id']}",
                            "source": "Land Salzburg",
                            "icon": "lake",
                            "lake": station["lake"]
                        },
                        "geometry": {
                            "coordinates": [station_data["latlng"][1], station_data["latlng"][0]],
                            "type": "Point"}})
            except Exception as e:
                print(e)
    return features
