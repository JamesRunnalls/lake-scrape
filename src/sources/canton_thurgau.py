import os
import requests
import pandas as pd
from datetime import datetime
from sources.functions import write_local_data


def temperature(stations, filesystem, min_date):
    """
    Water temperature data from Canton Thurgau
    http://www.hydrodaten.tg.ch
    """
    folder = os.path.join(filesystem, "media/lake-scrape/temperature")
    response = requests.get("http://www.hydrodaten.tg.ch/data/internet/layers/30/index.json")
    features = []
    if response.status_code == 200:
        for f in response.json():
            if f["metadata_station_no"] in stations:

                date = datetime.strptime(f["L1_timestamp"], "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()
                if f["metadata_river_name"] == "":
                    icon = "lake"
                else:
                    icon = "river"

                df = pd.DataFrame({'time': [date], "value": [float(f["L1_ts_value"])]})
                key = "thurgau_" + f["metadata_station_no"]
                write_local_data(os.path.join(folder, key), df)

                if date > min_date:
                    features.append({
                        "type": "Feature",
                        "id": key,
                        "properties": {
                            "label": f["metadata_station_name"],
                            "last_time": date,
                            "last_value": float(f["L1_ts_value"]),
                            "depth": "surface",
                            "url": "http://www.hydrodaten.tg.ch/app/index.html#{}".format(f["metadata_station_no"]),
                            "source": "Kanton Thurgau",
                            "icon": icon,
                            "lake": "constance"
                        },
                        "geometry": {
                            "coordinates": [float(f["metadata_station_longitude"]),
                                            float(f["metadata_station_latitude"])],
                            "type": "Point"}})
    return features
