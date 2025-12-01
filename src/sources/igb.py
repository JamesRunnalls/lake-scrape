import os
import time
import pytz
import random
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from sources.functions import write_local_data, parse_html, html_find_all


def temperature(stations, filesystem, min_date):
    """
    Water temperature data from IGB Berlin
    https://emon.igb-berlin.de/arendsee.html
    """
    features = []
    folder = os.path.join(filesystem, "media/lake-scrape/temperature")
    german_timezone = pytz.timezone("Europe/Berlin")
    for station in stations:
        try:
            response = requests.get("https://emon.igb-berlin.de/{}.html".format(station["name"]), verify=False)
            if response.status_code == 200:
                root = parse_html(response.text)
                element = html_find_all(root, tag="div", class_name="datenaktuell_rechts")
                date = datetime.strptime(html_find_all(element[0], tag="td", class_name="td1s")[0].text.split(": ")[-1],
                                         "%d.%m.%Y  %H:%M")
                date = german_timezone.localize(date).timestamp()
                water_temp_index = next(
                    (i for i, row in enumerate(html_find_all(element[0], tag="td", class_name="td1"))
                     if row[0].text == "Wassertemperatur:"), None)
                if water_temp_index is None:
                    raise ValueError("Unable to find water temperature")
                value = html_find_all(element[0], tag="td", class_name="td2")[water_temp_index].text
                key = "igb_{}".format(station["name"])
                df = pd.DataFrame({'time': [date], "value": [value]})
                write_local_data(os.path.join(folder, key), df)
                if date > min_date:
                    features.append({
                        "type": "Feature",
                        "id": key,
                        "properties": {
                            "label": station["label"],
                            "last_time": date,
                            "last_value": value,
                            "depth": "surface",
                            "url": "https://emon.igb-berlin.de/{}.html".format(station["name"]),
                            "source": "IGB Berlin",
                            "icon": "lake",
                            "lake": station["lake"]
                        },
                        "geometry": {
                            "coordinates": station["coords"],
                            "type": "Point"}})
        except Exception as e:
            print("FAILED: IGB: " + station)
            print(e)
    return features
