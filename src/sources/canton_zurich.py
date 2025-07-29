import os
import pytz
import requests
import pandas as pd
from datetime import datetime
from sources.functions import parse_html_table, write_local_data


def temperature(stations, filesystem, min_date):
    """
    Water temperature data from Canton Zurich
    https://www.zh.ch/de/umwelt-tiere/wasser-gewaesser/messdaten/wassertemperaturen.html
    """
    features = []
    folder = os.path.join(filesystem, "media/lake-scrape/temperature")
    response = requests.get("https://hydroproweb.zh.ch/Listen/AktuelleWerte/AktWassertemp.html")
    swiss_timezone = pytz.timezone("Europe/Zurich")
    if response.status_code == 200:
        df = parse_html_table(response.text.encode('latin-1').decode('utf-8'))
        for index, row in df.iterrows():
            label = row.iloc[0]
            if label in stations:
                date = datetime.strptime(str(row.iloc[3] + row.iloc[2]), "%d.%m.%Y%H:%M")
                date = swiss_timezone.localize(date).timestamp()
                df = pd.DataFrame({'time': [date], "value": [float(row.iloc[4])]})
                key = "canton_zurich_" + stations[label]["id"]
                write_local_data(os.path.join(folder, key), df)
                if date > min_date:
                    features.append({
                        "type": "Feature",
                        "id": key,
                        "properties": {
                            "label": label,
                            "last_time": date,
                            "last_value": float(row.iloc[4]),
                            "depth": "surface" if stations[label]["icon"] == "lake" else False,
                            "url": "https://www.zh.ch/de/umwelt-tiere/wasser-gewaesser/messdaten/wassertemperaturen.html",
                            "source": "Kanton Zurich",
                            "icon": stations[label]["icon"],
                            "lake": stations[label]["lake"]
                        },
                        "geometry": {
                            "coordinates": stations[label]["coordinates"],
                            "type": "Point"}})
    return features

def level(stations, filesystem, min_date):
    """
    Water level data from Canton Zurich
    https://www.zh.ch/de/umwelt-tiere/wasser-gewaesser/messdaten/abfluss-wasserstand.html
    """
    features = []
    response = requests.get("https://hydroproweb.zh.ch/Listen/AktuelleWerte/aktuelle_werte.html")
    swiss_timezone = pytz.timezone("Europe/Zurich")
    if response.status_code == 200:
        df = parse_html_table(response.text.encode('latin-1').decode('utf-8'))
        for index, row in df.iterrows():
            label = row.iloc[0]
            if label in stations:
                date = datetime.strptime(str(row.iloc[3] + row.iloc[2]), "%d.%m.%Y%H:%M")
                date = swiss_timezone.localize(date).timestamp()
                key = "canton_zurich_" + stations[label]["id"]
                if date > min_date:
                    features.append({
                        "type": "Feature",
                        "id": key,
                        "properties": {
                            "label": label,
                            "last_time": date,
                            "last_value": float(row.iloc[4]),
                            "url": "https://www.zh.ch/de/umwelt-tiere/wasser-gewaesser/messdaten/abfluss-wasserstand.html",
                            "source": "Kanton Zurich",
                            "icon": stations[label]["icon"],
                            "lake": stations[label]["lake"]
                        },
                        "geometry": {
                            "coordinates": stations[label]["coordinates"],
                            "type": "Point"}})
    return features