import os
import re
import pytz
import requests
import pandas as pd
from datetime import datetime
from sources.functions import parse_html_table, write_local_data


def temperature(stations, filesystem, min_date):
    """
    Water temperature data from APPA
    https://www.appa.provincia.tn.it/content/view/full/23087
    """
    features = []
    folder = os.path.join(filesystem, "media/lake-scrape/temperature")
    response = requests.get(f"https://docs.google.com/spreadsheets/d/e/2PACX-1vRxZAWmP2SPtuDepEMql7_Ht7g7Aq0_HeumgZ6VlV4dMSyzaWuzyts8wN-ckI28Cb7JPnbSxfC42m9X/pubchart?oid=1574049628")
    if response.status_code == 200:
        result = response.text
        value = float(result.split("°C")[0].split("x22")[-1])
        match = re.search(r"\d{2}\\/\d{2}\\/\d{4} \d{2}:\d{2}:\d{2}", result)
        if match:
            date = match.group().replace('\\/', '/')
            tz = pytz.timezone("Europe/Rome")
            date = datetime.strptime(date, "%d/%m/%Y %H:%M:%S")
            date = int(tz.localize(date).timestamp())
            df = pd.DataFrame({'time': [date], "value": [value]})
            key = "appa_garda"
            write_local_data(os.path.join(folder, key), df)
            if date > min_date:
                features.append({
                    "type": "Feature",
                    "id": key,
                    "properties": {
                        "label": "Spiaggia dei Sabbioni, Lago di Garda",
                        "last_time": date,
                        "last_value": value,
                        "depth": 1,
                        "url": "https://www.appa.provincia.tn.it/content/view/full/23087",
                        "source": "APPA",
                        "icon": "lake",
                        "lake": "garda"
                    },
                    "geometry": {
                        "coordinates": [10.848, 45.88],
                        "type": "Point"}})
    return features
