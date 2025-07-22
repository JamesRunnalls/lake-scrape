import os
import re
import requests
import pandas as pd
from sources.functions import parse_html_table, write_local_data


def temperature(stations, filesystem, min_date):
    """
    Water temperature data from SRV Annecy collected from https://en.lac-annecy.com/weather-forecast
    """
    features = []
    folder = os.path.join(filesystem, "media/lake-scrape/temperature")

    session = requests.Session()
    response = session.get("https://annecy.requea.com/rqdbs?dbId=5ceed418236e465fae80de9599b9e559")
    if response.status_code == 200:
        result = response.text
        match = re.search(r'<input type="hidden" value="([a-f0-9]+)" name="ctx"\s*/?>', result)
        if match:
            ctx = match.group(1)
            response = session.get(f"https://annecy.requea.com/rqdb3?ctx={ctx}&wgt=c09e75ffa2f5480283450951269526c5")
            if response.status_code == 200:
                data = response.json()
                df = pd.DataFrame(data["series"][0]["data"], columns=["time", "value"])
                df["time"] = df["time"] / 1000
                df['value'] = pd.to_numeric(df['value'], errors='coerce')
                df = df.sort_values("time")
                key = "srv_annecy"
                write_local_data(os.path.join(folder, key), df)
                df = df.dropna(subset=['value'])
                row = df.iloc[-1]
                date = row["time"]
                if date > min_date:
                    features.append({
                        "type": "Feature",
                        "id": key,
                        "properties": {
                            "label": "SRV Annecy",
                            "last_time": date,
                            "last_value": row["value"],
                            "depth": "surface",
                            "url": f"https://en.lac-annecy.com/weather-forecast/",
                            "source": "SRV Annecy",
                            "icon": "lake",
                            "lake": "annecy"
                        },
                        "geometry": {
                            "coordinates": [6.135, 45.895],
                            "type": "Point"}})
    return features
