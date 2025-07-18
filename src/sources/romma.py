import os
import re
import requests
import pandas as pd
from sources.functions import parse_html_table, write_local_data


def temperature(stations, filesystem, min_date):
    """
    Water temperature data from ROMMA
    https://www.romma.fr/
    """
    features = []
    folder = os.path.join(filesystem, "media/lake-scrape/temperature")
    for station in stations:
        response = requests.get(f"https://www.romma.fr/station_jour.php?id={station['id']}")
        if response.status_code == 200:
            result = response.text
            match = re.search(r'<input name="date" type="hidden"\s+value="([\d\-]+)">', result)
            if match:
                date_str = match.group(1)
                df = parse_html_table(result).iloc[5:-1, [0, station["column"]]]
                df.columns = ["hour", "value"]
                df['datetime'] = date_str + ' ' + df['hour']
                df['time'] = pd.to_datetime(df['datetime'], format='%d-%m-%Y %H:%M').dt.tz_localize('Europe/Paris').astype(int) // 10 ** 9
                df = df[["time", "value"]]
                df['value'] = pd.to_numeric(df['value'], errors='coerce')
                df = df.dropna(subset=['value'])
                df = df.sort_values("time")
                key = "romma_{}".format(station["id"])
                write_local_data(os.path.join(folder, key), df)
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
                            "url": f"https://www.romma.fr/station_24.php?id={station['id']}&tempe=1",
                            "source": "ARSO",
                            "depth": station["depth"],
                            "icon": station["icon"],
                            "lake": station["lake"]
                        },
                        "geometry": {
                            "coordinates": station["coordinates"],
                            "type": "Point"}})
    return features
