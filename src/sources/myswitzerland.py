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
    Water temperature data from various Badi's collected by MySwitzerland
    https://sospo.myswitzerland.com/lakesides-swimming-pools/
    """
    features = []
    folder = os.path.join(filesystem, "media/lake-scrape/temperature")
    swiss_timezone = pytz.timezone("Europe/Zurich")
    for station in stations:
        try:
            time.sleep(random.uniform(0, 0.3))
            response = requests.get("https://sospo.myswitzerland.com/lakesides-swimming-pools/{}".format(station))
            if response.status_code == 200:
                root = parse_html(response.text)
                element = html_find_all(root, tag="a", class_name="AreaMap--link")
                location = element[0].get('href').split("?q=")[-1].split(",")
                coords = [float(location[1]), float(location[0])]
                label = html_find_all(root, tag="h1", class_name="PageHeader--title")[0].text
                date = datetime.strptime(
                    html_find_all(root, tag="div", class_name="QuickFactsWidget--info")[0].text.strip().split(": ",
                                                                                                              1)[1],
                    "%d.%m.%Y, %H:%M")
                date = swiss_timezone.localize(date).timestamp()
                value = False
                icon = "lake"
                for info in html_find_all(root, tag="ul", class_name="QuickFacts--info"):
                    c = html_find_all(info, tag="li", class_name="QuickFacts--content")
                    if len(c) == 1:
                        content = c[0].text
                        value = html_find_all(info, tag="li", class_name="QuickFacts--value")[0].text
                        if content in ["Lake bathing", "River pools"] and value != "—":
                            if content == "River pools":
                                icon = "river"
                            value = float(value.replace("°", ""))
                if value:
                    df = pd.DataFrame({'time': [date], "value": [value]})
                    key = "myswitzerland_{}".format(station)
                    write_local_data(os.path.join(folder, key), df)
                if value and date > min_date:
                    features.append({
                        "type": "Feature",
                        "id": key,
                        "properties": {
                            "label": label,
                            "last_time": date,
                            "last_value": value,
                            "url": "https://sospo.myswitzerland.com/lakesides-swimming-pools/{}".format(station),
                            "source": "MySwitzerland",
                            "icon": icon,
                            "lake": False
                        },
                        "geometry": {
                            "coordinates": coords,
                            "type": "Point"}})
        except Exception as e:
            print("FAILED: MySwitzerland: " + station)
            print(e)
    return features
