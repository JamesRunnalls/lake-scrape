import os
import math
import pandas as pd
from html.parser import HTMLParser
import xml.etree.ElementTree as ET

def write_local_data(filepath, data):
    df = data.copy(deep=True)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    for year in range(df['time'].min().year, df['time'].max().year + 1):
        station_year_file = os.path.join(filepath, f"{year}.csv")
        station_year_data = df[df['time'].dt.year == year]
        if not os.path.exists(station_year_file):
            os.makedirs(os.path.dirname(station_year_file), exist_ok=True)
            station_year_data = station_year_data.dropna(subset=['value'])
            station_year_data['time'] = station_year_data['time'].astype('int64') // 10 ** 9
            station_year_data.to_csv(station_year_file, index=False)
        else:
            df_existing = pd.read_csv(station_year_file)
            df_existing['time'] = pd.to_datetime(df_existing['time'], unit='s')
            combined = pd.concat([df_existing, station_year_data])
            combined = combined.drop_duplicates(subset=['time'], keep='last')
            combined = combined.sort_values(by='time')
            combined = combined.dropna(subset=['value'])
            combined['time'] = combined['time'].astype('int64') // 10 ** 9
            combined.to_csv(station_year_file, index=False)

def ch1903_plus_to_latlng(x, y):
    x_aux = (x - 2600000) / 1000000
    y_aux = (y - 1200000) / 1000000
    lat = 16.9023892 + 3.238272 * y_aux - 0.270978 * x_aux ** 2 - 0.002528 * y_aux ** 2 - 0.0447 * x_aux ** 2 * y_aux - 0.014 * y_aux ** 3
    lng = 2.6779094 + 4.728982 * x_aux + 0.791484 * x_aux * y_aux + 0.1306 * x_aux * y_aux ** 2 - 0.0436 * x_aux ** 3
    lat = (lat * 100) / 36
    lng = (lng * 100) / 36
    return lat, lng

def cart_to_latlng(easting, northing):
    """
    Convert EPSG:31258 (MGI / Austria GK M31) to EPSG:4326 (WGS84) coordinates
    Based on the official EPSG parameters
    """

    # Step 1: Inverse Transverse Mercator projection (MGI GK M31 -> MGI Geographic)
    # EPSG:31258 parameters from epsg.io
    a = 6377397.155  # Bessel 1841 semi-major axis
    f = 1 / 299.1528128  # Bessel 1841 flattening

    # Transverse Mercator parameters
    lat_0 = 0.0  # Latitude of natural origin
    lon_0 = math.radians(13.3333333333333)  # Longitude of natural origin
    k_0 = 1.0  # Scale factor
    x_0 = 450000.0  # False easting
    y_0 = -5000000.0  # False northing

    # Remove false easting and northing
    x = easting - x_0
    y = northing - y_0

    # Calculate derived constants
    e = math.sqrt(2 * f - f * f)  # First eccentricity
    e_prime = e / math.sqrt(1 - e * e)  # Second eccentricity
    n = f / (2 - f)  # Third flattening

    # Calculate M (meridional arc)
    M = y / k_0

    # Calculate mu (footprint latitude)
    mu = M / (a * (1 - e * e / 4 - 3 * e ** 4 / 64 - 5 * e ** 6 / 256))

    # Calculate e1
    e1 = (1 - math.sqrt(1 - e * e)) / (1 + math.sqrt(1 - e * e))

    # Calculate footprint latitude
    lat_fp = mu + (3 * e1 / 2 - 27 * e1 ** 3 / 32) * math.sin(2 * mu) + \
             (21 * e1 ** 2 / 16 - 55 * e1 ** 4 / 32) * math.sin(4 * mu) + \
             (151 * e1 ** 3 / 96) * math.sin(6 * mu) + \
             (1097 * e1 ** 4 / 512) * math.sin(8 * mu)

    # Calculate rho1 and nu1
    rho1 = a * (1 - e * e) / (1 - e * e * math.sin(lat_fp) ** 2) ** (3 / 2)
    nu1 = a / math.sqrt(1 - e * e * math.sin(lat_fp) ** 2)

    # Calculate T1, C1, D
    T1 = math.tan(lat_fp) ** 2
    C1 = e_prime ** 2 * math.cos(lat_fp) ** 2
    D = x / (nu1 * k_0)

    # Calculate latitude
    lat = lat_fp - (nu1 * math.tan(lat_fp) / rho1) * \
          (D ** 2 / 2 - (5 + 3 * T1 + 10 * C1 - 4 * C1 ** 2 - 9 * e_prime ** 2) * D ** 4 / 24 + \
           (61 + 90 * T1 + 298 * C1 + 45 * T1 ** 2 - 252 * e_prime ** 2 - 3 * C1 ** 2) * D ** 6 / 720)

    # Calculate longitude
    lon = lon_0 + (D - (1 + 2 * T1 + C1) * D ** 3 / 6 + \
                   (5 - 2 * C1 + 28 * T1 - 3 * C1 ** 2 + 8 * e_prime ** 2 + 24 * T1 ** 2) * D ** 5 / 120) / math.cos(
        lat_fp)

    # Convert to degrees
    lat_mgi = math.degrees(lat)
    lon_mgi = math.degrees(lon)

    # Step 2: Datum transformation MGI -> WGS84 using 7-parameter Helmert transformation
    # TOWGS84 parameters from EPSG:31258
    dx = 577.326  # X-axis translation
    dy = 90.129  # Y-axis translation
    dz = 463.919  # Z-axis translation
    rx = 5.137  # X-axis rotation (arcseconds)
    ry = 1.474  # Y-axis rotation (arcseconds)
    rz = 5.297  # Z-axis rotation (arcseconds)
    ds = 2.4232  # Scale factor (ppm)

    # Convert to radians and proper units
    rx_rad = math.radians(rx / 3600)  # arcseconds to radians
    ry_rad = math.radians(ry / 3600)
    rz_rad = math.radians(rz / 3600)
    ds_factor = ds * 1e-6  # ppm to factor

    # Convert MGI lat/lon to cartesian coordinates (Bessel 1841)
    lat_rad = math.radians(lat_mgi)
    lon_rad = math.radians(lon_mgi)

    N = a / math.sqrt(1 - e * e * math.sin(lat_rad) ** 2)

    X_mgi = N * math.cos(lat_rad) * math.cos(lon_rad)
    Y_mgi = N * math.cos(lat_rad) * math.sin(lon_rad)
    Z_mgi = N * (1 - e * e) * math.sin(lat_rad)

    # Apply 7-parameter transformation
    X_wgs84 = dx + (1 + ds_factor) * X_mgi + rz_rad * Y_mgi - ry_rad * Z_mgi
    Y_wgs84 = dy - rz_rad * X_mgi + (1 + ds_factor) * Y_mgi + rx_rad * Z_mgi
    Z_wgs84 = dz + ry_rad * X_mgi - rx_rad * Y_mgi + (1 + ds_factor) * Z_mgi

    # Convert WGS84 cartesian back to lat/lon
    # WGS84 parameters
    a_wgs84 = 6378137.0
    f_wgs84 = 1 / 298.257223563
    e_wgs84 = math.sqrt(2 * f_wgs84 - f_wgs84 * f_wgs84)

    # Calculate longitude
    lon_wgs84 = math.atan2(Y_wgs84, X_wgs84)

    # Calculate latitude iteratively
    p = math.sqrt(X_wgs84 ** 2 + Y_wgs84 ** 2)
    lat_wgs84 = math.atan2(Z_wgs84, p * (1 - e_wgs84 * e_wgs84))

    for i in range(5):  # Iterate for precision
        N_wgs84 = a_wgs84 / math.sqrt(1 - e_wgs84 * e_wgs84 * math.sin(lat_wgs84) ** 2)
        lat_wgs84 = math.atan2(Z_wgs84 + e_wgs84 * e_wgs84 * N_wgs84 * math.sin(lat_wgs84), p)

    # Convert to degrees
    lat_final = math.degrees(lat_wgs84)
    lon_final = math.degrees(lon_wgs84)

    return lat_final, lon_final

class TableHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows = []
        self.current_row = []
        self.in_td = False

    def handle_starttag(self, tag, attrs):
        if tag == 'td':
            self.in_td = True

    def handle_endtag(self, tag):
        if tag == 'tr':
            if self.current_row:
                self.rows.append(self.current_row)
                self.current_row = []
        elif tag == 'td':
            self.in_td = False

    def handle_data(self, data):
        if self.in_td:
            self.current_row.append(data.strip())

def parse_html_table(html_table):
    parser = TableHTMLParser()
    parser.feed(html_table)
    df = pd.DataFrame(parser.rows)
    return df

class CustomHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.root = ET.Element("root")
        self.current = self.root
        self.stack = [self.root]  # Stack to keep track of parent elements

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        element = ET.SubElement(self.current, tag, attrs_dict)
        self.stack.append(self.current)  # Push current element to stack
        self.current = element

    def handle_endtag(self, tag):
        self.current = self.stack.pop()  # Pop from stack to move to the parent

    def handle_data(self, data):
        if self.current.text is None:
            self.current.text = data
        else:
            self.current.text += data

def parse_html(html_string):
    parser = CustomHTMLParser()
    parser.feed(html_string)
    return parser.root

def html_find_all(element, tag=None, class_name=None, attributes=None):
    def match_attributes(el, attributes):
        if not attributes:
            return True
        for attr, value in attributes.items():
            if el.get(attr) != value:
                return False
        return True

    def match_class_name(el, class_name):
        if class_name is None:
            return True
        return class_name in el.get('class', '').split()

    # Recursively search for matching elements
    def find_all_recursive(el, matches):
        if ((tag is None or el.tag == tag) and
            match_class_name(el, class_name) and
            match_attributes(el, attributes)):
            matches.append(el)
        for child in el:
            find_all_recursive(child, matches)

    matches = []
    find_all_recursive(element, matches)
    return matches