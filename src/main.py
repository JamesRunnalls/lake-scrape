# -*- coding: utf-8 -*-
import os
import json
import time
import boto3
import requests
import argparse
import importlib
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed


def run_process(key, config, type, filesystem, min_date):
    try:
        module = importlib.import_module(f"sources.{key}")
        process_function = getattr(module, type)
        start_time = time.time()
        result = process_function(config, filesystem, min_date)
        end_time = time.time()
        print(f"{key}: {type} took {end_time - start_time:.2f} seconds")
        return key, result
    except Exception as e:
        print(e)
        return key, False

def main(params):
    repo = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    min_date = (datetime.now() - timedelta(days=14)).timestamp()
    features = []
    failed = {}
    with open(os.path.join(repo, f'{params["type"]}.json'), 'r') as file:
        stations = json.load(file)

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(run_process, key, config, params["type"], params["filesystem"], min_date)
            for key, config in stations.items()
        ]
        for future in as_completed(futures):
            key, result = future.result()
            if isinstance(result, list):
                features = features + result
            else:
                failed[key] = 1

    if params["merge"] and params["bucket"]:
        response = requests.get("{}/insitu/summary/water_{}.geojson".format(params["bucket"], params["type"]))
        if response.status_code == 200:
            ids = [f["id"] for f in features]
            old_features = response.json()["features"]
            for f in old_features:
                if f["id"] not in ids:
                    features.append(f)

    geojson = {
        "type": "FeatureCollection",
        "name": "Current water {}".format(params["type"]),
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}},
        "features": features
    }

    local_file = os.path.join(params["filesystem"], "media/lake-scrape/water_{}.geojson".format(params["type"]))
    with open(local_file, 'w') as json_file:
        json.dump(geojson, json_file)

    if params["upload"]:
        bucket_key = params["bucket"].split(".")[0].split("//")[1]
        s3 = boto3.client("s3", aws_access_key_id=params["aws_id"], aws_secret_access_key=params["aws_key"])
        s3.upload_file(local_file, bucket_key, "insitu/summary/water_{}.geojson".format(params["type"]))

    fail_file = os.path.join(params["filesystem"], "media/lake-scrape/failed_{}.json".format(params["type"]))
    if os.path.exists(fail_file):
        with open(fail_file, 'r') as f:
            fail_list = json.load(f)
    else:
        fail_list = {}

    for key in failed.keys():
        if key in fail_list:
            failed[key] = failed[key] + fail_list[key]
    with open(fail_file, 'w') as f:
        json.dump(failed, f)

    if len(failed.keys()) > 0:
        print("Failed for {}".format(", ".join(failed.keys())))
        if 4 in failed.values():
            raise ValueError("WARNING. Continual failures for: {}".format(", ".join([key for key, value in failed.items() if value == 4])))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--type', '-t', help="Type of processing (temperature or level)", choices=['temperature', 'level'], required=True)
    parser.add_argument('--filesystem', '-f', help="Path to local storage filesystem", type=str, required=True)
    parser.add_argument('--upload', '-u', help="Upload current value to S3 bucket", action='store_true')
    parser.add_argument('--merge', '-m', help="Upload current value to S3 bucket", action='store_true')
    parser.add_argument('--bucket', '-b', help="S3 bucket", type=str, )
    parser.add_argument('--aws_id', '-i', help="AWS ID", type=str, )
    parser.add_argument('--aws_key', '-k', help="AWS KEY", type=str, )
    args = parser.parse_args()
    main(vars(args))