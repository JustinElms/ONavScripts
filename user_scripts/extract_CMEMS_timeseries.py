import argparse
import io
import json
import requests
from datetime import datetime, timedelta
from urllib.parse import urlencode


import numpy as np
import pandas as pd


def get_timestamps(start_time: str, end_time: str) -> np.array:
    """
    Returns every 25th CMEMS formatted timestamp beween start_time and
    end_time. start_time and end_time should be in "YYYY-MM-DD" format.

    """
    ref_time = datetime(1950, 1, 1)

    date = datetime.strptime(start_time, "%Y-%m-%d")
    end_date = datetime.strptime(end_time, "%Y-%m-%d")

    timestamps = []
    while date < end_date:
        timestamp = date - ref_time
        timestamps.append(timestamp.days * 24 + 12)
        date += timedelta(25)

    timestamps.append((end_date - ref_time).days * 24 + 12)

    return np.unique(timestamps)


def get_nearest_depth_idx(depth: float) -> int:
    url = "https://www.oceannavigator.ca/api/v2.0/dataset/cmems_daily/thetao/depths"

    resp = requests.get(url)

    if resp.status_code == 200:
        data = resp.json()[2:]

        ids = [d["id"] for d in data]
        depths = [d["value"] for d in data]
        depths = np.array(depths)
        depth_int = [int(d["value"].replace("m", "")) for d in data]
        depth_int = np.array(depth_int)
        diff = np.abs(depth_int - depth)
        diff_idx = np.argwhere(diff == np.nanmin(diff)).squeeze()

        return str(depths[diff_idx]), ids[diff_idx]

    return "0.00 m", 0


def requestFile(lat, lon, start_time, end_time, depth, depth_idx) -> pd.DataFrame:
    # json object for the query
    query = {
        "dataset": "cmems_daily",
        "plotTitle": "",
        "point": [[lat, lon]],
        "showmap": 0,
        "size": "10x7",
        "dpi": 144,
        "type": "timeseries",
        "variable": "thetao",
        "variable_range": [None],
        "starttime": int(start_time),
        "endtime": int(end_time),
        "depth": str(depth_idx),
        "colormap": "default",
        "interp": "gaussian",
        "radius": 25,
        "neighbours": 10,
    }

    # Assemble full request
    base_url = "https://www.oceannavigator.ca/api/v2.0/plot/timeseries?"
    url = (
        base_url
        + urlencode({"query": json.dumps(query)})
        + "&save=True&format=csv&size=10x7&dpi=144"
    )

    # Extract data
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        try:
            data_strs = response.content.decode("utf-8").split("\n")
            data = "\n".join(data_strs[3:])
            df = pd.read_csv(io.StringIO(data))
            df.insert(loc=3, column="Depth", value=depth)
            return df
        except Exception as e:
            print(e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("lat", help="Latitude value (e.g. -45.0).")
    parser.add_argument("lon", help="Longitude value (e.g. 51.2).")
    parser.add_argument(
        "start_time",
        help="Start date of timeseries in YYYY-MM-DD format (e.g. 2015-05-24).",
    )
    parser.add_argument(
        "end_time",
        help="End date of timeseries in YYYY-MM-DD format (e.g. 2016-08-01).",
    )
    parser.add_argument(
        "--depth",
        "-d",
        type=float,
        help="Requested depth in meters (e.g. 100 will get nearest level to 100 m). Optional - default is 0 (surface).",
    )
    args = parser.parse_args()

    if args.depth:
        depth, depth_idx = get_nearest_depth_idx(args.depth)
    else:
        depth = "0.00 m"
        depth_idx = 0

    timestamps = get_timestamps(args.start_time, args.end_time)

    csv_data = None

    for t0, t1 in zip(timestamps, timestamps[1:]):
        print(f"Downloading timestamps {t0}...{t1}")
        data = requestFile(args.lat, args.lon, t0, t1, depth, depth_idx)

        if not isinstance(data, pd.DataFrame):
            continue

        if not isinstance(csv_data, pd.DataFrame):
            csv_data = data
        else:
            csv_data = pd.concat([csv_data, data])

    csv_data.reset_index(drop=True, inplace=True)
    csv_data.drop_duplicates(inplace=True)
    csv_data.to_csv(
        f"CMEMS_timeseries_{args.lat}_{args.lon}_{args.start_time}_{args.end_time}_{depth}.csv",
        index=False,
    )
