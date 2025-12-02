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


def requestFile(lat, lon, start_time, end_time) -> pd.DataFrame:
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
        "depth": 0,
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
            depth = data_strs[2].split(" ")[-1]
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
    args = parser.parse_args()

    timestamps = get_timestamps(args.start_time, args.end_time)

    csv_data = None

    for t0, t1 in zip(timestamps, timestamps[1:]):
        print(f"Downloading timestamps {t0}...{t1}")
        data = requestFile(args.lat, args.lon, t0, t1)

        if not isinstance(data, pd.DataFrame):
            continue

        if not isinstance(csv_data, pd.DataFrame):
            csv_data = data
        else:
            csv_data = pd.concat([csv_data, data])

    csv_data.reset_index(drop=True, inplace=True)
    csv_data.to_csv(
        f"CMEMS_timeseries_{args.lat}_{args.lon}_{args.start_time}_{args.end_time}.csv"
    )
