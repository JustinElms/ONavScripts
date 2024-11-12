from pathlib import Path

import numpy as np
import xarray as xr


def update_timestamps() -> None:
    output_dir = "/data/gsl_daily_mod"
    years = np.arange(1970, 2100)
    for year in years:
        file = Path(f"/data/gsl_daily/CCSI_HF_{year}_icemod.nc")
        print(file.name)

        ds = xr.open_dataset(file, decode_times=False)

        attrs = ds.time_counter.attrs
        attrs["units"] = "seconds since 1950-01-01 00:00:00"
        attrs.pop("time_origin")

        dates = np.arange(
            np.datetime64(f"{year}-01-01"), np.datetime64(f"{year + 1}-01-01")
        )
        timestamps = dates.astype("datetime64[s]") - np.datetime64("1950-01-01")
        timestamps = timestamps.astype(int)

        ds = ds.assign(time_counter=timestamps)
        ds.time_counter.attrs = attrs

        ds = ds.drop_vars(
            [
                "ndastp",
                "model_time",
                "model_time_step",
                "deptht",
            ]
        )

        ds = ds.rename(
            {
                "time_counter": "time",
                "nav_lat": "latitude",
                "nav_lon": "longitude",
            }
        )

        ds.to_netcdf(f"{output_dir}/{file.name}")
        print("Done!")


if __name__ == "__main__":
    update_timestamps()
