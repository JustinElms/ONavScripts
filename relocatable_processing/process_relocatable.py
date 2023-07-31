import argparse

import numpy as np
import xarray as xr


def process_relocatable(input_file=None, output_file=None) -> None:
    # load input data
    input_ds = xr.open_dataset(input_file)

    # get dataset coordinates and attributes
    longitude = np.unique(input_ds.lon.data)
    latitude = np.unique(input_ds.lat.data)
    # convert time to seconds since 1950-01-01
    time = np.array(
        [
            (t - np.datetime64("1950-01-01T00:00:00")) / np.timedelta64(1, "s")
            for t in input_ds.time.data
        ]
    )

    output_coords = {
        "longitude": longitude,
        "latitude": latitude,
        "time": time,
    }

    # get list of variables from nc file
    ds_variables = list(input_ds.keys())

    # create placeholder for processed variable data
    output_variables = {
        variable: (
            ["time", "latitude", "longitude"],
            np.full((input_ds.time.size, latitude.size, longitude.size), np.nan),
            input_ds[variable].attrs,
        )
        for variable in ds_variables
    }

    # convert output to user grid
    for n in input_ds.ncell.data:
        lat = input_ds.lat.data[n]
        lon = input_ds.lon.data[n]

        lat_idx = np.argwhere(latitude == lat).flatten()
        lon_idx = np.argwhere(longitude == lon).flatten()

        for variable in output_variables.items():
            input_data = input_ds[variable[0]].data[:, n]
            output_shape = variable[1][1][:, lat_idx, lon_idx].shape
            variable[1][1][:, lat_idx, lon_idx] = input_data.reshape(output_shape)

    # create xarray dataset from output data
    nc_out = xr.Dataset(data_vars=output_variables, coords=output_coords)

    # save as nc4 file:
    nc_out.to_netcdf(output_file)


if __name__ == "__main__":
    input = None
    output = None

    parser = argparse.ArgumentParser(
        prog="process_relocatable",
        description="Converts relocatable model output data to usergrid with \
            ONAV compatable coordinates.",
    )

    parser.add_argument("-i", "--input")
    parser.add_argument("-o", "--output")

    args = parser.parse_args()

    process_relocatable(args.input, args.output)
