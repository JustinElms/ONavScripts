import glob

import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from dask import array as da


mask_path = "/data/mask_output/mask_z{}_{}_{}.npy"
topo_path = "/data/misc/etopo_EPSG:3857_z{}.nc"
mask_output_path = "/data/misc/etopo_masked_EPSG:3857_z{}.nc"

tile_size = 2048
chunks = tile_size * 5

for z_idx in range(9):
    print(topo_path.format(z_idx))
    topo = xr.open_dataset(topo_path.format(z_idx), chunks=chunks)
    n_tiles = 2 ** (z_idx - 3)
    z_tile_size = tile_size
    if n_tiles < 1:
        z_tile_size = topo.lat.size
        n_tiles = 1

    topo_mask = da.zeros(
        shape=(n_tiles * z_tile_size, n_tiles * z_tile_size),
        chunks=chunks,
        dtype=np.int8,
    )

    for x_idx in range(n_tiles):
        for y_idx in range(n_tiles):
            x_slice = slice(x_idx * z_tile_size, (x_idx + 1) * z_tile_size)
            y_slice = slice(y_idx * z_tile_size, (y_idx + 1) * z_tile_size)
            with open(mask_path.format(z_idx, x_idx, y_idx), "rb") as f:
                topo_mask[y_slice, x_slice] = np.load(f)

    topo["mask"] = xr.DataArray(
        data=topo_mask,
        dims=["lat", "lon"],
        coords=dict(
            lat=("lat", topo.lat.data, topo.lat.attrs),
            lon=("lon", topo.lon.data, topo.lon.attrs),
        ),
        attrs={"key": "0: bathymetry, 1: land topography, 2: ice topography"}
    )
    topo.to_netcdf(mask_output_path.format(z_idx))
