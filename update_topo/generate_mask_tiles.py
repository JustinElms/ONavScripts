import multiprocessing

import geopandas as gpd
import numpy as np
import xarray as xr
from shapely import MultiPolygon, points, prepare, polygonize, Polygon

"""
Shorelines are furthermore organized into 6 hierarchical levels:
L1: boundary between land and ocean, except Antarctica.
L2: boundary between lake and land.
L3: boundary between island-in-lake and lake.
L4: boundary between pond-in-island and island.
L5: boundary between Antarctica ice and ocean.
L6: boundary between Antarctica grounding-line and ocean.

Rivers are organized into 10 classification levels:
L0: Double-lined rivers (river-lakes).
L1: Permanent major rivers.
L2: Additional major rivers.
L3: Additional rivers.
L4: Minor rivers.
L5: Intermittent rivers - major.
L6: Intermittent rivers - additional.
L7: Intermittent rivers - minor.
L8: Major canals.
L9: Minor canals.
L10: Irrigation canals.
"""

def fix_ice(gdf):
    ice_shp = gdf.loc[0,"geometry"]
    lons, lats = ice_shp.exterior.xy
    pts = np.stack([lons, lats], axis=1)
    pts = pts[2:-1]
    pts = np.concatenate([pts, [[180., -90.], [0., -90.]]])
    poly = Polygon(pts)
    gdf.loc[0,"geometry"] = poly
    return gdf


def mask_tile(args):
    lon_tile, lat_tile, x_idx, y_idx, z_idx, mask_names, shapefiles = args
    lon_grid, lat_grid = np.meshgrid(lon_tile, lat_tile)
    pts = np.stack([lon_grid, lat_grid], axis=2)
    pts = points(pts)
    for mask_name, shapefile in zip(mask_names, shapefiles):
        fname = f"mask_z{z_idx}_{x_idx}_{y_idx}_{mask_name}.npy"
        print(f"Starting {fname}")
        mask = np.zeros((lon_tile.size, lat_tile.size), dtype=int)
        gdf = gpd.read_file(shapefile)
        
        if mask_name == "ice":
            gdf = fix_ice(gdf)

        gdf = gdf.cx[
            lon_tile[0] : lon_tile[-1], lat_tile[0] : lat_tile[-1]
        ]

        if len(gdf) > 0:
            geoms = gdf["geometry"].values
            geom_types = gdf.geom_type.unique()
            if len(geom_types) == 1 and geom_types[0] == "Polygon":
                geoms = MultiPolygon(geoms)
            else:
                geoms = polygonize(geoms)
            prepare(geoms)
            mask = geoms.contains(pts).astype(int)

        with open("/data/mask_output/" + fname, "wb") as f:
            np.save(f, mask.astype(bool))
        print(f"Saved {fname}")


if __name__ == "__main__":
    land_shp = "/data/gshhg-shp-2.3.7/GSHHS_shp/f/GSHHS_f_L1.shp"
    lake_shp = "/data/gshhg-shp-2.3.7/GSHHS_shp/f/GSHHS_f_L2.shp"
    island_shp = "/data/gshhg-shp-2.3.7/GSHHS_shp/f/GSHHS_f_L3.shp"
    ice_shp = "/data/gshhg-shp-2.3.7/GSHHS_shp/f/GSHHS_f_L5.shp"
    river1_shp = "/data/gshhg-shp-2.3.7/WDBII_shp/f/WDBII_river_f_L01.shp"
    river2_shp = "/data/gshhg-shp-2.3.7/WDBII_shp/f/WDBII_river_f_L02.shp"
    river3_shp = "/data/gshhg-shp-2.3.7/WDBII_shp/f/WDBII_river_f_L03.shp"

    shapefiles = [
        land_shp,
        lake_shp,
        island_shp,
        ice_shp,
        river1_shp,
        river2_shp,
        river3_shp,
    ]

    mask_names = ["land", "lake", "island", "ice", "river1", "river2", "river3"]
    topo_path = "/data/misc/etopo_EPSG:3857_z{}.nc"
    tile_size = 2048

    for z_idx in range(9):
        with xr.open_dataset(topo_path.format(z_idx)) as topo:
            n_tiles = topo.lat.data.size / tile_size
            if n_tiles < 1:
                n_tiles = 1
            lat_tiles = np.array_split(topo.lat.data, n_tiles)
            lon_tiles = np.array_split(topo.lon.data, n_tiles)

        args = []
        for x_idx, lon_tile in enumerate(lon_tiles):
            for y_idx, lat_tile in enumerate(lat_tiles):
                args.append(
                    [lon_tile, lat_tile, x_idx, y_idx, z_idx, mask_names, shapefiles]
                )

        with multiprocessing.Pool(6) as pool:
            pool.map(mask_tile, args)
