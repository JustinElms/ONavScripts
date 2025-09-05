These scripts create a mask layer that differenties water from ice and land which will be used to produce ONAV basemap tiles. The resulting mask will have three valles: 0 = water, 1=land, and 2=ice. 

The scripts should be used as follows:

1. Run generate_mask_tiles.py - this will produce a tile grid for each zoom level with separate files for each tile corresponding to each layer.
2. Run combine_mask_tiles.py to merge these tiles into tiles with the corresponding values.
3. Run create_topo_nc - This copies the ETOPO NC files and merges the tiles from step 2 into a single mask layer in the new NetCDF file.