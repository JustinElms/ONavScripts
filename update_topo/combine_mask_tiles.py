import multiprocessing

import numpy as np


def mask_tile(args):
    x, y, z = args
    mask_types = [
        "land",
        "lake",
        "river1",
        "river2",
        "river3",
        "island",
        "ice",
    ]
    mask = []
    for mask_type in mask_types:
        fname = f"mask_z{z}_{x}_{y}_{mask_type}.npy"
        with open("/data/mask_output/" + fname, "rb") as f:
            data = np.load(f)
        if len(mask) == 0:
            mask = data.astype(int)
            continue
        match mask_type:
            case "land" | "island":
                mask[data] = 1
            case "lake" | "river1" | "river2" | "river3":
                mask[data] = 0
            case "ice":
                mask[data] = 2
    with open(f"/data/mask_output/mask_z{z}_{x}_{y}.npy", "wb") as f:
        np.save(f, mask)
        print(f"Saved mask_z{z}_{x}_{y}.npy")


if __name__ == "__main__":
    args = []
    for z_idx in range(9):
        n_tiles = 2 ** (z_idx - 3)
        if n_tiles < 1:
            n_tiles = 1
        for x_idx in range(n_tiles):
            for y_idx in range(n_tiles):
                args.append([x_idx, y_idx, z_idx])

    with multiprocessing.Pool(6) as pool:
        pool.map(mask_tile, args)
