import pickle

import copernicusmarine

logged_in = copernicusmarine.login(check_credentials_valid=True)
if not logged_in:
    copernicusmarine.login()

prod_id = "GLOBAL_MULTIYEAR_PHY_001_030"
datasets = [
    "cmems_mod_glo_phy_my_0.083deg_P1D-m",
    "cmems_mod_glo_phy_my_0.083deg_P1M-m",
    "cmems_mod_glo_phy_myint_0.083deg_P1D-m",
    "cmems_mod_glo_phy_myint_0.083deg_P1M-m",
    "cmems_mod_glo_phy_my_0.083deg-climatology_P1M-m"
]
output_directory = "/data/my.cmems-du.eu/"

meta = copernicusmarine.describe(product_id=prod_id)
version = [v.label for v in meta.products[0].datasets[0].versions][-1]

file_list = []
for dataset in datasets:
    files = copernicusmarine.get(
        dataset_id=dataset,
        dataset_version=version,
        output_directory="data",
        sync_delete=True,
    )
    file_list.append(files)

with open("glorys_result.pkl", "wb") as f:
    pickle.dump(file_list, f)
