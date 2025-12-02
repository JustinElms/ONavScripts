#!/usr/bin/env bash

source ${HOME}/.bashrc
conda activate transfer-tools

CLASS4_PATH="/gpfs/fs7/dfo/dpnm/sedoo/class4/Database/"

cd ${HOME}
[ ! -d /tank/data/class4/$(date +%Y) ] && /home/jue000/mc mb myminio/data/class4/$(date +%Y)

/fs/vnas_Hdfo/dpnm/jue000/rclone-v1.64.0-linux-amd64/rclone -P --bwlimit "08:00,4096 18:00,8192k" copy  ${CLASS4_PATH}/$(date +%Y)/ digio:class4/$(date +%Y)/