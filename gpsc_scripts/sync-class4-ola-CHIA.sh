#!/usr/bin/env bash

source ${HOME}/.bashrc
conda activate transfer-tools

CLASS4OLA_PATH="/gpfs/fs7/dfo/dpnm/ylc001/ECCC/Database/RIOPS2/anal/OLA_SAM2_RD"

DATE=$(date +%Y%m)
TMP_DIR="${HOME}/class4_ola_tmp/$(date +%Y)"
FILES=["${CLASS4OLA_PATH}/class4_${DATE}*"]

echo $DATE

[ -d $TMP_DIR ] && rm -r $TMP_DIR

mkdir -p $TMP_DIR
cd $TMP_DIR
echo $TMP_DIR

for D in {01..31}
do
  FILE="${CLASS4OLA_PATH}/class4_${DATE}${D}_RIOPS_2.0_SAM2_OLA.nc.gz"
  if [ -f $FILE ]
  then
    OUT_DIR="${DATE}${D}/"
    mkdir -p $OUT_DIR
    cp $FILE $OUT_DIR
    gunzip -d $OUT_DIR*
  fi
done


cd ${HOME}
[ ! -d /tank/data/class4/$(date +%Y) ] && /home/jue000/mc mb myminio/data/class4-ola/$(date +%Y)
/fs/vnas_Hdfo/dpnm/jue000/rclone-v1.64.0-linux-amd64/rclone -P --bwlimit "08:00,4096 18:00,8192k" copy  ${TMP_DIR}/ digio:class4-ola/$(date +%Y)/
