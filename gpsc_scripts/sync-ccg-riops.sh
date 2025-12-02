#!/usr/bin/env bash

source ${HOME}/.bashrc

DATE=$(date +%Y%m%d)
YDAY=$(date --date="yesterday")

TMP_DIR="${HOME}/ccg_riops_tmp/${DATE}"
[ -d $TMP_DIR ] && rm -r $TMP_DIR
mkdir -p $TMP_DIR

lftp -e "lcd ${TMP_DIR} ; mirror --parallel=5 --include ${DATE}; bye" depot.cmc.ec.gc.ca/ftp/cmoi/dfo/dfo.ccg/
/fs/vnas_Hdfo/dpnm/jue000/rclone-v1.64.0-linux-amd64/rclone -P --bwlimit "08:00,4096 18:00,8192k" copy  ${TMP_DIR}/ digio:depot.cmc.ec.gc.ca/ftp/cmoi/dfo/dfo.ccg/${DATE}/

rm -r "${HOME}/ccg_riops_tmp/${YDAY}"