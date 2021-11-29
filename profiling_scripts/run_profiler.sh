#!/bin/bash

url="https://navigator.oceansdata.ca/"
config="/home/ubuntu/ONavScripts/profiling_scripts/api_profiling_config.json"
max_time=120
max_attempts=3
file_id="$(whoami)-$(hostname)-$(hostname -I | awk '/10/ {print $1}')-$(date -u +%Y%m%d%H%M%S)"

python api_profiling_driver.py --url $url --config $config --id $file_id

scp -i ${HOME}/keys/onav *.csv profile@justin-Sky-X4.ent.dfo-mpo.ca
