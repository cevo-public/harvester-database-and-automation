#!/bin/bash
set -euo pipefail

echo "username=${NETHZ_USERNAME}" > /credentials.txt
echo "password=${NETHZ_PASSWORD}" >> /credentials.txt
echo "domain=d.ethz.ch" >> /credentials.txt

echo NAS path: $NAS_PATH

DIR1=backup
URL1=$NAS_PATH/backup

# Mount folder
timestamp=`date +%Y/%m/%d-%H:%M:%S`
echo "[$timestamp] Mounting backup file server"
mkdir -p "/mnt/$DIR1"
mount -v -t cifs "$URL1" "/mnt/$DIR1" -o credentials=/credentials.txt

# Execute program
java -jar /app/vineyard-1.0-SNAPSHOT.jar --config /app/config/harvester-config.yml ViollierMetadataReceiver
