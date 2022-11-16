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
echo "[$timestamp] Mounting pangolin file server"
mkdir -p "/mnt/$DIR1"
mount -v -t cifs "$URL1" "/mnt/$DIR1" -o credentials=/credentials.txt


echo POLYBOX path: $POLYBOX_PATH
DIR2=webdav
echo "[$timestamp] Mounting polybox server"

echo "$POLYBOX_PATH ${NETHZ_USERNAME} ${NETHZ_PASSWORD}" > /etc/davfs2/secrets
mkdir -p "/mnt/$DIR2"
mount -v -t davfs $POLYBOX_PATH "/mnt/$DIR2"
echo "$POLYBOX_PATH /mnt/$DIR2"
# Execute program
python3 /app/viollier_metadata_receiver.py
umount.davfs "/mnt/$DIR2"
