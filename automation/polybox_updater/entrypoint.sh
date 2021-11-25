#!/bin/bash
set -euo pipefail

while true; do
  timestamp=`date +%Y/%m/%d-%H:%M:%S`
  echo "[$timestamp] Starting synchronization"

  IFS=',' read -ra ADDR <<< "$POLYBOX_DIRECTORIES"
  for i in "${ADDR[@]}"; do
    timestamp=`date +%Y/%m/%d-%H:%M:%S`
    echo "[$timestamp] Synchronizing $i"
    mkdir -p "/polybox$i"
    # Polybox might throw errors if (for unexplainable reason) it fails to synchronize a file. But it seems that it will
    # first synchronize all the other files so that we can simply ignore these errors.
    owncloudcmd -s -u $POLYBOX_USERNAME -p $POLYBOX_PASSWORD "/polybox$i" "https://polybox.ethz.ch/remote.php/webdav$i" || true
  done

  timestamp=`date +%Y/%m/%d-%H:%M:%S`
  echo "[$timestamp] Finished synchronization. Next synchronization is in ${SYNCHRONIZATION_INTERVAL}."
  sleep $SYNCHRONIZATION_INTERVAL
done
