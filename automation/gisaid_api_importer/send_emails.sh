#!/bin/bash
set -euo pipefail
shopt -s nullglob  # So that no error will be generated when the folder is empty.

DROP_OFF_LOCATION=$HOME/mail_dropoff
SENT_LOCATION=$DROP_OFF_LOCATION/sent

mkdir -p $SENT_LOCATION

for filename in $DROP_OFF_LOCATION/*.txt; do
    /usr/sbin/sendmail -t < $filename
    mv $filename $SENT_LOCATION
done

date > $DROP_OFF_LOCATION/last_checked
