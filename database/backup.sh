#!/bin/bash
set -euo pipefail

# The goal is to have hourly backups for one day, daily backups for one week, weekly backups for a month, monthly
# backups for a year, and yearly backups (forever). The script should be executed every hour.

# The database authentication must be possible without entering passwords. This can be achieved by creating a .pgpass
# file.


# ------------------------------------------------------
# Settings

BACKUP_DIR=$HOME/sars_cov_2_database_backup
PG_DUMP=$HOME/postgres/bin/pg_dump
POSTGRES_HOST=
POSTGRES_BACKUP_USER=


# ------------------------------------------------------
# Preparations

mkdir -p $BACKUP_DIR/hourly $BACKUP_DIR/daily $BACKUP_DIR/weekly $BACKUP_DIR/monthly $BACKUP_DIR/yearly

HOUR=$(date +%-H)
DAY_IN_WEEK=$(date +%u)
WEEK_IN_MONTH=$((($(date +%-d)-1)/7+1))
MONTH=$(date +%-m)
YEAR=$(date +%Y)

cd $BACKUP_DIR


# ------------------------------------------------------
# Create Backup

$PG_DUMP -h $POSTGRES_HOST\
  -p 5432\
  -U $POSTGRES_BACKUP_USER\
  -d sars_cov_2\
  -n public\
  --no-privileges --no-owner -Fc\
  -f sars_cov_2.backup

cp sars_cov_2.backup hourly/hour-${HOUR}.backup
cp sars_cov_2.backup daily/day-${DAY_IN_WEEK}.backup
cp sars_cov_2.backup weekly/week-${WEEK_IN_MONTH}.backup
cp sars_cov_2.backup monthly/month-${MONTH}.backup
cp sars_cov_2.backup yearly/year-${YEAR}.backup
