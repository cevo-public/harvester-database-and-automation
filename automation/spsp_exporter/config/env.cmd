@rem Run this script on Windows to configure the environment.
@rem This is but a template. Fill in the actual values below.
@echo off

rem Database connection
set DATABASE_NAME=
set DATABASE_HOST=
set DATABASE_PORT=5432
set DATABASE_USER=
set DATABASE_PASSWORD=

rem Email notifications
set EMAIL_HOST=
set EMAIL_PORT=587
set EMAIL_USER=
set EMAIL_PASSWORD=
set EMAIL_SENDER=
set EMAIL_RECIPIENTS=

rem V-pipe file system mount
set VPIPE_HOST=
set VPIPE_ROOT=
set VPIPE_USER=
set VPIPE_IDENTITY=SPSP-Exporter

rem Raw data upload
set RAWDATA_IDENTITY=raw_data_upload
set RAWDATA_PASSPHRASE=

rem SPSP transfer
set SPSP_SERVER_IP=
set SPSP_LAB_CODE=

rem Run-time behavior
set RUN_INTERVAL=43200
