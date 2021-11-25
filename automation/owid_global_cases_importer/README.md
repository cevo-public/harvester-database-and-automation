# OWID Global Cases Importer

This program imports the global incidence numbers from https://covid.ourworldindata.org/data/owid-covid-data.csv and
writes them into `ext_owid_global_cases`.


## Environment variables

The following environment variables are mandatory:

* `DB_HOST`
* `DB_USER`
* `DB_PASSWORD`
* `DB_DBNAME`
* `EMAILS_ACTIVATED` - Whether notification emails should be sent. If this is `true`, the other `EMAILS_` fields are required as well.
* `EMAILS_SENDER_SMTP_HOST`
* `EMAILS_SENDER_SMTP_PORT`
* `EMAILS_SENDER_SMTP_USERNAME`
* `EMAILS_SENDER_SMTP_PASSWORD`
* `EMAILS_SENDER_EMAIL`
* `EMAILS_RECIPIENTS` - The recipients of the notification emails. Multiple email addresses can be provided, separated by comma.
* `CHECK_FOR_NEW_DATA_INTERVAL_SECONDS`
