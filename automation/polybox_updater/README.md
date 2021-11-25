# Polybox Updater

This program synchronizes with Polybox periodically.


## Environment variables

The following environment variables are mandatory:

* `POLYBOX_USERNAME`
* `POLYBOX_PASSWORD`
* `POLYBOX_DIRECTORIES` - The directories that should be synchronized separated by `,`. Single files cannot be synchronized but only whole folders. Please make sure that the path starts with a `/`. Example: `/my projects/pipeline,/shared/covid`
* `$SYNCHRONIZATION_INTERVAL` - Example values: `120s`, `5m`, `1h`


## Volumes

* `/polybox`
