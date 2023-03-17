# SPSP-Exporter

Exports the sequencing data for fully processed samples from V-pipe as
well as corresponding meta data from the database. Then prepares all
files in the required format and folder structure for submission to the
Swiss Pathogen Surveillance Platform ([SPSP]). Finally runs SPSP's
[Transfer Tool] to submit the new results to their SFTP server.

Note that this service was never deployed in production. A previous
iteration of the container was run manually as needed. It has not been
extensively tested since then, so errors are expected at run-time.

[SPSP]: https://spsp.ch
[Transfer Tool]: https://gitlab.sib.swiss/clinbio/spsp-ng/transfer-tool


## Configuration

The service supports the following configuration options via environment
variables:
```
# Database connection
DATABASE_NAME=
DATABASE_HOST=                           # Postgres server
DATABASE_PORT=5432                       # Postgres port
DATABASE_USER=
DATABASE_PASSWORD=

# Email notifications
EMAIL_HOST=                              # SMTP server
EMAIL_PORT=587                           # SMTP port
EMAIL_USER=                              # account name
EMAIL_PASSWORD=
EMAIL_SENDER=                            # email address of sender
EMAIL_RECIPIENTS=                        # comma-separated list

# V-pipe file system mount (to access the results)
VPIPE_HOST=                              # domain name of remote machine
VPIPE_ROOT=                              # folder containing `working`
VPIPE_USER=                              # account name of technical user
VPIPE_IDENTITY=SPSP-Exporter             # name of SSH private key file

# Raw data upload (to trigger uploads from there directly to SPSP)
RAWDATA_IDENTITY=raw_data_upload        # name of SSH private key file
RAWDATA_PASSPHRASE=                     # passphrase for that key

# SPSP transfer
SPSP_SERVER_IP=
SPSP_LAB_CODE=

# Run-time behavior
RUN_INTERVAL=43200                      # in seconds
```

## Deployment

The application is intended to be deployed as a Docker service. The
entry-point script loops indefinitely and checks in regular intervals
for new batches to be exported to SPSP. The upload of raw data files
(like `.fastq`) is handled by a separate component: [Raw-Data-Uploader].
It runs on the V-pipe machine and is triggered by an SSH "forced
command". This is to keep the transfer time reasonably low, given the
amount of data per week, but could be simplified in less demanding
circumstances.

[Raw-Data-Uploader]: https://gitlab.ethz.ch/sars_cov_2/s3c/raw_data_uploader


## Implementation

The application internally uses the following configuration files:
* `config.yml`:           database connection details
* `harvester-config.yml`: email sender account and recipients
* `raw-data-upload.yml`:  connection details to V-pipe machine
* `spsp-config.yml`:      mandatory columns and author list

Inside the Docker container, the files are generated dynamically from
the environment variables (by the script `create_configs.sh`) to make
deployment in production easier.

The entry point is effectively `SpspExporter.java`. It parses
`harvester-config.yml` to get the email configuration. Then delegates
to `export_spsp_submission.R` to query the database for samples to
release to SPSP.

The R script performs a number of quality and sanity checks, prepares
meta data for the sequencing results (such as a list of authors,
depending on the sequencing center), and writes out the files in the
required folder structure for the upload to SPSP.

The Java caller then takes over again and sends the export report to
the email recipients. Eventually, it runs the SPSP Transfer Tool via
the `transfer.sh` wrapper script. Then delegates once more to R, namely
`record_spsp_submission.R`, to mark the samples that were submitted as
such in the database.


## Development

To develop and test locally:
* Make a copy of the `database` folder in this folder here.
* Set the environment variables appropriately, according to the
  description above. Find templates in the `config` folder.
* Copy the necessary SSH private keys to the `identities` folder.
* Make sure you have [VS Code] installed as well as its [Dev Containers]
  extension.
* Open this very folder in VS Code, e.g. by running `code .` in it.
  (It needs to inherit the environment in which the variables were
  defined.)
* You will be prompted to "Reopen in Container".
* Inside the container, run `create_configs.sh` and `vpipe_mount.sh`.

This will let you work with the code in an environment that has access
to the external resources, such as the V-pipe file system (mounted via
SSHfs) and the database: either the production database or possibly a
local test instance, depending on how the environment variables are set.
They are forwarded from the host to the dev container, as configured in
`.devcontainer.json`. Inside the dev container, a debugger for R is
pre-installed, also via a VS Code extension.

[VS Code]: https://code.visualstudio.com
[Dev Containers]: https://aka.ms/vscode-remote/containers
