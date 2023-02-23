# SPSP Exporter

Exports the sequencing raw data for fully processed samples from the
V-pipe results as well as corresponding meta data from the database.
Then prepares all files in the required format and folder structure for
submission to the Swiss Pathogen Surveillance Platform ([SPSP]). Finally
runs SPSP's Transfer Tool to upload everything to the upstream SFTP
server.

[SPSP]: https://spsp.ch


## Configuration

The following configuration files are used:
* `config.yml`: Contains database connection details.
                See example in the `database` folder.
* `harvester-config.yml`: Defines the email sender account and recipients.
                          See example in this folder here.
* `spsp-config.yml`: Specifies mandatory columns and author lists.
                     See example in the `database` folder.


## Implementation

The entry point is (effectively) `SpspExporter.java`. It:
* Parses `harvester-config.yml` in the working directory to get the
  email configuration.
* Delegates to `export_spsp_submission.R` to query the database for
  samples to release to SPSP, perform a number of quality and sanity
  checks, prepare meta data for the sequencing results (such as a list
  of authors, depending on the sequencing center), and write out the
  files to upload to SPSP.
* Sends the export report to the email recipients.
* Runs the SPSP [Transfer Tool] via the `transfer.sh` wrapper script.
* Delegates to `record_spsp_submission.R` to mark the samples that
  were submitted in the database.

[Transfer Tool]: https://gitlab.sib.swiss/clinbio/spsp-ng/transfer-tool
