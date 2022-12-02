# Harvester Services

### BAG Meldeformular Dashboard Importer

This BAG meldeformular dashboard importer imports the data shared with us by the BAG through the Polybox folder `Shared/BAG COVID19 Data` on a daily basis. The "dashboard" in the name is because the data were initially mainly used to fuel the timeseries (and Re) dashboard. Because we are not allowed to release the data on weekends, holidays, and in the morning, the importer only runs on particular days and time slots.

**Image name:** ghcr.io/cevo-public/harvester:bag_meldeformular_dashboard_importer

**Dependencies:** The program does not access Polybox directly but needs an external synchronization program (e.g., the polybox updater image).


### BAG Meldeformular Importer

The BAG meldeformular importer is supposed to import the data shared with us by the BAG through the Polybox folder `Shared/BAGsequenceMetadata`. However, the meldeformular import function is not very memory-efficient and needs more memory than the server can offer. The BAG meldeformular data must be manually imported for now.

Because Chaoran was too lazy to create another image, the BAG meldeformular importer has another task: it exports the sequence report to `Shared/BAGsequenceMetadata/sequence_report`.

**Image name:** ghcr.io/cevo-public/harvester:bag_meldeformular_importer

**Dependencies:** The program does not access Polybox directly but needs an external synchronization program (e.g., the polybox updater image).

**Gotchas:** Not all the outputs of the programs are redirected to stdout/stderr so that some log and error messages might not appear in the log files.


### Sequence Diagnostic Importer (automated part)

Imports QC scores.

**Image name:** ghcr.io/cevo-public/harvester:sequence_diagnostic_importer

**Dependencies:** It needs access to the `covid19-pangolin` drive.


### GISAID API Importer

The GISAID API importer runs on Euler once a day. It downloads the whole GISAID dataset from the API, identifies the changes (new/updated/deleted entries), performs pre-processing steps (alignment, mutation calling, etc.), and writes the changes to the database. It will send an email when it finishes (with a summary of the number of changes) and when it crashes (including the error message).

**Image name:** ghcr.io/cevo-public/harvester:gisaid_api_importer

**Dependencies:** The import program is written in Java and calls mafft and Nextclade which are included in the image. The image also includes the geo location rules from Nextstrain's repository which are used to clean up the location names. It needs internet access. It needs the GISAID API credentials.


### Nextclade Importer

The Nextclade importer loads sequences from the `consensus_sequence` table and runs Nextclade on them to obtain amino acid mutations and quality scores.

**Image name:** ghcr.io/cevo-public/harvester:nextclade_importer

**Dependencies:** The image includes Nextclade.

**Gotchas:**

The current implementation looks for sequences for which no amino acid mutation are stored in the database. However, there are (low-quality) sequences for which Nextclade is not able to obtain mutations. To avoid Nextclade processing them over and over again, the program stores the names of the samples that failed in memory and excludes them from processing. This is problematic when an old bad sequence gets updated with a good one. Then, it is necessary to manually restart the program.  To improve, the program should load sequences that do not have an entry in the `consensus_sequence_nextclade_data` table.


### OWID Global Cases Importer

The OWID global cases importer downloads the data and imports them into the database.

**Image name:** ghcr.io/cevo-public/harvester:owid_global_cases_importer

**Dependencies:** Internet


### Pangolin Lineage Alias Importer

The pangolin lineage alias importer downloads the alias file from the pango designation repository and imports changes into the database.

**Image name:** ghcr.io/cevo-public/harvester:java using the sub program "PangolinLineageAliasImporter"

**Dependencies:** Internet


### Pangolin Lineage Importer

The pangolin lineage importer loads sequences from the `consensus_sequence` table and determines the pangolin lineage using the official pangolin lineage classification tool. It checks the `consensus_sequence_nextclade_data` table and load the entries where `pangolin_status` column is null.

To use the newest version, the image should be rebuilt and redeployed regularly. To re-analyze existing sequences, execute the following SQL query:

```sql
update consensus_sequence_nextclade_data
set pangolin_status = null;
```

**Image name:** ghcr.io/cevo-public/harvester:pangolin_lineage_importer

**Dependencies:** The image includes the pangolin lineage classification tool.


### Polybox Updater

The Polybox updater has a supporting role. It synchronizes local folders with folders on Polybox.

**Image name:** ghcr.io/cevo-public/harvester:polybox_updater


### Viollier Metadata Importer

The Viollier metadata importer checks regularly for new sample metadata files from Viollier, imports them into the database and notifies the sequencing laboratories. It looks for new metadata files on the shared pangolin drive in `backup/sftp-viollier/sample_metadata`. The metadata files have to be CSVs and follow the following format:

- It is semicolon-separated.
- The encoding is Windows-1252.
- The first row contains the header.
- It has the following columns:
  - "Prescriber city"
  - "Zip code"
  - "Prescriber canton"
  - "Sequencing center": allowed values (case-insensitive): "viollier", "gfb", "fgcz", "health2030", "h2030"
  - "Sample number"
  - "Order date"
  - "PlateID"
  - "CT Wert"
  - "DeepWellLocation"

Like all the other services, this program is running in a Docker container and will be automatically restarted if it crashes. To make sure that a file is only processed once and to prevent that a file that leads to a crash will be read repeatedly, the program persists its state in the database. The state is in the database table `automation_state` in the row where the program name is `viollier_metadata_receiver`. It is written in the JSON format and has the following structure:

```json
{
  "processedFiles": [
    "<filename1>.csv",
    "..."
  ],
  "filesInProcessing": [
    "..."
  ]
}
```

The program works (roughly) as follows: Every 10 minutes:

1. Load the automation state from the database
2. Loads the list of files ending with ".csv" in `backup/sftp-viollier/sample_metadata`
3. If there are no new files, i.e. no files in the directory that are not already in either `processedFiles` or `filesInProcessing` in the automation state, leave.
4. Read all new csv files and write the file names into the automation state's `filesInProcessing` list.
5. Perform a few basic pre-processing and validations
6. Plates that are already in the database will be entirely ignored. The reason is that for unknown reasons, we often get the same file multiple times. This means that the program will only import a plate once. The program cannot perform any corrections or add more samples to a plate afterwards. The program will also not warn us if there are multiple files with conflicting information.
7. Write the new data into the database tables `viollier_test`, `viollier_plate` and `viollier_test__viollier_plate`
8. Send an email to all sequencing labs for which new metadata are available. The program will attach the metadata list in a transformed format.
9. Edit the automation state and move the file names from `filesInProcessing` to `processedFiles`

If the program crashes, it will try to send a notification email. If the program crashs between step 4 and 9, the file that caused the crash will be in `filesInProcessing`. Many crashes are due to an unexpected file format or values. In such a case, the resolution could be as follows:

1. Look into `filesInProcessing` to identify the problematic file
2. Open the problematic file in the editor and fix it
3. Remove the file from `filesInProcessing`

**Image name:** ghcr.io/cevo-public/harvester:viollier_metadata_receiver

**Gotchas:**

- Viollier uploads the files to a SFTP server. This program is, however, not directly reading from that server but from `backup/sftp-viollier/sample_metadata`.
