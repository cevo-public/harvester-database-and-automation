# Swiss SARS-CoV-2 Sequencing Consortium (S3C)

The code in this repository underlies a data infrastructure for SARS-CoV-2 genomic surveillance.
It is used to generate and publish SARS-CoV-2 genome sequences from the Swiss SARS-CoV-2 Sequencing Consortium (S3C).
For more information on the S3C, see our consortium [web site](https://bsse.ethz.ch/cevo/research/sars-cov-2/swiss-sars-cov-2-sequencing-consortium.html).


## Project structure

The basic project workflow is to collect SARS-CoV-2 test metadata and whole-genome sequences, store the data in a PostreSQL database, annotate the data with quality control and other information, and generate submission files for upload to public sequence databases.

This repository has two high-level directories, `database` and `automation`.
* `database` contains the code used to import data into and export data from our "Vineyard" database.
* `automation` contains the code used to automate some data management and basic analysis tasks. We call these "microservices".

The microservices defined in `automation` are built on top of the `database` code.
They are deployed as Docker services on a dedicated server hosted at ETH Zurich.
The code will not run out-of-the-box for external users as certain infracstructure components, such as network shares, have to be configured accordingly.


## Requirements

* A database.
  * Our PostgreSQL "Vineyard" database is set up as described in [`database/init.sql`](./database/init.sql).
  * The respective tables and columns are documented in [`database/column_documentation.tsv`](./database/column_documentation.tsv).
* Raw data.
  * Whole-genome consensus sequences and associated sample metadata.
    These are project-specific and not included in this repository.
  * External data, for example case counts and geographic mappings.
    Sources we use are mostly linked to from the respective import scripts.
* A containerization software. We build our automation code into Docker containers including all external dependencies, one per microservice.
* Secure server for hosting the database, running containers, and storing database back-ups.
