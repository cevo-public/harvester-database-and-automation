# Swiss SARS-CoV-2 Sequencing Consortium (S3C)

The code in this repository underlies a data infrastructure for SARS-CoV-2 genomic surveillance. 
It is used to generate and publish SARS-CoV-2 genome sequences from the Swiss SARS-CoV-2 Sequencing Consortium (S3C).
For more information on the S3C, see our consortium [website](https://bsse.ethz.ch/cevo/research/sars-cov-2/swiss-sars-cov-2-sequencing-consortium.html).

## Project structure
The basic project workflow is to collect SARS-CoV-2 test metadata and whole-genome sequences, store the data in a PostreSQL database, annotate the data with quality control and other information, and generate submission files for upload to public sequence databases.

This repository has two high-level directories, `database` and `automation`.  
* `database` contains the code used to import data into and export data from our "Harvester" database.  
* `automation` contains the code used to automate some data management and basic analysis tasks. We call these "microservices".

The microservices defined in `automation` are build on top of the `database` code.

## Usage
The infrastructure built by this code is hosted at ETH Zurich and the code is intended to be run on ETH servers.
Thus, the code will not run out-of-the-box for external users.  

GitHub actions automatically builds images for the code on the `main`-branch and pushes them to [GitHub packages](https://github.com/cevo-public/harvester-database-and-automation/pkgs/container/harvester). To build the images manually, the script [automation/build_and_push_all_images.sh](./automation/build_and_push_all_images.sh) can be used. Images are pulled from ETH servers, where they are continuously running.

Database access is governed by user-specific configuration files.

[comment]: <> (Each user maintains their own configuration file for database access.)

## Requirements
* A database. 
  * Our PostgreSQL "Harvester" database is set up as described in [database/init.sql](./database/init.sql). 
  * The respective tables and columns are (partially) documented in [database/column_documentation.tsv](./database/column_documentation.tsv).
* Raw data.
  * Whole-genome consensus sequences and associated sample metadata. These are project-specific and not included in this repository.
  * External data, for example case counts and geographic mappings. Sources we use are mostly linked to from the respective import scripts.
* A containerization software. We build our automation code into Docker containers containing all the necessary requirements, one per microservice.
  * R and python package requirements are given in [database/R/install_packages.R](./database/R/install_packages.R) and [database/python/requirements.txt](./database/python/requirements.txt).
  * The java application requirements are defined in [database/java/build.gradle](./database/java/build.gradle).
* Secure server(s) for hosting the database, running containers, and storing database back-ups.
