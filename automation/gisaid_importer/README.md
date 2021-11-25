# GISAID Importer

This program (re-)imports the entire GISAID dataset. Different than the other images in this repository, this tool will be run one time and not minotor the input directory for new data.

Because the alignments and Nextclade analyses of the large dataset are very computing-intensive, it should be run with at least 64 cores (better 128) and about 64 GB memory.


## Usage

The image expects the Nextmeta and Nextfasta files to be provided in the `/data` directory:

```
/data
├── metadata.tsv
└── sequences.fasta
```

An example command that mounts the data directory and provides the necessary environment variables could be:

```
# Build the image
docker build -t gisaid_importer .

# Run
docker run -it --rm --volume="<absolute path to the data>:/data" --env-file <path to a file containing the environment variables> gisaid_importer
```


## Environment variables

The following environment variables are mandatory:

* `DB_HOST`
* `DB_USER`
* `DB_PASSWORD`
* `DB_DBNAME`


## Singularity

After converting the Docker image to a singularity container, run:

```
singularity run  --env-file <environment_variables.txt> --bind <data dir>:/data --bind <temporary workdir>:/app_wkdir gisaid_importer.sif
```

Because a Singularity container is per default read-only, the working directory `/app_wkdir` has to be mounted.
