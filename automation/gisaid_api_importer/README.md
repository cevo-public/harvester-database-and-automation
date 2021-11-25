# GISAID Api Importer

This program needs so many configurations that a configuration file seems better than environment variables. Rather than setting environment variables, this program expects the `config.yml` be at `/config/config.yml`. An example config file is in `config.example.yml`.


## Usage

### Singularity + Euler

Transform to a Singularity image:

```
singularity build --docker-login gisaid_api_importer.sif docker://registry.ethz.ch/sars_cov_2/automation:gisaid_api_importer
```

We need to define a work directory. Because files in a Singularity container is by default read-only, we need to create the work directory outside and mount it into the container:

```
mkdir $SCRATCH/gisaid_api_wkdir
```

We will map the following files and directories into the container:

- /scratch->/scratch: This can be used as a temporary directory and is generally recommended to be mounted when using Singularity on Euler.
- $SCRATCH/gisaid_api_wkdir:/workdir: The work directory for the program
- $HOME/mail_dropoff:/mail_dropoff: The dropoff notification system seems to be the only notification system that works on Euler. This defines the location where the mails shall be dropped off.
- $HOME/gisaid_api_importer/config.yml:/config/config.yml: The configuration file

Resource usage: I use 2 workers and 1 GB RAM per CPU with a batch size of 100 but this can certainly be further optimized.

Start an Euler job:

```
bsub -N -n 64 -R "rusage[mem=1600]" -W 20:00 -B "singularity run --bind /scratch:/scratch --bind $SCRATCH/gisaid_api_wkdir:/workdir --bind $HOME/mail_dropoff:/mail_dropoff --bind $HOME/gisaid_api_importer/config.yml:/config/config.yml gisaid_api_importer.sif"
```
