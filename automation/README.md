# Automation

## Singularity

To run the Docker images with Singularity, they have to be converted into `.sif` files:

```
singularity build <image_name>.sif docker://ghcr.io/cevo-public/harvester:<image_name>
```

## SPSP Exporter

This function is unique because it is not (yet) included in the Harvester automation, even though the code to do it lives here. It also requires a key pair that is not available in the repo.

This image is built locally and requires the local configuration files config.yml (an example lives in the database repo, for connecting to the Vineyard database), harvester-config.yml (documented here), and spsp-config.yml (lives in the database repo, specifies mandatory columns and author lists).
