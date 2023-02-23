# Automation

## Singularity

To run the Docker images with Singularity, they have to be converted into `.sif` files:

```
singularity build <image_name>.sif docker://ghcr.io/cevo-public/harvester:<image_name>
```
