set -euo pipefail

docker build --network host -t spsp-uploader .
singularity build -F spsp-uploader.sif docker-daemon://spsp-uploader:latest
