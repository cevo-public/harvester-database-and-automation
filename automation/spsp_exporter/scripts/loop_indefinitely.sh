# Entry point for the Docker service running in production.

# Fail on any error so that Docker Compose would restart the container.
set -o errexit -o nounset -o pipefail -o errtrace

# Get absolute path of folder that this script here resides in.
here=$(dirname $(realpath ${BASH_SOURCE:-$0}))

# Loop indefinitely until the service is forcefully stopped.
while true
do
    # Create configuration files from environment variables.
    $here/create_configs.sh

    # Mount remote V-pipe file system.
    $here/vpipe_mount.sh

    # Run the actual entry point.
    java -Xmx12g -jar vineyard-1.0-SNAPSHOT.jar \
        SpspExporter vpipe/sampleset/ spsp/ vpipe/working/

    # Unmount remote V-pipe file system.
    $here/vpipe_unmount.sh

    # Wait until next run.
    sleep $RUN_INTERVAL
done
