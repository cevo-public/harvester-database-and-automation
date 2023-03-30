# Pangolin lineage exporter

Required environment variables

```
DB_HOST
DB_DBNAME
DB_USER
DB_PASSWORD
WEBDAV_BASE_URL
WEBDAV_PASSWORD
WEBDAV_USER
WEBDAV_EXPORT_DIRECTORY
```

Optional environment variables

```
SLEEP_IN_SECONDS
SLEEP_IN_SECONDS_AFTER_FAILURE
EXPORT_IMMEDIATELY_AFTER_X_DAYS
MAX_LINES_PER_FILE
```

Build and run the Docker image for testing

```
export DOCKER_DEFAULT_PLATFORM=linux/amd64
echo "Build image"
docker build .
image_id=$(docker build -q .)
echo "Start container"
docker run -it --init --env-file .env --link database-container $image_id
```
