$ Viollier Metadata Receiver

Required environment variable

```
NAS_PATH
POLYBOX_PATH
NETHZ_USERNAME
NETHZ_PASSWORD
DB_HOST
DB_NAME
DB_USER
DB_PASSWORD
SMTP_HOST
SMTP_PORT
SMTP_USER
SMTP_PASSWORD
EMAIL_ADDRESS
VIOLLIER_METADATA_PATH
POLYBOX_METADATA_PATH
WAIT_SECONDS
ADMIN_EMAILS
META_EMAILS
SEND_EMAILS
```

Build and run the Docker image for testing

```
export DOCKER_DEFAULT_PLATFORM=linux/amd64
mkdir -p database
cp -rp ../../database/* database/
echo "Build image"
docker build .
image_id=$(docker build -q .)
echo "Test the container with a development database container named 'sars-cov-2-database-dev'"
docker run -it --init --env-file .env --link sars-cov-2-database-dev $image_id
```
