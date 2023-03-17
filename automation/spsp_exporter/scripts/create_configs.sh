# Creates the various configuration files from the environment variables.
#
# This is done so that the Docker service can be configured in a central
# location, like `docker-compose.yaml`. The code base was not originally
# developed with that deployment scenario in mind.

cat >/app/config.yml <<END
default:
  database:
    server:
      host: $DATABASE_HOST
      port: $DATABASE_PORT
      username: $DATABASE_USER
      dbname: $DATABASE_NAME
      password: $DATABASE_PASSWORD
END

cat >/app/harvester-config.yml <<END
senderSmtpHost: $EMAIL_HOST
senderSmtpPort: $EMAIL_PORT
senderSmtpUsername: $EMAIL_USER
senderSmtpPassword: $EMAIL_PASSWORD
senderAddress: $EMAIL_SENDER
recipients:
END
for RECIPIENT in ${EMAIL_RECIPIENTS//,/ }
do
	echo "  - $RECIPIENT" >>/app/harvester-config.yml
done

cat >/app/raw-data-upload.yml <<END
server: $VPIPE_HOST
user: $VPIPE_USER
uploads_folder: $VPIPE_ROOT/working/uploads
private_key_euler: /app/identities/$RAWDATA_IDENTITY
passphrase: $RAWDATA_PASSPHRASE
max_conn: 10
max_samples_per_call: 200
END

# Make sure SSH identity files (private keys) have correct access mode.
chmod u=rw,go= /app/identities/$VPIPE_IDENTITY
chmod u=rw,go= /app/identities/$RAWDATA_IDENTITY
