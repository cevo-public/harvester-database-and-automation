FROM ghcr.io/cevo-public/harvester:java
WORKDIR /app/

# Update packages
RUN apt-get update && apt-get -y upgrade

# Install package for mounting a smb cifs drive
RUN apt-get install -y cifs-utils

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["bash", "entrypoint.sh"]
