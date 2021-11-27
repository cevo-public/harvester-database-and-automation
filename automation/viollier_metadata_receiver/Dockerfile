FROM registry.ethz.ch/sars_cov_2/harvester-database-and-automation:java
WORKDIR /app/

# Update packages
RUN apt-get update && apt-get -y upgrade

# Install package for mounting a smb cifs drive
RUN apt-get install -y cifs-utils

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["bash", "entrypoint.sh"]
