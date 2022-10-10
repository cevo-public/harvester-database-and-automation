## Build app ##

FROM openjdk:15 AS builder
WORKDIR /build/

COPY database/java/ .
RUN chmod +x ./gradlew
RUN ./gradlew clean build -x test


## Run server ##

FROM rocker/tidyverse:4.0.3 AS server
WORKDIR /app/
# Copy compiled harvester java program into this container
COPY --from=builder /build/build/libs /app

# Update packages
RUN apt-get update && apt-get -y upgrade

## Install SPSP transfer tool prerequisites
RUN apt-get install -y netcat #nc command
RUN apt-get install -y gpg #gpg command
RUN apt-get install -y openssh-client #sftp command
RUN apt-get update && apt-get install -y curl  # for testing IP address

# Install package for mounting a smb cifs drive
RUN apt-get install -y cifs-utils

# Install java
RUN wget https://download.java.net/openjdk/jdk15/ri/openjdk-15+36_linux-x64_bin.tar.gz
RUN tar xvf openjdk-15*_bin.tar.gz
RUN mkdir -p /usr/java
RUN mv jdk-15 /usr/java/jdk-15
ENV PATH="/usr/java/jdk-15/bin:${PATH}"
ENV JAVA_HOME="/usr/java/jdk-15/bin"

# Install necessary R packages
# 1st command is somehow necessary for rJava after update of the system Java installation
# Do this before copying the rest of database directory because it takes ages, want to use cached version if possible
RUN /usr/local/bin/R CMD javareconf
RUN mkdir /app/R
COPY database/R/install_packages.R /app/R
RUN /usr/local/bin/Rscript /app/R/install_packages.R

# Install SPSP transfer tool
RUN git clone https://gitlab.sib.swiss/SPSP/transfer-tool.git transfer-tool/

# Copy over database scripts
COPY database/ /app

# Copy script to do transfer
COPY transfer.sh /app/transfer.sh
RUN chmod +x /app/transfer.sh

# Run script to generate and submit submission files
ENTRYPOINT ["java", "-Xmx12g", "-jar", "/app/vineyard-1.0-SNAPSHOT.jar", "SpspExporter", "/mnt/pangolin/sampleset", "/app/outdir", "/mnt/pangolin/working"]
