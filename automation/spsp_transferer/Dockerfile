## Build app ##

FROM openjdk:15-slim AS builder

COPY database/java/ /build/
RUN cd /build && chmod +x gradlew && ./gradlew clean build -x test && ls -l /build/*


## Run server ##

FROM registry.ethz.ch/sars_cov_2/harvester-database-and-automation:base_image_r_and_python AS server

# Copy compiled harvester java program into this container

COPY --from=builder /build/build/libs /app

# Update packages

## Install SPSP transfer tool prerequisites

# Install java
RUN apt-get update\
    && apt-get install -y wget\
    && wget https://download.java.net/openjdk/jdk15/ri/openjdk-15+36_linux-x64_bin.tar.gz \
    && tar xvf openjdk-15*_bin.tar.gz \
    && mkdir -p /usr/java \
    && mv jdk-15 /usr/java/jdk-15

ENV PATH="/usr/java/jdk-15/bin:${PATH}"
ENV JAVA_HOME="/usr/java/jdk-15/bin"

# Install necessary R packages
# 1st command is somehow necessary for rJava after update of the system Java installation
# Do this before copying the rest of database directory because it takes ages, want to use cached version if possible
RUN R CMD javareconf

# Install SPSP transfer tool
RUN git clone https://gitlab.sib.swiss/SPSP/transfer-tool.git /app/transfer-tool/

# Copy over database scripts
# Copy script to do transfer

COPY transfer.sh /app/transfer.sh
RUN chmod +x /app/transfer.sh

# Run script to generate and submit submission files
ENTRYPOINT ["java", "-Xmx12g", "-jar", "/app/vineyard-1.0-SNAPSHOT.jar", "SpspExporter", "/mnt/pangolin/sampleset", "/app/outdir"]
