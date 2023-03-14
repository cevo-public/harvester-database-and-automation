# This Docker file is meant to be build in the context of this very
# folder, with `docker build --tag spsp-exporter .`, but requires that
# the `database` folder has been copied here beforehand.


# Compile Java components in separate Docker container.
# This is intended to make the final image smaller, by keeping out the
# Java build system.

FROM openjdk:15 AS builder
WORKDIR /build

COPY database/java/ .
RUN chmod +x ./gradlew
RUN ./gradlew clean build -x test


# Build actual Docker image.

FROM rocker/tidyverse:4.2
WORKDIR /app
RUN apt-get update

# Install Java run-time environment, see: https://jdk.java.net/java-se-ri/15.
RUN mkdir -p /usr/java
ARG JRE_OPENJDK=https://download.java.net/openjdk
ARG JRE_ARCHIVE=jdk15/ri/openjdk-15+36_linux-x64_bin.tar.gz
RUN wget $JRE_OPENJDK/$JRE_ARCHIVE --output-document=- | \
    tar --extract --gunzip --directory /usr/java
ENV JAVA_HOME=/usr/java/jdk-15/bin
ENV PATH=$JAVA_HOME:$PATH

# Detect current Java setup and update the corresponding configuration in R.
RUN /usr/local/bin/R CMD javareconf

# Install required R packages in addition to those in the Tidyverse collection.
COPY dependencies.R .
RUN /usr/local/bin/Rscript dependencies.R

# Install package for mounting an SMB CIFS drive.
RUN apt-get install -y cifs-utils

# Install package for mounting remote file system via SSH.
RUN apt-get install -y sshfs

# Install SPSP Transfer Tool and its prerequisites.
RUN apt-get install -y netcat
RUN apt-get install -y gpg
RUN apt-get install -y openssh-client
RUN apt-get install -y curl
RUN git clone https://gitlab.sib.swiss/clinbio/spsp-ng/transfer-tool.git

# Copy over pre-built Java jar file.
COPY --from=builder /build/build/libs .

# Copy R scripts from source.
COPY database/R/ R/
COPY database/data/ data/
COPY database/spsp-config.yml .

# Copy utility scripts.
COPY scripts/ scripts/
RUN chmod +x scripts/*.sh
COPY ReadMe.md .

# Entry point when running in production.
ENTRYPOINT ["/bin/bash", "scripts/loop_indefinitely.sh"]
