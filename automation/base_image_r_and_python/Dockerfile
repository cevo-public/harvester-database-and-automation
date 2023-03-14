FROM debian:bullseye-slim

RUN \
    apt-get update \
    && apt-get install -y --no-install-recommends wget gnupg ca-certificates \
    && wget -nv --no-check-certificate https://download.owncloud.com/desktop/ownCloud/stable/latest/linux/Debian_11/Release.key -O - | apt-key add - \
    && echo 'deb https://download.owncloud.com/desktop/ownCloud/stable/latest/linux/Debian_11/ /' | tee -a /etc/apt/sources.list.d/owncloud.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends  owncloud-client \
    && apt-get -y --no-install-recommends install \
        cifs-utils \
        python3 \
        python3-dev \
        python3-pip \
        r-base \
        r-base-dev \
        libcurl4-openssl-dev \
        libxml2-dev \
        libssl-dev \
        libpq-dev \
        libudunits2-dev \
        libgdal-dev \
        git \
        curl \
        wget \
        nodejs \
        netcat \
        gpg \
        openssh-client \
        npm \
    && rm -rf /var/lib/apt/lists/*

COPY database/ pangolin/ /app/

RUN Rscript /app/R/install_packages.R \
    && pip3 install -r /app/python/requirements.txt
