FROM debian:buster-slim

# Update packages
RUN apt-get update  \
&& apt-get -y install \
    cifs-utils \
    python3 \
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
    owncloud-client-cmd \
    netcat \
    gpg \
    openssh-client \
    npm


COPY database/ pangolin/ transfer-tool/ /app/

RUN Rscript /app/R/install_packages.R
RUN pip3 install -r /app/python/requirements.txt

RUN apt-get install -y npm
