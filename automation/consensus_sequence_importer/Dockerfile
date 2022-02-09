FROM rocker/tidyverse:4.0.3
WORKDIR /app/

# Update packages
RUN apt-get update && apt-get -y upgrade

# Install R packages
COPY database/R/install_packages.R R/
RUN Rscript R/install_packages.R

# Install package for mounting a smb cifs drive
RUN apt-get install -y cifs-utils

# Prepare python
RUN apt-get install -y python3 python3-pip
COPY requirements.txt python/
RUN pip3 install -r python/requirements.txt

# Clone nextstrain/ncov repository
RUN git clone https://github.com/nextstrain/ncov.git python/ncov/
RUN cd python/ncov && git checkout 76d9d30dc196ecbbb24650605573ad2eef486081 && cd ../..

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

COPY database/ .

ENTRYPOINT ["bash", "entrypoint.sh"]
