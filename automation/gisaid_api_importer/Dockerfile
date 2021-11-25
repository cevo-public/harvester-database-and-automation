## Build app ##

FROM openjdk:15 AS builder
WORKDIR /build/

COPY database/java/ .
RUN chmod +x ./gradlew
RUN ./gradlew clean build -x test


## Run server ##

FROM openjdk:17-slim-buster AS server

# Update packages
RUN apt-get update && apt-get -y upgrade

# Install nextclade
RUN apt-get install -y curl
RUN curl -sL https://deb.nodesource.com/setup_lts.x | bash -
RUN apt-get install -y nodejs
RUN npm install --global @neherlab/nextclade

# Install mafft
RUN apt-get install -y wget
RUN wget https://mafft.cbrc.jp/alignment/software/mafft_7.475-1_amd64.deb
RUN dpkg -i mafft_7.475-1_amd64.deb
RUN rm mafft_7.475-1_amd64.deb

COPY --from=builder /build/build/libs /app

# Download geo location corrections list from Nextstrain
RUN wget -O /app/gisaid_geoLocationRules.tsv https://raw.githubusercontent.com/nextstrain/ncov-ingest/master/source-data/gisaid_geoLocationRules.tsv

ENTRYPOINT ["java", "-Xmx12g", "-jar", "/app/vineyard-1.0-SNAPSHOT.jar", "--config", "/config/config.yml", "GisaidApiImporter"]
