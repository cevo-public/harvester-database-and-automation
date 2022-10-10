## Build app ##

FROM openjdk:15-slim AS builder

COPY database/java/ /build/
RUN cd /build && chmod +x gradlew && ./gradlew clean build -x test && ls -l /build/*


## Run server ##

FROM openjdk:17-slim-buster AS server

# Update packages
RUN apt-get update && apt-get install -y npm curl wget

# Install nextclade
RUN curl -sL https://deb.nodesource.com/setup_lts.x | bash -
RUN npm install --global @neherlab/nextclade

# Install mafft
RUN wget https://mafft.cbrc.jp/alignment/software/mafft_7.475-1_amd64.deb
RUN dpkg -i mafft_7.475-1_amd64.deb
RUN rm mafft_7.475-1_amd64.deb

COPY --from=builder /build/build/libs /app

# Download geo location corrections list from Nextstrain
RUN wget -O /app/gisaid_geoLocationRules.tsv https://raw.githubusercontent.com/nextstrain/ncov-ingest/master/source-data/gisaid_geoLocationRules.tsv

ENTRYPOINT ["java", "-Xmx12g", "-jar", "/app/vineyard-1.0-SNAPSHOT.jar", "--config", "/config/config.yml", "GisaidApiImporter"]
