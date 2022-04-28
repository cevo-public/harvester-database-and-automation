FROM registry.ethz.ch/sars_cov_2/harvester-database-and-automation:java
WORKDIR /app/

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["bash", "entrypoint.sh"]
