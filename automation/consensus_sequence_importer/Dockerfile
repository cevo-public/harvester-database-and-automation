FROM registry.ethz.ch/sars_cov_2/s3c/harvester-database-and-automation:base_image_r_and_python

# clone nextstrain/ncov repository
RUN git clone https://github.com/nextstrain/ncov.git /app/python/ncov/
RUN cd /app/python/ncov && git checkout 76d9d30dc196ecbbb24650605573ad2eef486081

COPY entrypoint.sh /app
RUN chmod +x /app/entrypoint.sh


ENTRYPOINT ["bash", "/app/entrypoint.sh"]
