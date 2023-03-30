FROM python:3.10.8
WORKDIR /app/

RUN apt-get update && apt-get -y upgrade

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY init.sql /app/
COPY pangolin_lineage_exporter.py /app/

ENTRYPOINT ["bash", "entrypoint.sh"]
