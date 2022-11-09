FROM python:3.10
WORKDIR /app/

# Update packages
RUN apt-get update && apt-get -y upgrade
# Install package for mounting a smb cifs drive
RUN apt-get install -y cifs-utils
RUN apt-get install -y libpq-dev
RUN apt-get -y install sudo
RUN yes | sudo DEBIAN_FRONTEND=noninteractive  apt-get install -yqq davfs2

COPY ./database/python/viollier/requirements.txt .
RUN pip3 install -r requirements.txt

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh
COPY ./database/python/viollier/ /app/.

ENTRYPOINT ["bash", "entrypoint.sh"]
