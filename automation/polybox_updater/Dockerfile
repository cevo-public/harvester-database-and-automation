FROM debian:buster

RUN apt-get update
RUN apt-get install -y owncloud-client-cmd

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

RUN mkdir -p /etc/owncloud-client
RUN touch /etc/owncloud-client/sync-exclude.lst
RUN mkdir -p /polybox

VOLUME ["/polybox"]
ENTRYPOINT ["bash", "/entrypoint.sh"]
