FROM debian:buster

COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh \
    && mkdir -p /etc/owncloud-client \
    && touch /etc/owncloud-client/sync-exclude.lst \
    && mkdir -p /polybox

VOLUME ["/polybox"]
ENTRYPOINT ["bash", "/entrypoint.sh"]
