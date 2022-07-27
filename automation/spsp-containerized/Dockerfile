FROM ubuntu:20.04

ENV TOOLZIP=transfer-toolv1.9.0.zip
ENV SPSP_SERVER_IP=192.42.198.50
ENV LAB_CODE=LAB_0003

RUN apt update && apt install -y wget unzip netcat gpg openssh-client curl \
    && wget https://gitlab.sib.swiss/SPSP/transfer-tool/-/jobs/4454/artifacts/raw/builds/${TOOLZIP}\
    && unzip ${TOOLZIP} \
    && rm ${TOOLZIP}

COPY transfer.sh .pub /
