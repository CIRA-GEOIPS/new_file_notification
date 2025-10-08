FROM python:3.8 AS intermediate

LABEL org.opencontainers.image.authors="Jim Fluke <james.fluke@colostate.edu>"

ARG INV_API_TOKEN

ARG INV_API_VERSION=V1.8.6

RUN git config --global http.sslverify false && \
    pip install --no-cache-dir git+https://whatever:${INV_API_TOKEN}@bear.cira.colostate.edu/geoips/data_inv_api@${INV_API_VERSION}

RUN pip show xxhash

RUN find / -name dpc_inv_api

FROM python:3.8

RUN apt-get update && apt-get install -y sudo libnss-wrapper

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && apt-get upgrade -y && apt-get install -y python3-pip \
  && rm -rf /var/lib/apt/lists/*

ARG SITE_VS_DIST=site-packages
ARG PIP_PKG_DIR=//usr/local/lib/python3.8/${SITE_VS_DIST}/dpc_inv_api

COPY --from=intermediate ${PIP_PKG_DIR} ${PIP_PKG_DIR}

# Health check port
EXPOSE 5000

RUN pip install --no-cache-dir xxhash psycopg2 pika

ENTRYPOINT [ "python", "/app/new_file_notification/get_file_notif.py" ]
