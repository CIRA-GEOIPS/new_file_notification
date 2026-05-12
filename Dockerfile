FROM python:3.12 AS intermediate

LABEL org.opencontainers.image.authors="Jim Fluke <james.fluke@colostate.edu>"

ARG INV_API_TOKEN

#ARG INV_API_VERSION=@1.8.6
ARG INV_API_VERSION=latest

RUN git config --global http.sslverify false && \
    pip install --no-cache-dir git+https://whatever:${INV_API_TOKEN}@bear.cira.colostate.edu/geoips/data_inv_api@${INV_API_VERSION}

RUN pip show xxhash

RUN pip install git+https://github.com/NRLMMD-GEOIPS/geoips_clavrx@biosafetylvl5-patch-1

RUN find / -name data_inv_api

FROM python:3.12

RUN apt-get update && apt-get install -y sudo libnss-wrapper

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && apt-get upgrade -y && apt-get install -y python3-pip \
  && rm -rf /var/lib/apt/lists/*

ARG SITE_VS_DIST=site-packages
ARG PIP_PKG_DIR=//usr/local/lib/python3.12/${SITE_VS_DIST}
ARG DIA_PKG_DIR=${PIP_PKG_DIR}/data_inv_api

COPY --from=intermediate ${DIA_PKG_DIR} ${DIA_PKG_DIR}

# Health check port
EXPOSE 5000

RUN pip install --no-cache-dir geoips xxhash psycopg2 pika

COPY --from=intermediate ${PIP_PKG_DIR}/geoips_clavrx/plugins/modules/readers/clavrx_hdf4.py ${PIP_PKG_DIR}/geoips/plugins/modules/readers/

RUN GEOIPS_OUTDIRS=/app/geoips_outdirs geoips config create-registries

ENTRYPOINT [ "python", "/app/new_file_notification/get_file_notif.py" ]
