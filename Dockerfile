FROM python:3.12

LABEL org.opencontainers.image.authors="Jim Fluke <james.fluke@colostate.edu>"

# Avoid interactive time zone questions
ARG DEBIAN_FRONTEND=noninteractive

# Don't keep a pip cache dir
ENV PIP_NO_CACHE_DIR=1

RUN apt-get update -y && apt-get upgrade -y && \
    apt-get install -y --no-install-recommends python3-pip && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir geoips xxhash psycopg2 pika

ARG INV_API_TOKEN

#ARG INV_API_VERSION=@1.8.6
ARG INV_API_VERSION

# Change this to force a rebuild from this point
RUN echo "re-install 0"

RUN git config --global http.sslverify false && \
    pip install --no-cache-dir git+https://whatever:${INV_API_TOKEN}@bear.cira.colostate.edu/geoips/data_inv_api@${INV_API_VERSION}

#RUN pip show xxhash

RUN pip install git+https://github.com/NRLMMD-GEOIPS/geoips_clavrx@biosafetylvl5-patch-1

ARG GITHUB_TOKEN
ARG GIT_UNAME
RUN pip install git+https://${GIT_UNAME}:${GITHUB_TOKEN}@github.com/NRLMMD-GEOIPS/overcast_package@overcast-package-overhaul

#RUN find / -name data_inv_api

# Health check port
EXPOSE 5000

RUN GEOIPS_OUTDIRS=/app/geoips_outdirs geoips config create-registries

ENTRYPOINT [ "python", "/app/new_file_notification/get_file_notif.py" ]
