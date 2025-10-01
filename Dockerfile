ARG BASE_IMAGE=senzing/senzingapi-tools:3.10.3
FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2024-06-24

LABEL Name="senzing/g2search" \
  Maintainer="support@senzing.com" \
  Version="2.1.7"

HEALTHCHECK CMD ["/app/healthcheck.sh"]

# Run as "root" for system installation.

USER root

RUN apt-get update \
  && apt-get -y --no-install-recommends install \
  python3 \
  python3-pip \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Install packages via PIP.

COPY requirements.txt .
RUN pip3 install --upgrade pip \
  && pip3 install -r requirements.txt \
  && rm requirements.txt

# Install packages via apt.

# Copy files from repository.

COPY ./rootfs /
COPY ./G2Search.py /app/

# Make non-root container.

USER 1001

# Runtime execution.

WORKDIR /app
CMD ["/app/sleep-infinity.sh"]
