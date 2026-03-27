ARG CONTAINER_REGISTRY_PREFIX=docker.io/

FROM ${CONTAINER_REGISTRY_PREFIX}astral/uv:0.9-python3.13-trixie-slim AS builder

LABEL maintainer="MetaboLights (metabolights-help @ ebi.ac.uk)"
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app-root
ARG GROUP1_ID=2222
ARG GROUP2_ID=2223
ARG USER_ID=2222
RUN groupadd group1 -g $GROUP1_ID \
    && groupadd group2 -g $GROUP2_ID \
    && useradd -ms /bin/bash -u $USER_ID -g group1 -G group1,group2 metabolights
USER metabolights
ENV PYTHONPATH=/app-root
EXPOSE 7077
COPY uv.lock uv.lock
COPY README.md README.md
RUN uv sync --locked
COPY . .
CMD ["uv", "run", "--no-project",  "/app-root/mtbls/run/rest_api/submission/main.py"]
