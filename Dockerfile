ARG CONTAINER_REGISTRY_PREFIX=docker.io/
FROM ${CONTAINER_REGISTRY_PREFIX}astral/uv:python3.13-trixie AS builder

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
ENV PYTHONPATH=/app-root
ENV PATH=/app-root/.venv/bin:$PATH
ENV UV_LOCKED=1
EXPOSE 7077
COPY README.md README.md
COPY pyproject.toml pyproject.toml
COPY uv.lock uv.lock
RUN uv sync
COPY . .
USER metabolights
CMD ["python",  "/app-root/mtbls/run/rest_api/submission/main.py"]
