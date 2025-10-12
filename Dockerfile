ARG CONTAINER_REGISTRY_PREFIX=docker.io/
ARG UV_CONTAINER_IMAGE=ghcr.io/astral-sh/uv:0.9.1-python3.13-bookworm

FROM ${CONTAINER_REGISTRY_PREFIX}python:3.13-trixie AS builder

LABEL maintainer="MetaboLights (metabolights-help @ ebi.ac.uk)"


COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/


WORKDIR /app-root
COPY . .
RUN uv sync --locked --extra ws3

ARG GROUP1_ID=2222
ARG GROUP2_ID=2223
ARG USER_ID=2222
RUN groupadd group1 -g $GROUP1_ID \
    && groupadd group2 -g $GROUP2_ID \
    && useradd -ms /bin/bash -u $USER_ID -g group1 -G group1,group2 metabolights
USER metabolights
ENV PYTHONPATH=/app-root
EXPOSE 7077
CMD ["uv", "run", "--no-project",  "/app-root/mtbls/run/rest_api/submission/main.py"]
