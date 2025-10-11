import logging
import os
from pathlib import Path
from typing import Any

import yaml
from dependency_injector import containers
from jinja2 import Template

logger = logging.getLogger(__name__)

CONFIG_FILE_ENVIRONMENT_VARIABLE_NAME = "MTBLS_WS_CONFIG_FILE"
DEFAULT_CONFIG_FILE_PATH = "mtbls-ws-config.yaml"
SECRET_FILE_ENVIRONMENT_VARIABLE_NAME = "MTBLS_WS_SECRET_FILE"
DEFAULT_SECRETS_FILE_PATH = ".mtbls-ws-config-secrets/.secrets.yaml"


def get_application_config_files() -> tuple[str, str]:
    config_file_path = os.environ.get(
        CONFIG_FILE_ENVIRONMENT_VARIABLE_NAME, DEFAULT_CONFIG_FILE_PATH
    )
    secrets_file_path = os.environ.get(
        SECRET_FILE_ENVIRONMENT_VARIABLE_NAME, DEFAULT_SECRETS_FILE_PATH
    )

    return config_file_path, secrets_file_path


def render_config_secrets(
    config: dict[str, Any], secrets: dict[str, Any]
) -> dict[str, Any]:
    if not secrets:
        logger.warning("Secrets dictionary is empty. Skiping config rendering")
        return config
    rendered_config = {}
    for key, value in config.items():
        template = Template(str(value))
        rendered_data = template.render(secrets)
        rendered_config[key] = eval(rendered_data)
    for key, value in rendered_config.items():
        config[key] = value
    return config


def set_application_configuration(
    container: containers.Container,
    config_file_path: None | str = None,
    secrets_file_path: None | str = None,
) -> bool:
    if not container:
        raise ValueError("Container is not defined")

    if not config_file_path:
        config_file_path = os.environ.get(
            CONFIG_FILE_ENVIRONMENT_VARIABLE_NAME, DEFAULT_CONFIG_FILE_PATH
        )

    if not secrets_file_path:
        secrets_file_path = os.environ.get(
            SECRET_FILE_ENVIRONMENT_VARIABLE_NAME,
            DEFAULT_SECRETS_FILE_PATH,
        )
    if Path(config_file_path).exists():
        with Path(secrets_file_path).open() as f:
            secrets_dict = yaml.safe_load(f)
            if not secrets_dict:
                logger.warning("Secret file %s content is empty", config_file_path)

            container.secrets.from_dict(secrets_dict or {})
    else:
        logger.warning("Secret file %s does not exist.", config_file_path)

    if Path(config_file_path).exists():
        with Path(config_file_path).open() as f:
            config_dict = yaml.safe_load(f)
            if not config_dict:
                logger.error("Config file %s content is empty", config_file_path)
                return False
            if secrets_dict:
                config_dict = render_config_secrets(config_dict, secrets_dict)
            container.config.from_dict(config_dict)
    else:
        logger.error("Config file %s does not exist", config_file_path)
        return False

    return True
