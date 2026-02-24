from copy import deepcopy
from logging import config as logging_config
from typing import Any

DEFAULT_CLI_LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": '%(levelname)-8s %(asctime)s %(name)s "%(message)s"',
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "default",
            "stream": "ext://sys.stdout",
        }
    },
    "root": {
        "level": "INFO",
        "handlers": ["console"],
    },
}


def configure_cli_logging(config: Any) -> None:
    if isinstance(config, dict) and config:
        logging_config.dictConfig(config)
        return
    logging_config.dictConfig(deepcopy(DEFAULT_CLI_LOGGING_CONFIG))
