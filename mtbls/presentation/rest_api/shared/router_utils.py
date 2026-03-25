import importlib
import os
import pkgutil
from logging import getLogger

from fastapi import APIRouter, FastAPI

import mtbls

logger = getLogger(__name__)


def find_routers(path: str):
    modules: list[pkgutil.ModuleInfo] = []
    data = list(pkgutil.iter_modules([path]))

    if data:
        for module in data:
            if module:
                if module.ispkg:
                    parts = [
                        x
                        for x in module.module_finder.path.split(path, 1)
                        if x and x.strip()
                    ]
                    current_path = path
                    if len(parts) > 1:
                        current_path = path + os.sep + parts[1].strip(os.sep)
                    modules.extend(find_routers(f"{current_path}/{module.name}"))
                else:
                    modules.append(module)
    return modules


def add_routers(application: FastAPI, root_path: str):
    modules = find_routers(root_path)
    for m in modules:
        path = str(mtbls.application_root_path)
        relative = m.module_finder.path.replace(path, "").lstrip(os.sep)
        module_name = f"{relative.replace(os.sep, '.')}.{m.name}"
        try:
            module = importlib.import_module(module_name)
            if hasattr(module, "router"):
                logger.info("Module loaded: %s", module_name)
                router_field = getattr(module, "router")
                if isinstance(router_field, APIRouter):
                    application.include_router(router_field)
        except Exception as ex:
            logger.error("Module '%s' is not found ", module_name)
            raise ex
