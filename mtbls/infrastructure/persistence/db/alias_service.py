import inspect
import logging
import pkgutil
import re
import sys
from functools import partial
from typing import Any, Callable, OrderedDict

from pydantic import BaseModel

import mtbls
from mtbls.domain.entities.base_entity import BaseEntity
from mtbls.domain.enums.entity import Entity
from mtbls.infrastructure.persistence.db.alias_generator import AliasGenerator

logger = logging.getLogger(__name__)


class AliasService:
    def __init__(self, alias_generator: AliasGenerator):
        self.alias_generator = alias_generator
        self.root_packages = [mtbls.__name__]

    @staticmethod
    def get_package_name(package: str) -> str:
        app_root = str(mtbls.application_root_path)
        return package.replace(app_root, "", 1).strip("/").replace("/", ".")

    def update_validation_alias(self):
        if not self.alias_generator:
            logger.warning("Alias generator is not found. Skipping ...")
        else:
            name = self.get_full_class_name(self.alias_generator)
            logger.info("Alias generator '%s' will be used.", name)

        all_loaded_modules = {
            m: sys.modules[m]
            for m in sys.modules
            if re.match(rf"{mtbls.__name__}(\..+)?", m)
        }
        target_modules = {}
        for package in self.root_packages:
            target_modules.update(
                {
                    m: all_loaded_modules[m]
                    for m in all_loaded_modules
                    if re.match(rf"{package}(\..+)?", m)
                }
            )

        loaded_entity_classes: OrderedDict[str, type[BaseModel]] = OrderedDict()
        for module in target_modules.values():
            self.find_classes_in_module(BaseEntity, module, loaded_entity_classes)
        logger.debug("Setting alias generators for the entities")
        for name, loaded_entity_class in loaded_entity_classes.items():
            if hasattr(loaded_entity_class, "__entity_type__"):
                entity_type = loaded_entity_class.__entity_type__
                if isinstance(entity_type, Entity):
                    alias_generator = partial(
                        self.alias_generator.get_alias, entity_type
                    )
                    self.set_model_validation_alias(
                        loaded_entity_class, alias_generator
                    )

    @staticmethod
    def get_full_class_name(obj):
        cls = type(obj)
        module = cls.__module__
        name = cls.__qualname__
        if module is not None and module != "__builtin__":
            name = module + "." + name
        return name

    @classmethod
    def set_model_validation_alias(
        cls, domain_class: BaseModel, alias_generator: Callable
    ) -> None:
        for field_name in domain_class.model_fields:
            if field_name in domain_class.model_fields:
                field_info = domain_class.model_fields[field_name]
                alias = alias_generator(field_name)
                if alias != field_name:
                    field_info.validation_alias = alias

        domain_class.model_rebuild(force=True)

    @classmethod
    def find_modules(cls, path: str) -> list[pkgutil.ModuleInfo]:
        modules = []
        data = list(pkgutil.walk_packages([path]))

        if data:
            for module in data:
                if module and module.__module__.startswith(mtbls.__name__):
                    if module.ispkg:
                        modules.extend(cls.find_modules(f"{path}/{module.name}"))
                    else:
                        modules.append(module)
        return modules

    def find_classes_in_module(
        self,
        base_class: type[BaseModel],
        module: Any,
        loaded_classes: dict[str, type[BaseModel]],
    ) -> None:
        for name in dir(module):
            obj = getattr(module, name)
            is_class = inspect.isclass(obj)
            is_subclass = issubclass(obj, base_class) if is_class else False

            if not is_class or (is_class and not is_subclass):
                continue

            full_name = f"{module.__name__}.{obj.__name__}"
            loaded_classes[full_name] = obj
