from mtbls.domain.enums.entity import Entity
from mtbls.infrastructure.persistence.db.model.study_models import (
    Base,
    Study,
    StudyFile,
    User,
)


class EntityMapper:
    entity_table_map: dict[Entity, Base] = {
        Entity.User: User,
        Entity.Study: Study,
        Entity.StudyFile: StudyFile,
    }

    def get_table_model(self, entity_type: Entity) -> Base:
        return self.entity_table_map[entity_type]
