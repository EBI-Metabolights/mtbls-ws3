from metabolights_utils.models.enums import GenericMessageType
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel
from metabolights_utils.models.parser.enums import ParserMessageType


def evaulate_mtbls_model(model: MetabolightsStudyModel) -> list[str]:
    folder_errors = [
        x for x in model.folder_reader_messages if x.type == GenericMessageType.ERROR
    ]
    error_messages = []
    if folder_errors:
        error_messages.append(
            "Study folder load error:  "
            f"{folder_errors[0].short} {folder_errors[0].detail}"
        )
    parse_errors = []
    for _, messages in model.parser_messages.items():
        parse_errors.extend(
            [x for x in messages if x.type in (ParserMessageType.CRITICAL,)]
        )

    if parse_errors:
        error_messages.append(f"Study file parse errors:  {parse_errors}")

    db_errors = [
        x
        for x in model.db_reader_messages
        if x.type in (GenericMessageType.ERROR, GenericMessageType.CRITICAL)
    ]
    if db_errors:
        error_messages.append(
            f"Database load error:  {db_errors[0].short} {db_errors[0].detail}"
        )

    return error_messages
