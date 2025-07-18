import enum


class FilterOperand(enum.StrEnum):
    EQ = "eq"
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="
    LIKE = "like"
    IN = "in_"
