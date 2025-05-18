from enum import Enum

class Cardinality(Enum):
    ONE_TO_ONE = "1..1"
    ONE_TO_MANY = "1..*"
    MANY_TO_ONE = "*..1"
    MANY_TO_MANY = "*..*"