from enum import Enum

class Cardinality(Enum):
    ONE_TO_ONE = "1..1"
    ONE_TO_MANY = "1..n"
    MANY_TO_ONE = "n..1"
    MANY_TO_MANY = "n..n"

def infer_cardinality(count1: int, count2: int) -> Cardinality:
    if count1 == 1 and count2 == 1:
        return Cardinality.ONE_TO_ONE
    elif count1 == 1 and count2 > 1:
        return Cardinality.ONE_TO_MANY
    elif count1 > 1 and count2 == 1:
        return Cardinality.MANY_TO_ONE
    else:
        return Cardinality.MANY_TO_MANY
