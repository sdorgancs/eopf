from dataclasses import dataclass
from enum import Enum
from typing import Union

from eopf.core.data.catalog import (FeaturePropertyPredicate,
                                    FeaturePropertyTransformation)

Number = Union[int, float]


class MathTransformation(FeaturePropertyTransformation):
    pass


class Round(MathTransformation):
    pass


class Floor(MathTransformation):
    pass


class Ceiling(MathTransformation):
    pass


class Add(MathTransformation):
    value: Number


class Div(MathTransformation):
    value: Number


class Mult(MathTransformation):
    value: Number


class Mod(MathTransformation):
    value: int


class Comparator(Enum):
    LESS_THAN = -2
    LESS_THAN_OR_EQUAL = -1
    EQUAL = 0
    GREATER_THAN_OR_EQUAL = 1
    GREATER_THAN = 2


@dataclass
class MathPredicate(FeaturePropertyPredicate):
    comparator: Comparator
    value: Number
