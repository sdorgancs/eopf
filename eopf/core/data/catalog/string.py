from dataclasses import dataclass
from typing import Optional

from eopf.core.data.catalog import (
    FeaturePropertyPredicate,
    FeaturePropertyTransformation,
)


class StringPredicate(FeaturePropertyPredicate):
    pass


class SubstringOf(StringPredicate):
    string: str


class EndWith(StringPredicate):
    string: str


class StartWith(StringPredicate):
    string: str


class StringTransformation(FeaturePropertyTransformation):
    pass


class Length(StringTransformation):
    pass


@dataclass
class IndexOf(StringTransformation):
    find: str


@dataclass
class Replace(StringTransformation):
    find: str
    replace: str


@dataclass
class Substring(StringTransformation):
    pos: int
    Length: Optional[int]


class ToLower(StringTransformation):
    pass


class ToUpper(StringTransformation):
    pass


class Trim(StringTransformation):
    pass


class Concat(StringTransformation):
    tail: str
