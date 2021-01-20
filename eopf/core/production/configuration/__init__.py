from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Type, TypeVar

from dataclasses_jsonschema import JsonSchemaMixin
from genericpath import exists
from serde import from_dict  # type: ignore
from serde import deserialize, serialize
from serde.toml import TomlDeserializer  # type: ignore


@dataclass
class ParameterValidationResult:
    """Parameters validation result"""

    is_ok: bool
    reasons: List[str] = field(default_factory=list)


class Parameter(ABC, JsonSchemaMixin):
    """Base abstract class for Algorithm input and output parameters"""

    @abstractmethod
    def validate(self) -> ParameterValidationResult:
        """Test if the parameter is valid

        :return: True if the parameter is valid
        :rtype: bool
        """


def config_file_name(afqn: str) -> str:
    """Compute the name of the configuration file of the class which the fully qualified name (module.submodule.ClassName) is afqn

    :param afqn: fully qualified name of the class to configure
    :type afqn: str
    :return: the path of the configuration file
    :rtype: str
    """
    p = afqn.replace(".", "/")
    return f"{Path.home()}/.eopf/config/{p}.toml"


T = TypeVar("T")


def get(model: Type[T], afqn: str) -> Optional[T]:
    """Create a Python object of type Type[T] deserializing configuration file of the class which fully qualified name is afqn.
    The path of the configuration file is compute by the config_file_name function

    :param model: Type use a model to create a Python object deserializing a configuration file
    :type model: Type[T]
    :param afqn: fully qualified name of the class to configure
    :type afqn: str
    :return: an instance of Type[T] created deserializing the configuration file
    :rtype: T
    """
    name = config_file_name(afqn)
    if not exists(name):
        return None
    with open(name) as c:
        return from_dict(model, TomlDeserializer.deserialize(c.read()))


def config(clazz):
    """Decorator allowing to transform a python object into a configuration file, and vice versa

    :param clazz: class to decorate
    :return: the decorated class
    """
    return deserialize(serialize(dataclass(clazz)))
