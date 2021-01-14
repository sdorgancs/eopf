from dataclasses import dataclass, field
import logging
from abc import ABC, abstractmethod
from logging import Logger
from typing import Generic, List, Optional, Type, TypeVar, get_args

from dataclasses_jsonschema import JsonSchemaMixin
from eopf.core.computing.pool import PoolAPI, RayPool
from eopf.core.production import configuration


@dataclass
class ParameterValidationResult:
    """Parameters validation result"""

    is_ok : bool
    reasons : List[str] = field(default_factory=list)

class Parameter(ABC, JsonSchemaMixin):
    """Base abstract class for Algorithm input and output parameters"""

    @abstractmethod
    def validate(self) -> ParameterValidationResult:
        """Test if the parameter is valid

        :return: True if the parameter is valid
        :rtype: bool
        """

class InvalidParameter(Exception):
    """Base exception to raise when a parameter is invalid"""

    def __init__(self, reasons: List[str]) -> None:
        super().__init__()
        self.reasons : List[str] = reasons

    def __str__(self) -> str:
        msg = '\n\t- '.join(self.reasons)
        return f"\n\t- {msg}"

class InvalidInputParameter(InvalidParameter):
    """Exception raised when an input parameter is invalid"""

class InvalidOutputParameter(InvalidParameter):
    """Exception raised when an output parameter is invalid"""

class InvalidConfigurationParameter(InvalidParameter):
    """Exception raised when a configuration parameter is invalid"""

Configuration = TypeVar("Configuration")
"""Generic Algorithm configuration parameters"""

Input = TypeVar("Input", bound=Parameter)
"""Generic Algorithm input parameter bound to Parameter abstract class"""

Output = TypeVar("Output", bound=Parameter)
"""Generic Algorithm input parameter bound to Parameter abstract clas"""


class ProcessingContext:
    """Processing context allows a master process to control the resources allocated to an Algorithm and defines the logging confguration"""

    def __init__(self, pool: PoolAPI, logger: Optional[Logger] = None) -> None:
        """ProcessingContext constructor

        :param pool:  the resource pool allocated by the master process to algorithms
        :type pool: PoolAPI
        :param logger: the logger allocated by the master process to the algorithms
        :type logger: Optional[Logger], optional
        """
        self.pool: PoolAPI = pool
        self._logger: Optional[Logger] = logger

    @property
    def logger(self) -> Logger:
        """If the logger is not set returns the default logger

        :return: a logger
        :rtype: Logger
        """
        if self._logger is None:
            return logging.getLogger(__name__)
        return self._logger



class Algorithm(Generic[Configuration, Input, Output], ABC):
    """Algorithm is a generic abstract class that defines and eopf Algorithm. From the user point of view an Algorithm is a Callable[[Input], Output]).
    Errors are handled by exception, the master process calling the algorithm is responsible for handling exceptions.

    :param ABC: utility class to make Algorithm an abstract class
    :type ABC: [type]
    :param Generic[Configuration]: a generic class used to store Algorithm parameters.
    :type Generic[Configuration]: a valid Configuration concrete class is a Python class decorated using @eopf.production.configuration.config
    :param Callable[[Input], Output]): Algorithm is a Callable that has to provide a methode __call__(param: Input) -> Output.
    :type Callable[[Input], Output]): Valid Input and Output concrete classes are Python dataclass that inherits from eopf.production.triggering.Parameter
    """     

    def __init__(self, context: ProcessingContext) -> None:
        """Algorithm constructor

        :param context: allows a master process to control the resources allocated to an Algorithm and to define the logging confguration, defaults to ProcessingContext()
        :type context: ProcessingContext, optional
        """
        self.context: ProcessingContext = context
        if self.context.logger is None:
            self.context.logger = logging.getLogger(self.name())
        if self.configuration_class() != type(None):
            self.configuration = configuration.get(
                self.configuration_class(), self.name())

    @abstractmethod
    def call(self, param: Input) -> Output:
        """Method to overload to implement algorithm logic.
        Errors are managed using Exception, this is the responsability of the master process to manage them

        :param param: an object storing the input parameters of the Algorithm
        :type param: Input
        :return: an object storing the results of the Algorithm
        :rtype: Output, A valid Output concrete class is a dataclass inheriting eopf.production.trigerring.Parameter class
        """

    def __call__(self, param: Input) -> Output:
        """Algorithms are callable objects, this methods wraps call method to validate input and output parameters 

        :param param: an object storing the input parameters of the Algorithm
        :type param: Input
        :return: an object storing the results of the Algorithm
        :rtype: Output, A valid Output concrete class is a dataclass inheriting eopf.production.trigerring.Parameter class
        """

        # validate input parameter
        validation = param.validate()
        if not validation.is_ok:
            raise InvalidOutputParameter(validation.reasons)

        result = self.call(param)

        # validate output parameter
        validation = result.validate()
        if not validation.is_ok:
            raise InvalidOutputParameter(validation.reasons)
        return result

    @classmethod
    def name(clazz: Type) -> str:
        """ Returns the fully qualified name of the concrete Algorithm implementation

        :param clazz: class of the concrete Algorithm implementation
        :type clazz: Type[Algorithm]
        :return: the fully qualified name of the concrete Algorithm implementation
        :rtype: str
        """        
        return ".".join([clazz.__module__, clazz.__name__])

    @classmethod
    def input_class(clazz: Type) -> Type:
        """Returns the concrete Input class of the concrete Algorithm implementation

        :param clazz: class of the concrete Algorithm implementation
        :type clazz: Type[Algorithm]
        :return: the concrete Input class of the concrete Algorithm implementation
        :rtype: Type
        """
        return get_args(clazz.__orig_bases__[0])[1]

    @classmethod
    def output_class(clazz: Type) -> Type:
        """Returns the concrete Output class of the concrete Algorithm implementation

        :param clazz: class of the concrete Algorithm implementation
        :type clazz: Type[Algorithm]
        :return: the concrete Output class of the concrete Algorithm implementation
        :rtype: Type
        """
        return get_args(clazz.__orig_bases__[0])[2]

    @classmethod
    def configuration_class(clazz: Type) -> Type:
        """Returns the concrete Configuration class of the concrete Algorithm implementation

        :param clazz: class of the concrete Algorithm implementation
        :type clazz: Type[Algorithm]
        :return: the concrete Configuration class of the concrete Algorithm implementation
        :rtype: Type
        """
        return get_args(clazz.__orig_bases__[0])[0]
