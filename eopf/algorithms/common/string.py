from dataclasses import dataclass
from typing import List, Optional

from eopf.algorithms import ProcessingUnit
from eopf.core.production.configuration import (Parameter,
                                                ParameterValidationResult,
                                                config)
from eopf.core.production.triggering import expose


@config
class SeparatorConfig(Parameter):
    """SeparatorConfig is a configuration object that contains a string separator"""

    separator: str

    def validate(self) -> ParameterValidationResult:
        if self.separator is None:
            return ParameterValidationResult(
                False, ["Configuration SeparatorConfig.separator cannot be null"]
            )
        return ParameterValidationResult(True)


@dataclass
class ConcatInput(Parameter):
    """ConcatInput is the input parameter of the Concat algorithm"""

    strings: List[List[str]]
    """A list where elements are lists of string, each element of the list is concatenated using a separator"""

    def validate(self) -> ParameterValidationResult:
        if self.strings is None:
            return ParameterValidationResult(
                False, ["Parameter ConcatInput.strings cannot be null"]
            )
        if len(self.strings) <= 0:
            return ParameterValidationResult(
                False, ["Parameter ConcatInput.strings cannot be empty"]
            )
        return ParameterValidationResult(True)


@dataclass
class ConcatOutput(Parameter):
    """ConcatOutput is the output parameter of the Concat algorithm"""

    results: List[str]
    """The list of concatenated strings"""

    def validate(self) -> ParameterValidationResult:
        if self.results is None:
            return ParameterValidationResult(
                False, ["Parameter ConcatOutput.results cannot be null"]
            )
        if len(self.results) <= 0:
            return ParameterValidationResult(
                False, ["Parameter ConcatOutput.results cannot be empty"]
            )
        return ParameterValidationResult(True)


@expose
class Concat(ProcessingUnit[SeparatorConfig, ConcatInput, ConcatOutput]):
    """Concat alorithm take a list in parameter, each element is a list of string which is concatenated"""

    def call(self, param: ConcatInput) -> ConcatOutput:
        self.context.logger.debug("Concat starts")
        # read the separator from the configuration (~/.eopf/config/eopf/algorithms/common/string/Concat.toml)
        assert self.configuration is not None
        assert self.configuration.separator is not None
        sep = self.configuration.separator
        # parallize the concatenation tasks using the resource Pool given by the master process
        results = self.context.pool.map(sep.join, param.strings)
        self.context.logger.debug("Concat ends")
        return ConcatOutput(results)


@dataclass
class SplitInput(Parameter):
    """SplitInput is the input parameter of the Split algorithm"""

    strings: List[str]
    """The list of string to be splitted"""

    def validate(self) -> ParameterValidationResult:
        if self.strings is None:
            return ParameterValidationResult(
                False, ["Parameter SplitInput.strings cannot be null"]
            )
        if len(self.strings) <= 0:
            return ParameterValidationResult(
                False, ["Parameter SplitInput.strings cannot be empty"]
            )
        return ParameterValidationResult(True)


@dataclass
class SplitOutput(Parameter):
    """SplitOutput is the input parameter of the Split algorithm"""

    results: List[List[str]]
    """ a list where each element is a list of string producr by the Split algorithm"""

    def validate(self) -> ParameterValidationResult:
        if self.results is None:
            return ParameterValidationResult(
                False, ["Parameter SplitOutput.results cannot be null"]
            )
        if len(self.results) <= 0:
            return ParameterValidationResult(
                False, ["Parameter SplitOutput.results cannot be empty"]
            )
        return ParameterValidationResult(True)


@expose
class Split(ProcessingUnit[SeparatorConfig, SplitInput, SplitOutput]):
    """Split algorithm take a list of string in input and split them"""

    def call(self, param: SplitInput) -> SplitOutput:
        self.context.logger.debug("Split starts")
        # read the separator from the configuration (~/.eopf/config/eopf/algorithms/common/string/Split.toml)
        assert self.configuration is not None
        sep = self.configuration.separator

        def split(original: str) -> List[str]:
            return original.split(sep)

        # parallize the split tasks using the resource Pool given by the master process
        results = self.context.pool.map(split, param.strings)
        self.context.logger.debug("Split ends")
        return SplitOutput(results)


@dataclass
class ReplaceInput(Parameter):
    """ReplaceInput is the input parameter of the Replace algorithm"""

    strings: List[str]
    """The list of string to be replaced"""
    oldvalue: str
    """the value to replace"""
    newvalue: str
    """the value used to replace oldvalue"""
    maxreplace: Optional[int] = None
    """the maximum number of occurences to replace by string"""

    def is_valid(self) -> bool:
        return (
            (self.strings is not None)
            and (len(self.strings) > 0)
            and (len(self.oldvalue) > 0)
        )

    def validate(self) -> ParameterValidationResult:
        reasons = []
        if self.strings is None:
            reasons.append("Parameter ReplaceInput.strings cannot be null")
        elif len(self.strings) <= 0:
            reasons.append("Parameter ReplaceInput.strings cannot be empty")
        if self.oldvalue is None:
            reasons.append("Parameter ReplaceInput.oldvalue cannot be null")
        elif len(self.oldvalue) <= 0:
            reasons.append("Parameter ReplaceInput.oldvalue cannot be empty")
        if self.newvalue is None:
            reasons.append("Parameter ReplaceInput.newvalue cannot be null")
        return ParameterValidationResult(len(reasons) == 0, reasons)


@dataclass
class ReplaceOutput(Parameter):
    """ReplaceOutput is the input parameter of the Replace algorithm"""

    results: List[str]
    """A list of string where input.olvalue have been replaced by input.newvalue"""

    def validate(self) -> ParameterValidationResult:
        if self.results is None:
            return ParameterValidationResult(
                False, ["Parameter ReplaceOutput.results cannot be null"]
            )
        if len(self.results) <= 0:
            return ParameterValidationResult(
                False, ["Parameter ReplaceOutput.results cannot be empty"]
            )
        return ParameterValidationResult(True)


@expose
class Replace(ProcessingUnit[None, ReplaceInput, ReplaceOutput]):
    """Replace algorithm replaces input.oldvalue by input.newvalue for each items of input.strings"""

    def call(self, param: ReplaceInput) -> ReplaceOutput:
        self.context.logger.debug("Replace starts")

        def replace(original: str):
            if param.maxreplace is None:
                return original.replace(param.oldvalue, param.newvalue)
            else:
                return original.replace(
                    param.oldvalue, param.newvalue, param.maxreplace
                )

        print(f"{self.context.pool}")
        # parallize the replace tasks using the resource Pool given by the master process
        results = self.context.pool.map(replace, param.strings)
        self.context.logger.debug("Replace ends")
        return ReplaceOutput(results)
