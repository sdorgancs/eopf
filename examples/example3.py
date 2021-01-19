from dataclasses import dataclass
from typing import List
from eopf.algorithms import (
    ProcessingUnit,
    Parameter,
    ParameterValidationResult,
    ProcessingContext,
)
from eopf.algorithms.common.string import (
    Replace,
    ReplaceInput,
    Split,
    SplitInput,
    Concat,
    ConcatInput,
)

from eopf.core.computing.pool import DistributedPool

import ray

ray.init()

dp = DistributedPool(2, 6, 1)


@dataclass
class RCSInput(Parameter):
    strings: List[str]

    def validate(self) -> ParameterValidationResult:
        return ParameterValidationResult(is_ok=True)


@dataclass
class RCSOutput(Parameter):
    strings: List[str]

    def validate(self) -> ParameterValidationResult:
        return ParameterValidationResult(is_ok=True)


class RCS(ProcessingUnit[None, RCSInput, RCSOutput]):
    def call(self, param: RCSInput) -> RCSOutput:
        replace = Replace(self.context)
        split = Split(self.context)
        concat = Concat(self.context)

        rout = replace(ReplaceInput(param.strings, oldvalue="#", newvalue="eopf-"))
        sout = split(SplitInput(strings=rout.results))
        cout = concat(ConcatInput(strings=sout.results))

        return RCSOutput(strings=cout.results)


algo = RCS(ProcessingContext(dp))
vsl = [f"#file{i}.txt" for i in range(4000)]
result = algo(RCSInput(strings=vsl))

print(result.strings[0:10])

dp.close()
