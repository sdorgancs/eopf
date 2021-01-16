# import ray
from eopf.algorithms import ProcessingContext
from eopf.algorithms.common.string import (
    Concat,
    ConcatInput,
)

from eopf.core.computing.pool import LocalPool

strings = [[f"{f}", "txt"] for f in range(12)]

param = ConcatInput(strings)

#
if __name__ == "__main__":
    # ray.init()
    local = LocalPool(6, 2)
    concat = Concat(ProcessingContext(local))

    out = concat(param=param)
    print(out.results)

    def sum(a: int, b: int) -> int:
        return a + b

    s = local.reduce(sum, list(range(100)))
    s2 = 0
    for e in range(100):
        s2 = s2 + e
    print(s2)
    print(s)
    local.close()
