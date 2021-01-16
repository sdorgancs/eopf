from eopf.core.computing.pool import LocalPool
from eopf.core.computing.pool import PoolTask


def sum(a: int, b: int):
    return a + b


if __name__ == "__main__":
    pool = LocalPool(6, 6)
    tasks = [PoolTask(func=sum, args=(i, 10)) for i in range(10)]
    results = pool.parallel(tasks)
    print(results)
