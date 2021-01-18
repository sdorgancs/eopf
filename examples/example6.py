from eopf.core.computing.pool import LocalPool
from eopf.core.computing.pool import star_wrap


def sum(a: int, b: int):
    return a + b


@star_wrap
def sum2(a: int, b: int):
    return a + b


if __name__ == "__main__":
    pool = LocalPool(6, 6)
    ziplist = [(i, j) for i in range(100) for j in range(100)]
    res = pool.starmap(sum, ziplist)
    res2 = pool.map(sum2, ziplist)
    print(res[99])
    print(res2[99])
