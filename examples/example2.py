from typing import List
from eopf.core.computing.pool import DistributedPool, PoolAPI
from functools import partial


def test2(who):
    return f"Hello {who}"


def testmult(sl: List[str], pool: PoolAPI):
    res = pool.map(lambda s: s.split("."), sl)
    pool.close()
    return res


if __name__ == "__main__":
    import ray

    ray.init(include_dashboard=True)
    pool = DistributedPool(2, 4, 1.5)

    print(pool.map(test2, ["world", "toto"]))

    dask_pool = pool.create_local_pool(4)
    vsl = [f"file{i}.txt" for i in range(4000)]
    res = pool.map(partial(testmult, pool=dask_pool), [vsl, vsl])
    assert res is not None
    print(res[0][0:10])
    print(len(res))

    pool.close()
    # print("shutdown")
    # ray.shutdown(True)
