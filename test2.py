from typing import List
from eopf.core.computing.pool import LocalCluster, LocalPoolAPI, DistributedCluster



def test2(who):
    return f"Hello {who}"

def testmult(pool: LocalPoolAPI, sl: List[str]): 
    return pool.map(lambda s : s.split(".") , sl)


if __name__ == "__main__":
    import ray
    ray.init(include_dashboard=False)
    pool = DistributedCluster(2, 1, 0,  LocalCluster, 100)

    print(pool.map(test2, ["world", "toto"]))

    dask_pool = pool.create_local_pool(4)
    vsl = [f"file{i}.txt" for i in  range(4000)]
    res = pool.map(lambda sl: testmult(dask_pool, sl), [vsl, vsl])
    assert not res is None
    print(res[0][0:10])
    print(len(res))

    pool.close()
    print("shutdown")
    ray.shutdown(True)
