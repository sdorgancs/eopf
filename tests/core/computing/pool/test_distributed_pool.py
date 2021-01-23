import pytest
import ray
from eopf.core.computing.pool import DistributedPool, PoolTask, star_wrap


@pytest.fixture(scope="session")
def ditributed_cluster():
    ray.init("auto")
    yield DistributedPool(2, 4)
    ray.shutdown()


def test_map(ditributed_cluster):
    def square(x: float) -> float:
        return x * x

    params = [i for i in range(10)]
    results = ditributed_cluster.map(square, params)
    assert results == [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

    @star_wrap
    def power(v: float, power: float) -> float:
        return v ** power

    powers = [(i % 2) * 2 for i in range(10)]
    params = zip(params, powers)
    results = ditributed_cluster.map(power, params)
    assert results == [1, 1, 1, 9, 1, 25, 1, 49, 1, 81]


def test_parallel(ditributed_cluster):
    params = [(i, j) for i in range(4) for j in range(4)]
    tasks = []

    for i, j in params:
        sumtask = PoolTask(func=lambda x, y: x + y, args=(i, j))
        tasks.append(sumtask)

    results = ditributed_cluster.parallel(tasks)
    expected_results = [i + j for i in range(4) for j in range(4)]
    assert results == expected_results


def test_reduce(ditributed_cluster):
    N = 1000
    result = ditributed_cluster.reduce(func=lambda x, y: x + y, iterable=range(N))
    assert result == (N * (N - 1)) / 2
