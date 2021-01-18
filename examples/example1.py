from eopf.core.computing.pool import LocalPool


def test(who):
    print("Hello", who)


def test2(who):
    return f"Hello {who}"


def test3(a):
    return a * a


if __name__ == "__main__":
    pool = LocalPool(n_cpu=2, memory_limit=4, visible_gpu=[], lazy=False)
    pool2 = LocalPool(2, 4, [], False)

    pool.apply(test, "world")
    pool2.apply(test, "toto")

    print(pool.map(test2, ["world", "toto"]))

    print(pool2.map(test3, [3, 4]))
