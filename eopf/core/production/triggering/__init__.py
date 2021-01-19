from typing import Dict, Type
from eopf.algorithms import ProcessingUnit


class AlgorithmRegistry:
    def __init__(self) -> None:
        self.algorithms: Dict[str, Type[ProcessingUnit]] = dict()

    def register(self, algo: Type[ProcessingUnit]) -> None:
        self.algorithms[algo.name()] = algo

    def __call__(self) -> None:
        pass


registry = AlgorithmRegistry()


def expose(algo: Type[ProcessingUnit]) -> Type[ProcessingUnit]:
    """Expose is a class annotation used to add Algorithms to the command line tool and the webservice"""
    registry.register(algo)
    return algo


def import_algorithms():
    from pkgutil import walk_packages
    from importlib import import_module
    from eopf import algorithms

    for m in walk_packages(algorithms.__path__, algorithms.__name__ + "."):
        import_module(m.name)
