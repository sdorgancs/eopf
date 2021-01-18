from __future__ import annotations

import functools
from abc import ABC, abstractmethod
from dataclasses import dataclass
from socket import socket
import sys
import threading
from time import sleep, time
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Mapping,
    Optional,
    Type,
    TypeVar,
    cast,
)
from uuid import uuid4
import uuid

import ray  # type: ignore
from dask_cuda import LocalCUDACluster  # type: ignore
from dask.distributed import Client  # type: ignore
from dask.distributed import LocalCluster as DaskLocalClutster  # type: ignore
from distributed.client import Future  # type: ignore # type: ignore
from ray.util.queue import Queue, Empty  # type: ignore
from ray.exceptions import RayActorError  # type: ignore
import more_itertools as mitertools  # type: ignore

_InputType = TypeVar("_InputType")
_OutputType = TypeVar("_OutputType")


class AsyncResult(ABC, Generic[_OutputType]):
    """AsyncResult defines resuts for asynchronous functions
    """

    @abstractmethod
    def get(self, timeout: Optional[float]) -> _OutputType:
        """Returns the result when it is ready

        :param timeout: timeout duration in fraction of seconds
        :type timeout: Optional[float]
        :return: the resulting value
        :rtype: _OutputType
        """

    @abstractmethod
    def wait(self, timeout: Optional[float]) -> None:
        """Waits until result is ready

        :param timeout: timeout duration in fraction of seconds
        :type timeout: Optional[float]
        """

    @abstractmethod
    def ready(self) -> bool:
        """Test if the result is ready

        :return: True if the result is ready
        :rtype: bool
        """

    @abstractmethod
    def successful(self) -> bool:
        """Waits until result is ready and return True is the computation returns without error
        :rtype: bool
        """


class ReduceResult(AsyncResult[_OutputType], Generic[_OutputType]):
    """AsyncResult defines resuts for asynchronous functions
    """

    def __init__(
        self,
        func: Callable[[_OutputType, _OutputType], _OutputType],
        map_result: MapResult[_OutputType],
    ) -> None:
        self.map_result: MapResult[_OutputType] = map_result
        self.func: Callable[[_OutputType, _OutputType], _OutputType] = func
        self.reduction_result: Optional[_OutputType] = None

    def get(self, timeout: Optional[float]) -> _OutputType:
        """Returns the result when it is ready

        :param timeout: timeout duration in fraction of seconds
        :type timeout: Optional[float]
        :return: the resulting value
        :rtype: _OutputType
        """
        if self.reduction_result is not None:
            return self.reduction_result
        partial_results = self.map_result.get(timeout=timeout)
        self.reduction_result = functools.reduce(self.func, partial_results)
        return self.reduction_result

    def wait(self, timeout: Optional[float]) -> None:
        """Waits until result is ready

        :param timeout: timeout duration in fraction of seconds
        :type timeout: Optional[float]
        """
        self.get(timeout=timeout)

    def ready(self) -> bool:
        """Test if the result is ready

        :return: True if the result is ready
        :rtype: bool
        """
        return self.reduction_result is not None

    def successful(self) -> bool:
        """Waits until result is ready and return True is the computation returns without error
        :rtype: bool
        """
        self.wait(timeout=None)
        return True


class MapResult(Generic[_OutputType], AsyncResult[List[_OutputType]]):
    ...


class DefaultMapResult(Generic[_OutputType], MapResult[_OutputType]):
    """MapResult is used as return type for PoolAPI.map(...) method, it is an AsyncResult managing a list of result
    """

    def __init__(self, results: List[AsyncResult[_OutputType]]) -> None:
        self.results = results

    def get(self, timeout: Optional[float]) -> List[_OutputType]:
        """Returns the result when it is ready

        :param timeout: timeout duration in fraction of seconds
        :type timeout: Optional[float]
        :return: the resulting value
        :rtype: _OutputType
        """
        return [cast(_OutputType, res.get(timeout=timeout)) for res in self.results]

    def wait(self, timeout: Optional[float]) -> None:
        """Waits until result is ready

        :param timeout: timeout duration in fraction of seconds
        :type timeout: Optional[float]
        """
        self.get(timeout=timeout)

    def ready(self) -> bool:
        """Test if the result is ready

        :return: True if the result is ready
        :rtype: bool
        """
        return all([r.ready() for r in self.results])

    def successful(self) -> bool:
        """Waits until result is ready and return True is the computation returns without error
        :rtype: bool
        """
        return all([r.successful() for r in self.results])


def compose(*funcs: Callable[..., Any]) -> Callable[..., Any]:
    """Compose function in the mathematical sense, compose(e, f, g) returns F: x -> e(f(g(x)))

    :return: F: *args -> funcs[0](funcs[1](... funcs[N](args)))...)
    :rtype: Callable[..., Any]
    """
    return functools.reduce(lambda f, g: lambda *args: f(g(*args)), funcs)


@dataclass
class PoolTask(Generic[_OutputType]):
    func: Callable[..., _OutputType]
    args: Optional[Iterable[Any]] = None
    kwds: Optional[Mapping[str, Any]] = None
    callback: Optional[Callable[[_OutputType], None]] = None
    error_callback: Optional[Callable[[BaseException], None]] = None


class PoolAPI:
    """PoolAPI is an abstract class defining a resource Pool.
    A resource pool object which controls a pool of ressources (CPU, GPU, ...) to which jobs can be submitted. It supports asynchronous results with timeouts and callbacks and has a parallel map implementation.
    """

    def __init__(self, n_worker: int) -> None:
        self.n_worker: int = n_worker

    def apply(
        self,
        func: Callable[..., _OutputType],
        args: Optional[Iterable[Any]] = None,
        kwds: Optional[Mapping[str, Any]] = None,
    ) -> Optional[_OutputType]:
        """Call func with arguments args and keyword arguments kwds. It blocks until the result is ready. Given this blocks, apply_async() is better suited for performing work in parallel.

        :param func: the callable to be run
        :type func: Callable[..., _OutputType]
        :param args: arguments to pass to func
        :type args: Optional[Iterable[Any]]
        :param kwds: keyword arguments to pass to func
        :type kwds: Optional[Mapping[str, Any]]
        :return: func output
        :rtype: _OutputType
        """
        return self.apply_async(func=func, args=args, kwds=kwds).get(timeout=None)

    @abstractmethod
    def apply_async(
        self,
        func: Callable[..., _OutputType],
        args: Optional[Iterable[Any]] = None,
        kwds: Optional[Mapping[str, Any]] = None,
        callback: Optional[Callable[[_OutputType], None]] = None,
        error_callback: Optional[Callable[[BaseException], None]] = None,
    ) -> AsyncResult[_OutputType]:
        """A variant of the apply() method which returns a AsyncResult object.

        If callback is specified then it should be a callable which accepts a single argument. When the result becomes ready callback is applied to it, that is unless the call failed, in which case the error_callback is applied instead.

        If error_callback is specified then it should be a callable which accepts a single argument. If the target function fails, then the error_callback is called with the exception instance.

        Callbacks should complete immediately since otherwise the resource which handles the results will get blocked.

        :param func: the callable to be run
        :type func: Callable[..., _OutputType]
        :param args: arguments to pass to func
        :type args: Optional[Iterable[Any]]
        :param kwds: keyword arguments to pass to func
        :type kwds: Optional[Mapping[str, Any]]
        :param callback: a callable that is call after func execution, callback is called with func output as input parameter
        :type callback: Optional[Callable[[_OutputType], None]]
        :param error_callback: a callable that is call when func raise an exception, error_callback is called with this exceptionas input parameter
        :type error_callback: Optional[Callable[[BaseException], None]]
        :return: an asynchronous result
        :rtype: AsyncResult[_OutputType]
        """

    def map(
        self,
        func: Callable[[_InputType], _OutputType],
        iterable: Iterable[_InputType],
        chunksize: Optional[int] = 1,
    ) -> List[_OutputType]:
        """A parallel equivalent of the map() built-in function (it supports only one iterable argument though, for multiple iterables see starmap()). It blocks until the result is ready.

        This method chops the iterable into a number of chunks which it submits to the process pool as separate tasks. The (approximate) size of these chunks can be specified by setting chunksize to a positive integer.

        :param func: the callable to be run
        :type func: Callable[[_InputType], _OutputType]
        :param iterable: The input parameters on which func is called
        :type iterable: Iterable[_InputType]
        :param chunksize: Divides the iterable group of size chuncksize, a group is sent in batch to a resource
        :type chunksize: Optional[int]
        :return: the list of results ([..., func(iterable[i]), ...])
        :rtype: List[_OutputType]
        """
        return self.map_async(func=func, iterable=iterable, chunksize=chunksize).get(
            timeout=None
        )

    def map_async(
        self,
        func: Callable[[_InputType], _OutputType],
        iterable: Iterable[_InputType],
        chunksize: Optional[int] = 1,
        callback: Optional[Callable[[_OutputType], None]] = None,
        error_callback: Optional[Callable[[BaseException], None]] = None,
    ) -> MapResult[_OutputType]:
        """A variant of the map() method which returns a AsyncResult object.

        If callback is specified then it should be a callable which accepts a single argument. When the result becomes ready callback is applied to it, that is unless the call failed, in which case the error_callback is applied instead.

        If error_callback is specified then it should be a callable which accepts a single argument. If the target function fails, then the error_callback is called with the exception instance.

        Callbacks should complete immediately since otherwise the resource which handles the results will get blocked.

        :param func: the callable to be run
        :type func: Callable[[_InputType], _OutputType]
        :param iterable: The input parameters on which func is called
        :type iterable: Iterable[_InputType]
        :param chunksize: Divides the iterable group of size chuncksize, a group is sent in batch to a resource
        :type chunksize: Optional[int]
        :param callback: a callable that is called each time a result is available, callback is called with this result
        :type callback: Optional[Callable[[_OutputType], None]]
        :param error_callback: a callable that is called each time func raise an exception, error_callback is called with this exception
        :type error_callback: Optional[Callable[[BaseException], None]]
        :return: The list of async results ([..., AsyncResult(func(iterable[i])), ...])
        :rtype: MapResult[_OutputType]
        """
        return DefaultMapResult(
            [
                self.apply_async(
                    func=func,
                    args=cast(Optional[Iterable[Any]], e),
                    kwds={},
                    callback=callback,
                    error_callback=error_callback,
                )
                for e in iterable
            ]
        )

    def reduce(
        self,
        func: Callable[[_InputType, _InputType], _InputType],
        iterable: Iterable[_InputType],
    ) -> Optional[_InputType]:
        """A parallel equivalent of the functools.reduce function. It blocks until the result is ready.

        This method chops the iterable into a number of chunks which it submits to the process pool as separate tasks. The (approximate) size of these chunks can be specified by setting chunksize to a positive integer.

        :param func: the callable to be run
        :type func: Callable[[_InputType], _OutputType]
        :param iterable: The input parameters on which func is called
        :type iterable: Iterable[_InputType]
        :return: the list of results ([..., func(iterable[i]), ...])
        :rtype: List[_OutputType]
        """
        return self.reduce_async(func=func, iterable=iterable).get(timeout=None)

    def reduce_async(
        self,
        func: Callable[[_InputType, _InputType], _InputType],
        iterable: Iterable[_InputType],
        callback: Optional[Callable[[_InputType], None]] = None,
        error_callback: Optional[Callable[[BaseException], None]] = None,
    ) -> ReduceResult[_InputType]:
        """A variant of the map() method which returns a AsyncResult object.

        If callback is specified then it should be a callable which accepts a single argument. When the result becomes ready callback is applied to it, that is unless the call failed, in which case the error_callback is applied instead.

        If error_callback is specified then it should be a callable which accepts a single argument. If the target function fails, then the error_callback is called with the exception instance.

        Callbacks should complete immediately since otherwise the resource which handles the results will get blocked.

        :param func: the callable to be run, func must be associative (func(func(x, y), z) == func(x, func(y, z)))
        :type func: Callable[[_InputType, _InputType], _OutputType]
        :param iterable: The input parameters on which func is called
        :type iterable: Iterable[_InputType]
        :param callback: a callable that is called each time a result is available, callback is called with this result
        :type callback: Optional[Callable[[_OutputType], None]]
        :param error_callback: a callable that is called each time func raise an exception, error_callback is called with this exception
        :type error_callback: Optional[Callable[[BaseException], None]]
        :return: The list of async results ([..., AsyncResult(func(iterable[i])), ...])
        :rtype: MapResult[_OutputType]
        """

        chunks = mitertools.divide(n=self.n_worker, iterable=iterable)
        tasks = []

        def reduce(*chunk):
            return functools.reduce(func, chunk)

        for c in chunks:
            task = PoolTask(
                func=reduce, args=c, callback=callback, error_callback=error_callback,
            )
            tasks.append(task)
        result = self.parallel_async(tasks=tasks)
        return ReduceResult[_InputType](func=func, map_result=result)

    def starmap(
        self, func: Callable[..., _OutputType], iterable: Iterable[Iterable[Any]],
    ) -> List[_OutputType]:
        """Like map() except that the elements of the iterable are expected to be iterables that are unpacked as arguments.

        Hence an iterable of [(1,2), (3, 4)] results in [func(1,2), func(3,4)].

        :param func: the callable to be run
        :type func: Callable[[_InputType], _OutputType]
        :param iterable: The input parameters on which func is called
        :type iterable: Iterable[_InputType]
        :param chunksize: Divides the iterable group of size chuncksize, a group is sent in batch to a resource
        :type chunksize: Optional[int]
        :return: the list of results ([..., func(iterable[i]), ...])
        :rtype: List[_OutputType]
        """
        return self.starmap_async(func=star_wrap(func), iterable=iterable).get(
            timeout=None
        )

    def starmap_async(
        self,
        func: Callable[..., _OutputType],
        iterable: Iterable[Iterable[Any]],
        callback: Optional[Callable[[_OutputType], None]] = None,
        error_callback: Optional[Callable[[BaseException], None]] = None,
    ) -> MapResult[_OutputType]:
        """A combination of starmap() and map_async() that iterates over iterable of iterables and calls func with the iterables unpacked. Returns a result object.

        :param func: the callable to be run
        :type func: Callable[[_InputType], _OutputType]
        :param iterable: The input parameters on which func is called
        :type iterable: Iterable[_InputType]
        :param chunksize: Divides the iterable group of size chuncksize, a group is sent in batch to a resource
        :type chunksize: Optional[int]
        :param callback: a callable that is called each time a result is available, callback is called with this result
        :type callback: Optional[Callable[[_OutputType], None]]
        :param error_callback: a callable that is called each time func raise an exception, error_callback is called with this exception
        :type error_callback: Optional[Callable[[BaseException], None]]
        :return: the list of async results ([..., AsyncResult(func(iterable[i])), ...])
        :rtype: MapResult[_OutputType]
        """
        return DefaultMapResult(
            [
                self.apply_async(
                    func=func,
                    args=tuple(it),
                    kwds={},
                    callback=callback,
                    error_callback=error_callback,
                )
                for it in iterable
            ]
        )

    def parallel_async(self, tasks: Iterable[PoolTask[Any]]) -> MapResult[Any]:
        results = []

        for task in tasks:
            func = star_wrap(task.func)
            res = self.apply_async(
                func=func,
                args=task.args,
                kwds=task.kwds,
                callback=task.callback,
                error_callback=task.error_callback,
            )
            results.append(res)
        return DefaultMapResult(results=results)

    def parallel(
        self, tasks: Iterable[PoolTask[Any]], timeout: Optional[float] = None
    ) -> List[Any]:
        """Run tasks in parallel

        :param tasks: a list of PoolTask to run in parallel
        :type tasks: Iterable[PoolTask[Any]]
        :param timeout: timeout, defaults to None
        :type timeout: Optional[float], optional
        :return: a list containing the results of exection of the tasks
        :rtype: List[Any]
        """
        return self.parallel_async(tasks=tasks).get(timeout=timeout)

    @abstractmethod
    def terminate(self) -> None:
        """Stops the worker processes immediately without completing outstanding work. When the pool object is garbage collected terminate() will be called immediately.
        """

    @abstractmethod
    def close(self) -> None:
        """ Prevents any more tasks from being submitted to the pool. Once all the tasks have been completed the worker processes will exit.
        """


class LocalPoolAPI(PoolAPI):
    """Defines the API of a local resource Pool running on a single machine
    """

    def __init__(
        self,
        n_cpu: int = 0,
        memory_limit: float = 0,
        visible_gpu: List[int] = [],
        lazy: bool = False,
    ) -> None:
        """LocalPoolAPI api constructor

        :param n_cpu: number of CPU of this resource pool, when n_cpu is 0 all the logical cores available on the machine are used, defaults to 0
        :type n_cpu: int, optional
        :param memory_limit: maximum memory that can be allocated per cpu in fraction of GB
        :type memory_limit: float, optional
        :param n_visible_gpu: number of visible GPU from this resource Pool, defaults to 0
        :type n_visible_gpu: int, optional
        :param lazy: when lazy is True the resources are not allocated when constructing the Pool but only when a first call is made on a mehtod of the Pool. Subsequent calls to Pool mehtods must not reallocate resources, defaults to False
        :type lazy: bool, optional
        """
        super().__init__(n_cpu)
        self.n_cpu = n_cpu
        self.memory_limit = memory_limit
        self.n_visible_gpu = visible_gpu
        self.lazy = lazy
        self.initialized = False


class DistributedPoolAPI(PoolAPI):
    """Defines the API of a distribued Pool of resources (resources are distributed over a cluster of machine)
    """

    def __init__(
        self,
        n_worker: int,
        n_cpu_per_worker: int,
        memory_limit_per_worker: float,
        n_gpu_per_worker: float,
        local_pool_class: Type[LocalPoolAPI],
    ) -> None:
        """DistributedPoolAPI constructor

        :param n_worker: number of worker to deploy on the cluster
        :type n_worker: int
        :param n_cpu_per_worker: number of cpu per worker
        :type n_cpu_per_worker: int
        :param memory_limit_per_worker: limit of memory, in GB, a worker can use
        :type memory_limit_per_worker: float
        :param n_gpu_per_worker: number of gpu per worker, can be a fraction of gpu
        :type n_gpu_per_worker: float
        :param local_pool_class: class of the LocalPool to use on the workers
        :type local_pool_class: Type[LocalPoolAPI]
        """
        self.n_worker = n_worker
        self.n_cpu_per_worker = n_cpu_per_worker
        self.memory_limit_per_worker = memory_limit_per_worker
        self.n_gpu_per_worker = n_gpu_per_worker
        self.local_pool_class = local_pool_class

    def create_local_pool(
        self, n_cpu: int = 0, memory_limit: float = 0, n_visible_gpu: List[int] = []
    ) -> LocalPoolAPI:
        """Create a lazy local resource Pool to use on a worker

        :param n_cpu: number of CPU of this resource pool, when n_cpu is 0 all the logical cores available on the machine are used, defaults to 0
        :type n_cpu: int, optional
        :param n_visible_gpu: number of visible GPU from this resource Pool, defaults to 0
        :type n_visible_gpu: int, optional
        :param lazy: when lazy is True the resources are not allocated when constructing the Pool but only when a first call is made on a mehtod of the Pool. Subsequent calls to Pool mehtods must not reallocate resources, defaults to False
        :type lazy: bool, optional
        :return: a LocalPoolAPI
        :rtype: LocalPoolAPI
        """
        return self.local_pool_class(n_cpu, memory_limit, n_visible_gpu)


class DaskAsyncResult(AsyncResult):
    def __init__(self, f: Future) -> None:
        __doc__ = super().__init__.__doc__  # noqa: F841
        self.future = f

    def get(self, timeout: Optional[float]) -> _OutputType:
        __doc__ = super().get.__doc__  # noqa: F841
        return self.future.result(timeout=timeout)

    def wait(self, timeout: Optional[float]) -> None:
        __doc__ = super().wait.__doc__  # noqa: F841
        self.future.result(timeout)

    def ready(self) -> bool:
        __doc__ = super().ready.__doc__  # noqa: F841
        return self.future.done()

    def successful(self) -> bool:
        __doc__ = super().successful.__doc__  # noqa: F841
        assert self.ready()
        return not self.future.exception()


def _safe_call(pool, caller, func, args, kwds):
    if hasattr(func, "context") and hasattr(func.context, "pool"):
        func.context.pool = pool
    if caller:
        if args and kwds:
            print("args/kwds")
            return caller(func, args, kwds)
        else:
            return caller(func, args)
        # print("no args")
        # return caller(func)
    else:
        if args and kwds:
            return func(args, kwds)
        if args:
            return func(args)
        return func


class LocalPool(LocalPoolAPI):
    """PoolAPI is an abstract class defining a resource Pool.
    A resource pool object which controls a pool of ressources (CPU, GPU, ...) to which jobs can be submitted. It supports asynchronous results with timeouts and callbacks and has a parallel map implementation.
    """

    def __init__(
        self,
        n_cpu: int = 0,
        memory_limit: float = 0,
        visible_gpu: List[int] = [],
        lazy: bool = False,
    ) -> None:
        super().__init__(
            n_cpu=n_cpu, memory_limit=memory_limit, visible_gpu=visible_gpu, lazy=lazy
        )
        __doc__ = LocalPoolAPI.__init__.__doc__  # noqa: F841
        self.client: Optional[Client] = None
        if not lazy:
            self._init()

    def apply_async(
        self,
        func: Callable[..., _OutputType],
        args: Optional[Iterable[Any]] = None,
        kwds: Optional[Mapping[str, Any]] = None,
        callback: Optional[Callable[[_OutputType], None]] = None,
        error_callback: Optional[Callable[[BaseException], None]] = None,
    ) -> AsyncResult[_OutputType]:
        # copy documentation from super class
        __doc__ = super().apply_async.__doc__  # noqa: F841

        # allocate the local resources if not done previously (lazy)
        if self.client is None:
            self._init()
            assert self.client is not None
        future = _safe_call(None, self.client.submit, func, args, kwds)

        # adpat dask callback mechanism to PoolAPI
        def dask_callback(f: Future):
            exception = f.exception()
            if exception and error_callback:
                error_callback(exception)
            if callback:
                callback(f.result())

        future.add_done_callback(dask_callback)

        return DaskAsyncResult(future)

    def _init(self):
        """Allocate cluster resources
        """
        # cluster = LocalCluster(n_workers=self.n_cpu, scheduler_port=self._get_port())
        # convert memory_limit in byte
        memory_limit = self.memory_limit * 1024 * 1024 * 1024
        if self.n_visible_gpu:
            cluster = LocalCUDACluster(
                n_workers=self.n_cpu,
                memory_limit=int(memory_limit),
                dashboard_address=f":{self._get_port()}",
                CUDA_VISIBLE_DEVICES=self.n_visible_gpu,
            )
        else:
            cluster = DaskLocalClutster(
                n_workers=self.n_cpu,
                memory_limit=int(memory_limit),
                dashboard_address=f":{self._get_port()}",
            )
        self.client = Client(cluster)
        self.initialized = True

    @staticmethod
    def _get_port() -> int:
        """Find a free tcp port to run a new dask cluster

        :return: a free tcp port
        :rtype: int
        """
        with socket() as s:
            s.bind(("", 0))
            return s.getsockname()[1]

    def terminate(self) -> None:
        __doc__ = LocalPoolAPI.__init__.__doc__  # noqa: F841
        assert self.client is not None
        self.client.shutdown()

    def close(self) -> None:
        __doc__ = LocalPoolAPI.__init__.__doc__  # noqa: F841
        assert self.client is not None
        self.client.close()


@dataclass
class _RayTask(Generic[_OutputType]):
    """RayTask is used to the input parameters of apply_async to the Ray workker"""

    task_id: uuid.UUID
    """Identifer of the Task"""
    func: Callable[..., _OutputType]
    """Function to exectute remotely"""
    args: Optional[Iterable[Any]] = None
    """Function non-keyworded arguments"""
    kwds: Optional[Mapping[str, Any]] = None
    """Function keyworded arguments"""
    callback: Optional[Callable[[_OutputType], None]] = None
    """Callback to call after a correct function execution"""
    error_callback: Optional[Callable[[BaseException], None]] = None
    """Callback to call after an erroneous Function execution"""

    def run(self, pool: LocalPoolAPI) -> _RayTaskResult[Any]:
        """Execute the function and call the callbacks"""
        res = _RayTaskResult[_OutputType](self.task_id)
        try:
            res.result = _safe_call(pool, None, self.func, self.args, self.kwds)
            if self.callback and res.result is not None:
                self.callback(res.result)
        except Exception as e:
            res.exception = e
            if self.error_callback:
                self.error_callback(e)
        return res


@dataclass
class _RayMapTask(Generic[_OutputType]):
    """[summary]

    :param Generic: [description]
    :type Generic: [type]
    :raises RuntimeError: [description]
    :return: [description]
    :rtype: [type]
    """

    task_id: uuid.UUID
    """Identifer of the Task"""
    func: Callable[..., _OutputType]
    """Function to exectute remotely"""
    args: Iterable[Any]
    """Function non-keyworded arguments"""
    callback: Optional[Callable[[_OutputType], None]] = None
    """Callback to call after a correct function execution"""
    error_callback: Optional[Callable[[BaseException], None]] = None
    """Callback to call after an erroneous Function execution"""

    def run(self, pool: LocalPoolAPI) -> _RayTaskResult[Any]:
        """Execute the function and call the callbacks"""
        res = _RayTaskResult[List[_OutputType]](self.task_id)
        try:
            res.result = pool.map_async(
                func=self.func,
                iterable=self.args,
                callback=self.callback,
                error_callback=self.error_callback,
            ).get(timeout=None)
        except Exception as e:
            if self.error_callback:
                self.error_callback(e)
        return res


@ray.remote
class _RayExecutorActor:
    """A Ray actor that reads task from the task queue and execute them"""

    def __init__(
        self, task_queue: Queue, result_queue: Queue, local_pool: LocalPoolAPI
    ) -> None:
        """_RayExecutorActor constructor

        :param task_queue: the task queue of the RayDistributedCluster
        :type task_queue: Queue
        :param result_queue: the result queue of the RayDistributedCluster
        :type result_queue: Queue
        """
        self.started = False
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.pool = local_pool

    def start(self):
        """Continuously dequeue tasks form task queue, run them and put the result into the result queue"""
        self.started = True
        self.pool._init()
        while self.started:
            try:
                task = self.task_queue.get(timeout=10, block=True)
                if task is None:
                    continue
                res = task.run(self.pool)
                self.result_queue.put(res)
            except Empty:
                continue

    def stop(self):
        """stop the processing loop"""
        self.started = False


@dataclass
class _RayTaskResult(Generic[_OutputType]):
    """Stores the results of the executed tasks"""

    task_id: uuid.UUID
    """Identifier of the task"""
    result: Optional[_OutputType] = None
    """Result returned by the task"""
    exception: Optional[Exception] = None
    """An eventual exception raised during task execution"""


class _RayAsyncResult(Generic[_OutputType], AsyncResult[_OutputType]):
    """AsyncResult implementation used by RayDistributedCluster"""

    def __init__(self, task_id: uuid.UUID) -> None:
        """_RayAsyncResult constructor

        :param task_id: task identifier
        :type task_id: uuid.UUID
        """
        super().__init__()
        self.task = task_id
        self.result: Optional[_RayTaskResult[_OutputType]] = None

    def get(self, timeout: Optional[float] = None) -> _OutputType:
        __doc__ = super().get.__doc__  # noqa: F841

        self.wait(timeout)
        if self.result is not None and self.result.result is not None:
            return self.result.result
        if self.result is not None:
            raise RuntimeError(self.result.exception)
        raise RuntimeError()

    def wait(self, timeout: Optional[float]) -> None:
        __doc__ = super().wait.__doc__  # noqa: F841

        start_time = time()
        tm = timeout
        if tm is None:
            tm = sys.float_info.max
        while tm > (time() - start_time) and self.result is None:
            sleep(0.001)

    def ready(self) -> bool:
        __doc__ = super().ready.__doc__  # noqa: F841

        return self.result is not None

    def successful(self) -> bool:
        __doc__ = super().successful.__doc__  # noqa: F841
        assert self.ready()
        return self.result is not None and self.result.exception is None


class _RayAsyncMapResult(Generic[_OutputType], MapResult[_OutputType]):
    """[summary]

    :param Generic: [description]
    :type Generic: [type]
    :param MapResult: [description]
    :type MapResult: [type]
    """

    def __init__(self, async_results: List[AsyncResult[List[_OutputType]]]) -> None:
        """_RayAsyncResult constructor

        :param task_ids: list of the Task identifiers to track
        :type task_id: uuid.UUID
        """
        self.results: List[AsyncResult[List[_OutputType]]] = async_results

    def get(self, timeout: Optional[float]) -> List[_OutputType]:
        __doc__ = super().get.__doc__  # noqa: F841
        output_list = []
        for res in self.results:
            try:
                outputs = res.get(timeout=timeout)  # TODO improve timeout management
                assert outputs is not None
                for out in outputs:
                    output_list.append(out)
            except Empty:
                continue
        return output_list

    def wait(self, timeout: Optional[float]) -> None:
        for res in self.results:
            res.wait(timeout=timeout)

    def ready(self) -> bool:
        __doc__ = super().ready.__doc__  # noqa: F841
        return self.results is not None

    def successful(self) -> bool:
        __doc__ = super().successful.__doc__  # noqa: F841
        assert self.ready()
        return all((res.successful() is None for res in self.results))


class DistributedPool(DistributedPoolAPI):
    """PoolAPI is an abstract class defining a resource Pool.
    A resource pool object which controls a pool of ressources (CPU, GPU, ...) to which jobs can be submitted. It supports asynchronous results with timeouts and callbacks and has a parallel map implementation.
    """

    def __init__(
        self,
        n_worker: int,
        n_cpu_per_worker: int,
        memory_limit_per_worker: float = 0,
        n_gpu_per_worker: float = 0,
        max_pending_task: int = 10000,
        local_pool_class: Type[LocalPoolAPI] = LocalPool,
    ) -> None:
        """RayDistributedCluster constructor"""

        super().__init__(
            n_worker=n_worker,
            n_cpu_per_worker=n_cpu_per_worker,
            memory_limit_per_worker=memory_limit_per_worker,
            n_gpu_per_worker=n_gpu_per_worker,
            local_pool_class=local_pool_class,
        )
        self.max_pending_task = max_pending_task
        # create task and results queues
        self.task_queue = Queue(max_pending_task)
        self.result_queue = Queue(max_pending_task)

        # consume processed results from result_queue
        # start consuming result queue
        def consume_result_queue():
            self.started = True
            while self.started:
                try:
                    result = self.result_queue.get(timeout=1, block=True)
                    if type(result) is str:
                        continue

                    if result and result.task_id in self.processed_results:
                        self.processed_results[result.task_id].result = result
                        del self.processed_results[result.task_id]
                except Empty:
                    continue
                except RayActorError:
                    break

        self.result_consumer_thread = threading.Thread(target=consume_result_queue)
        self.result_consumer_thread.start()

        # start actors
        opt = {
            "num_cpus": n_cpu_per_worker,
            "num_gpus": n_gpu_per_worker,
        }

        self.actor_pool = [
            _RayExecutorActor.options(**opt).remote(  # type: ignore
                self.task_queue,
                self.result_queue,
                self.create_local_pool(
                    n_cpu=n_cpu_per_worker, memory_limit=0, n_visible_gpu=[]
                ),
            )
            for _ in range(n_worker)
        ]
        for a in self.actor_pool:
            a.start.remote()

        # wait agent ready
        # self.result_queue.get(block=True, timeout=30)

        self.processed_results: Dict[uuid.UUID, _RayAsyncResult] = {}

    def apply_async(
        self,
        func: Callable[..., _OutputType],
        args: Optional[Iterable[Any]] = None,
        kwds: Optional[Mapping[str, Any]] = None,
        callback: Optional[Callable[[_OutputType], None]] = None,
        error_callback: Optional[Callable[[BaseException], None]] = None,
    ) -> AsyncResult[_OutputType]:
        __doc__ = super().apply_async.__doc__  # noqa: F841
        task = _RayTask(
            task_id=uuid4(),
            func=func,
            args=args,
            kwds=kwds,
            callback=callback,
            error_callback=error_callback,
        )
        self.task_queue.put(task)
        async_res = _RayAsyncResult[_OutputType](task.task_id)
        self.processed_results[task.task_id] = async_res
        return async_res

    def map_async(
        self,
        func: Callable[[_InputType], _OutputType],
        iterable: Iterable[_InputType],
        chunksize: Optional[int] = 1,
        callback: Optional[Callable[[_OutputType], None]] = None,
        error_callback: Optional[Callable[[BaseException], None]] = None,
    ) -> MapResult[_OutputType]:
        __doc__ = super().apply_async.__doc__  # noqa: F841

        chuncks_async_results: List[AsyncResult[List[_OutputType]]] = []
        for c in mitertools.divide(self.n_worker, iterable=iterable):
            task = _RayMapTask(
                task_id=uuid4(),
                func=func,
                args=c,
                callback=callback,
                error_callback=error_callback,
            )

            chunck_async_result = _RayAsyncResult[List[_OutputType]](task.task_id)
            chuncks_async_results.append(chunck_async_result)
            self.processed_results[task.task_id] = chunck_async_result

            self.task_queue.put(task)

        async_res: MapResult[_OutputType] = _RayAsyncMapResult[_OutputType](
            async_results=chuncks_async_results
        )

        return async_res

    def terminate(self) -> None:
        __doc__ = super().terminate.__doc__  # noqa: F841

        self.close()
        for a in self.actor_pool:
            ray.kill(a)

    def close(self) -> None:
        __doc__ = super().close.__doc__  # noqa: F841
        self.started = False
        for a in self.actor_pool:
            a.stop.remote()

        ray.kill(self.result_queue.actor)
        ray.kill(self.task_queue.actor)

        sleep(1)

    def create_local_pool(
        self, n_cpu: int = 0, memory_limit: float = 0, n_visible_gpu: List[int] = []
    ) -> LocalPoolAPI:
        if memory_limit == 0:
            memory_limit = self.memory_limit_per_worker
        return self.local_pool_class(n_cpu, memory_limit, n_visible_gpu, lazy=True)


def star_wrap(func):
    def star_func(args):
        if args is not None:
            return func(*args)
        return func()

    return star_func
