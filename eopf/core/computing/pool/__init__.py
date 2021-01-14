from __future__ import annotations

import functools
import os
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
from distributed.client import Future  # type: ignore
from ray.util.multiprocessing import Pool as OriginalRayPool  # type: ignore
from ray.util.multiprocessing.pool import RAY_ADDRESS_ENV  # type: ignore
from ray.util.multiprocessing.pool import logger  # type: ignore
from ray.util.queue import Queue, Empty  # type: ignore
from ray.exceptions import RayActorError  # type: ignore

_InputType = TypeVar("_InputType")
_OutputType = TypeVar("_OutputType")


class AsyncResult(ABC, Generic[_OutputType]):
    """AsyncResult defines resuts for asynchronous functions
    """

    @abstractmethod
    def get(self, timeout: Optional[float]) -> Optional[_OutputType]:
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


class MapResult(AsyncResult[List[_OutputType]]):
    """MapResult is used as return type for PoolAPI.map(...) method, it is an AsyncResult managing a list of result
    """

    def __init__(self, results: List[AsyncResult[_OutputType]]) -> None:
        self.result = results

    def get(self, timeout: Optional[float]) -> List[_OutputType]:
        """Returns the result when it is ready

        :param timeout: timeout duration in fraction of seconds
        :type timeout: Optional[float]
        :return: the resulting value
        :rtype: _OutputType
        """
        return [cast(_OutputType, res.get(timeout=timeout)) for res in self.result]

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
        return all([r.ready() for r in self.result])

    def successful(self) -> bool:
        """Waits until result is ready and return True is the computation returns without error
        :rtype: bool
        """
        return all([r.successful() for r in self.result])


def compose(*funcs: Callable[..., Any]) -> Callable[..., Any]:
    """Compose function in the mathematical sense, compose(e, f, g) returns F: x -> e(f(g(x)))

    :return: F: *args -> funcs[0](funcs[1](... funcs[N](args)))...)
    :rtype: Callable[..., Any]
    """
    return functools.reduce(lambda f, g: lambda *args: f(g(*args)), funcs)


class PoolAPI:
    """PoolAPI is an abstract class defining a resource Pool.
    A resource pool object which controls a pool of ressources (CPU, GPU, ...) to which jobs can be submitted. It supports asynchronous results with timeouts and callbacks and has a parallel map implementation.
    """

    def __init__(self, n_worker) -> None:
        self.n_worker = n_worker

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
        return MapResult(
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
        func: Callable[[_InputType, _InputType], _OutputType],
        iterable: Iterable[_InputType],
        chunksize: Optional[int] = 1,
    ) -> Optional[_OutputType]:
        """A parallel equivalent of the functools.reduce function. It blocks until the result is ready.

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
        return self.reduce_async(func=func, iterable=iterable, chunksize=chunksize).get(
            timeout=None
        )

    def reduce_async(
        self,
        func: Callable[[_InputType, _InputType], _OutputType],
        iterable: Iterable[_InputType],
        chunksize: Optional[int],
        callback: Optional[Callable[[_OutputType], None]] = None,
        error_callback: Optional[Callable[[BaseException], None]] = None,
    ) -> AsyncResult[_OutputType]:
        """A variant of the map() method which returns a AsyncResult object.

        If callback is specified then it should be a callable which accepts a single argument. When the result becomes ready callback is applied to it, that is unless the call failed, in which case the error_callback is applied instead.

        If error_callback is specified then it should be a callable which accepts a single argument. If the target function fails, then the error_callback is called with the exception instance.

        Callbacks should complete immediately since otherwise the resource which handles the results will get blocked.

        :param func: the callable to be run
        :type func: Callable[[_InputType, _InputType], _OutputType]
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

    def starmap(
        self,
        func: Callable[..., _OutputType],
        iterable: Iterable[Iterable[Any]],
        chunksize: Optional[int],
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
        return self.starmap_async(
            func=func, iterable=iterable, chunksize=chunksize
        ).get(timeout=None)

    def starmap_async(
        self,
        func: Callable[..., _OutputType],
        iterable: Iterable[Iterable[Any]],
        chunksize: Optional[int],
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
        return MapResult(
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
        n_visible_cpu_per_worker: int,
        local_pool_class: Type[LocalPoolAPI],
    ) -> None:
        """DistributedPoolAPI constructor

        :param n_worker: number of worker to deploy on the cluster
        :type n_worker: int
        :param n_cpu_per_worker: number of cpu per worker
        :type n_cpu_per_worker: int
        :param n_visible_cpu_per_worker: number of visible gpu per worker
        :type n_visible_cpu_per_worker: int
        :param local_pool_class: class of the LocalPool to use on the workers
        :type local_pool_class: Type[LocalPoolAPI]
        """
        self.n_worker = n_worker
        self.n_cpu_per_worker = n_cpu_per_worker
        self.n_visible_cpu_per_worker = n_visible_cpu_per_worker
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


class RayPool(OriginalRayPool):
    """ Implementation of the PoolAPI using ray framework
    """

    def _init_ray(self, processes=None, ray_address=None):
        """
        This method have been overloadded to deactivate the dashboard
        """
        # Initialize ray. If ray is already initialized, we do nothing.
        # Else, the priority is:
        # ray_address argument > RAY_ADDRESS > start new local cluster.
        if not ray.is_initialized():
            # Cluster mode.
            if ray_address is None and RAY_ADDRESS_ENV in os.environ:
                logger.info(
                    "Connecting to ray cluster at address='{}'".format(
                        os.environ[RAY_ADDRESS_ENV]
                    )
                )
                ray.init(include_dashboard=False)
            elif ray_address is not None:
                logger.info(f"Connecting to ray cluster at address='{ray_address}'")
                ray.init(address=ray_address, include_dashboard=False)
            # Local mode.
            else:
                logger.info("Starting local ray cluster")
                ray.init(num_cpus=processes, include_dashboard=False)

        ray_cpus = int(ray.state.cluster_resources()["CPU"])
        if processes is None:
            processes = ray_cpus
        if processes <= 0:
            raise ValueError("Processes in the pool must be >0.")
        if ray_cpus < processes:
            raise ValueError(
                "Tried to start a pool with {} processes on an "
                "existing ray cluster, but there are only {} "
                "CPUs in the ray cluster.".format(processes, ray_cpus)
            )

        return processes


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


def _safe_call(caller, func, args, kwds):
    if caller:
        if args and kwds:
            return caller(func, args, kwds)
        if args:
            return caller(func, args)
        return caller(func)
    else:
        if args and kwds:
            return func(args, kwds)
        if args:
            return func(args)
        return func


class LocalCluster(LocalPoolAPI):
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

        self.initialized = False
        self.client: Client
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
        if not self.initialized:
            self._init()
        future = _safe_call(self.client.submit, func, args, kwds)

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
        self.client.shutdown()

    def close(self) -> None:
        __doc__ = LocalPoolAPI.__init__.__doc__  # noqa: F841
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

    def run(self) -> _RayTaskResult[Any]:
        """Execute the function and call the callbacks"""
        res = _RayTaskResult[_OutputType](self.task_id)
        try:
            out = _safe_call(None, self.func, self.args, self.kwds)
            res.result = out
            if self.callback:
                self.callback(out)
        except Exception as e:
            res.exception = e
            if self.error_callback:
                self.error_callback(e)
        return res


@ray.remote
class _RayExecutorActor:
    """A Ray actor that reads task from the task queue and execute them"""

    def __init__(self, task_queue: Queue, result_queue: Queue) -> None:
        """_RayExecutorActor constructor

        :param task_queue: the task queue of the RayDistributedCluster
        :type task_queue: Queue
        :param result_queue: the result queue of the RayDistributedCluster
        :type result_queue: Queue
        """
        self.started = False
        self.task_queue = task_queue
        self.result_queue = result_queue

    def start(self):
        """Continuously dequeue tasks form task queue, run them and put the result into the result queue"""
        with open("ray.log", "w") as f:
            f.write("test\n")
            f.flush()
            self.started = True
            self.result_queue.put("start")
            while self.started:
                try:
                    f.write("waiting\n")
                    f.flush()
                    task = self.task_queue.get(timeout=1, block=True)
                    f.write(f"task {task}\n")
                    f.flush()
                    if task is None:
                        continue
                    res = task.run()
                    f.write(f"result {res}\n")
                    f.flush()
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

    def get(self, timeout: Optional[float]) -> Optional[_OutputType]:
        __doc__ = super().get.__doc__  # noqa: F841

        self.wait(timeout)
        if self.result:
            return self.result.result
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


class DistributedCluster(DistributedPoolAPI):
    """PoolAPI is an abstract class defining a resource Pool.
    A resource pool object which controls a pool of ressources (CPU, GPU, ...) to which jobs can be submitted. It supports asynchronous results with timeouts and callbacks and has a parallel map implementation.
    """

    def __init__(
        self,
        n_worker: int,
        n_cpu_per_worker: int,
        n_visible_cpu_per_worker: int,
        local_pool_class: Type[LocalPoolAPI],
        max_pending_task: int = 10000,
    ) -> None:
        """RayDistributedCluster constructor"""

        super().__init__(
            n_worker, n_cpu_per_worker, n_visible_cpu_per_worker, local_pool_class
        )
        # create task and results queues
        self.task_queue = Queue(max_pending_task)
        self.result_queue = Queue(max_pending_task)
        # start actors
        opt = {
            "num_cpus": n_cpu_per_worker,
            "num_gpus": n_visible_cpu_per_worker,
        }

        self.actor_pool = [
            _RayExecutorActor.options(**opt).remote(  # type: ignore
                self.task_queue, self.result_queue
            )
            for _ in range(n_worker)
        ]
        for a in self.actor_pool:
            a.start.remote()

        # wait agent ready
        self.result_queue.get(block=True, timeout=30)

        # consume processed results from result_queue
        self.processed_results: Dict[uuid.UUID, _RayAsyncResult] = {}
        self.started = False

        def consume_result_queue():
            self.started = True
            while self.started:
                # if self.result_queue.empty():
                #     continue
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

    def terminate(self) -> None:
        __doc__ = super().terminate.__doc__  # noqa: F841

        self.close()
        for a in self.actor_pool:
            ray.kill(a)

    def close(self) -> None:
        __doc__ = super().close.__doc__  # noqa: F841

        for a in self.actor_pool:
            a.stop.remote()

        ray.kill(self.result_queue.actor)
        ray.kill(self.task_queue.actor)
        self.started = False
        sleep(1)

    def create_local_pool(
        self, n_cpu: int = 0, memory_limit: float = 0, n_visible_gpu: List[int] = []
    ) -> LocalPoolAPI:
        return self.local_pool_class(n_cpu, memory_limit, n_visible_gpu, True)
