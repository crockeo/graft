import inspect
from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Callable
from typing import Dict
from typing import Generic
from typing import Set
from typing import TypeVar

import gevent
from gevent.pool import Pool


NodeVal = TypeVar("NodeVal")
T = TypeVar("T")


class Node(Generic[NodeVal]):
    def __init__(self, name: str, work: Callable[..., NodeVal]):
        self.name = name
        self.work = work

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Node):
            return False
        return self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)

    def __call__(self, *args, **kwargs) -> NodeVal:
        return self.work(*args, **kwargs)

    def __repr__(self) -> str:
        return f"Node({self.name})"


class Graph(Generic[T]):
    nodes: Dict[str, Node]
    dependencies: Dict[Node, Set[Node]]
    terminal_node: Node[T]

    def __init__(self):
        self.nodes = {}
        self.dependencies = {}

    def node(self, work: Callable[..., T]) -> Node[T]:
        name = work.__name__
        dependencies = set()

        args = inspect.getfullargspec(work)[0]
        for arg in args:
            if arg not in self.nodes:
                # TODO(crockeo): populate this with something meaningful
                raise KeyError()
            dependencies.add(self.nodes[arg])

        n = Node(work.__name__, work)
        self.nodes[name] = n
        self.dependencies[n] = dependencies

        return n

    def result(self, work: Callable[..., T]) -> Node[T]:
        n = self.node(work)
        self.terminal_node = n
        return n


class Executor(ABC):
    @abstractmethod
    def __call__(self, graph: Graph[T]) -> T:
        pass


class GreenletExecutor(Executor):
    pool: Pool

    def __init__(self, worker_count: int):
        self.pool = gevent.pool.Pool(worker_count)

    def __call__(self, graph: Graph[T]) -> T:
        return_values: Dict[Node, Any] = {}
        not_run: Set[Node] = set(graph.nodes.values())
        can_run = [node for node in not_run if not graph.dependencies[node]]
        has_run: Set[Node] = set()

        def _mark_done(node: Node[NodeVal], value: NodeVal):
            return_values[node] = value
            has_run.add(node)

            to_remove = set()
            for node in not_run:
                if len(graph.dependencies[node] & has_run) == len(graph.dependencies[node]):
                    to_remove.add(node)
                    can_run.append(node)

            for node in to_remove:
                not_run.discard(node)

        def _make_node_wrapper(node: Node) -> Callable[..., T]:
            def _node_wrapper(*args, **kwargs) -> T:
                ret = node(*args, **kwargs)
                _mark_done(node, ret)
                return ret

            return _node_wrapper

        while not_run or can_run:
            if not can_run:
                self.pool.join()
                continue

            node = can_run.pop()

            node_wrapper = _make_node_wrapper(node)
            kwargs = {}
            for dependency in graph.dependencies[node]:
                kwargs[dependency.name] = return_values[dependency]

            self.pool.wait_available()
            self.pool.spawn(node_wrapper, **kwargs)

        self.pool.join()

        return return_values[graph.terminal_node]
