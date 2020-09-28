import inspect
from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Callable
from typing import Dict
from typing import Generic
from typing import List
from typing import Optional
from typing import Set
from typing import TypeVar

from gevent import Greenlet
from gevent.pool import Pool


T = TypeVar("T")
U = TypeVar("U")


class WorkItem(Generic[T], ABC):
    @abstractmethod
    def __call__(self, *args, **kwargs) -> T:
        pass


class Node(Generic[T], WorkItem[T]):
    name: str
    # intentionally left out, because mypy complains about being unable to assign to a method
    # work: Callable[..., T]

    def __init__(self, work: Callable[..., T], name: Optional[str] = None):
        self.name = name if name else work.__name__
        self.work = work

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Node):
            return False
        return self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)

    def __call__(self, *args, **kwargs) -> T:
        return self.work(*args, **kwargs)

    def __repr__(self) -> str:
        return f"Node({self.name})"


class BaseGraph(Generic[T]):
    vertices: Set[T]
    edges: Dict[T, Set[T]]

    def __init__(self):
        self.vertices = set()
        self.edges = {}

    def add_vertex(self, t: T) -> "BaseGraph[T]":
        """
        Adds a vertex to the graph.
        """
        self.vertices.add(t)
        return self

    def add_edge(self, from_vertex: T, to_vertex: T) -> "BaseGraph[T]":
        """
        Adds a directed edge from a particular vertex to another vertex.
        """
        if from_vertex not in self.vertices or to_vertex not in self.vertices:
            raise KeyError()

        self.edges[from_vertex].add(to_vertex)
        return self

    def add_edges(self, from_vertex: T, to_vertices: Set[T]) -> "BaseGraph[T]":
        """
        Adds directed edges from a particular vertex to many other vertices, using built-in set
        operations as an accelerant.
        """
        if from_vertex not in self.vertices or not (to_vertices & self.vertices):
            raise KeyError()

        if from_vertex not in self.edges:
            self.edges[from_vertex] = set()
        self.edges[from_vertex] |= to_vertices
        return self

    def topological_ordering(self) -> List[T]:
        """
        Produces the topological ordering for the nodes contained in this graph. If a cycle is
        detected, then this throws an exception instead.
        """
        # TODO(crockeo): implement topological sort
        return []


class Graph(Generic[T], WorkItem[T]):
    nodes: Dict[str, Node]
    graph: BaseGraph[Node]
    topological_ordering: List[Node]
    terminal_node: Optional[Node[T]]

    def __init__(self):
        self.nodes = {}
        self.graph = BaseGraph()
        self.topological_ordering = []

    # TODO(crockeo): typing
    def node(self, *, name: Optional[str] = None, terminal: bool = False):
        """
        A decorator that maps a function onto its corresponding Node while registering it onto the
        graph and recomputing the topological ordering. Example:

        graph = Graph()

        @graph.node()
        def work() -> int:
            return 5

        @graph.node(terminal=True)
        def work2(work: int) -> int:
            return 2 * work

        graph()  # 10
        """

        def _node(work: Callable) -> Node:
            n = Node(work, name)

            dependencies: Set[Node] = set()
            for arg in inspect.getfullargspec(work)[0]:
                if arg not in self.nodes:
                    # TODO(crockeo): raise better
                    raise KeyError()
                dependencies.add(self.nodes[arg])

            if terminal:
                if self.terminal_node is not None:
                    # TODO(crockeo): raise better
                    raise Exception()
                    # if not isinstance(n, Node[T]):
                    #     # TODO(crockeo): raise better
                    raise Exception()

                self.terminal_node = n

            self.nodes[n.name] = n
            self.graph.add_vertex(n)
            self.graph.add_edges(n, dependencies)

            return n

        return _node

    def __call__(self, *args, **kwargs) -> T:
        """
        Executes all of the Nodes contained in this graph asynchronously (ordering according to
        their dependencies).
        """
        if self.terminal_node is None:
            # TODO: raise better
            raise Exception()

        pool = Pool(5)  # TODO(crockeo): parametrize worker count
        return_values: Dict[Node, Any] = {}
        in_flight: Dict[Node, Greenlet] = {}

        topological_ordering = self.graph.topological_ordering()
        i = 0
        while i < len(topological_ordering) and not in_flight:
            head = topological_ordering[i]

            # collect any other nodes that haven't finished yet
            missing_dependencies: Set[Greenlet] = set()
            for dependency in self.graph.edges[head]:
                if dependency not in return_values:
                    if dependency not in in_flight:
                        # TODO(crockeo): raise better
                        raise Exception()
                    missing_dependencies.add(in_flight[dependency])

            # wait for all missing deps
            for missing_dependency in missing_dependencies:
                missing_dependency.join()

            # collect kwargs
            kwargs = {
                dependency.name: return_values[dependency] for dependency in self.graph.edges[head]
            }

            # launch it!!
            pool.wait_available()
            in_flight[head] = pool.spawn(
                self._make_node_wrapper(head, return_values, in_flight),
                **kwargs,
            )

        return return_values[self.terminal_node]

    def _make_node_wrapper(
        self,
        node: Node[U],
        return_values: Dict[Node, Any],
        in_flight: Dict[Node, Greenlet],
    ) -> Callable[..., U]:
        """
        Produces a Callable wrapper around a Node to perform clean-up and earmarking after it's
        done executing.
        """

        def _node_wrapper(*args, **kwargs) -> U:
            ret = node(*args, **kwargs)
            return_values[node] = ret
            # aggressively throwing now so we can see if this edge case every comes up
            in_flight.pop(node)
            return ret

        return _node_wrapper
