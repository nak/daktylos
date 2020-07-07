"""
This package contains the classes used to define composite metrics, a hierarchical collection of floating point metric
values.  It also defines the interface for collection and retrieval of those values from a data store.
"""
import datetime
import multiprocessing
import numbers
import platform
import socket
from abc import abstractmethod, ABC
from contextlib import AbstractContextManager
from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Optional, Iterable, Union, Set

__all__ = ["Metadata", "Metric", "CompositeMetric", "MetricStore"]


@dataclass
class Metadata:
    """
    Information data associated with a (top-level) composite metric
    """

    class Types(Enum):
        """
        Allowed types of metrics
        """
        STRING = 'str'
        INTEGER = 'int'
        FLOAT = 'float'

    values: Dict[str, Union[str, int, float]]
    """
    The metadata key/value pairs
    """

    @staticmethod
    def system_info() -> "Metadata":
        """
        :return: a standard set of metadata descibing the host system
        """
        try:
            ip_address = socket.gethostbyname(socket.getfqdn())
        except Exception:
            ip_address = "<<indeterminate>>"
        return Metadata({
            'machine': platform.machine(),
            'platform': platform.platform(),
            'system': platform.system(),
            'processor': platform.processor(),
            'num_cores': multiprocessing.cpu_count(),
            'ip_address': ip_address
        })


@dataclass
class BasicMetric(ABC):
    name: str
    metadata: Optional[Metadata]

    @abstractmethod
    def flatten(self, prefix: str="") -> Dict[str, float]:
        """
        Flatten the hierarchy of metrics to a simple Dict

        :return: dict of string-path/float pairs
        """

    @classmethod
    def from_dict(cls, values: Dict[str, float]) -> "BasicMetric":
        """
        Return a metric object from the given str/float value pairs.
        The names must match a specific convention, and this method is only
        meant for use in "unflattening" metrics that have been produced
        from the "flatten" method (either explicitly or restored from a datastore)

        :param values: a set of string-value pairs that representes the metric
        :return: Metric or CompositeMetric equivalent of the given names and values
        :raises ValueError: if values is not conformant with a flattened metric exepcteation
        """
        if len(values) == 0:
            raise ValueError("Empty value set when constructing Metric")
        if len(values.values()) == 1 and type(list(values.values())[0]) == float:
            name = list(values.keys())[0]
            if '#' in name:
                raise ValueError("Metric names must not contain a '#'")
            return Metric(name=name, value=list(values.values())[0])  # TODO: in 3.8 can use walrus operator in if

        root: Optional[BasicMetric] = None

        def process(path: str, value: float):
            nonlocal root
            if '#' not in path:
                raise ValueError("Composite metric path must contain one '#' element")
            location, name = path.split('#')
            path_elements = location[1:].split('/')
            if root is None:
                root = CompositeMetric(name=path_elements[0], values=[])
            elif path_elements[0] != root.name:
                raise ValueError(f"More than one root found: {root.name} and {path_elements[0]}")

            base = root
            for element in path_elements[1:]:
                if not isinstance(base, CompositeMetric):
                    raise ValueError("Mixed composite and leaf nodes at same level")
                if element not in base.value:
                    base.add(CompositeMetric(name=element))
                base = base.value[element]
                if not isinstance(base, CompositeMetric):
                    raise ValueError("Mixed composite and leaf nodes at same level")

            if name in base.value:
                raise ValueError("Mixed composite and leaf nodes at same level")
            base.add(Metric(name=name, value=value))

        for path, value in values.items():
            process(path, value)

        if not root:
            raise ValueError("No data to process")
        return root


@dataclass
class Metric(BasicMetric):
    """
    Leaf class defining an actual key/value pair
    """
    value: float

    def __init__(self, name: str, value: float):
        if not isinstance(value, numbers.Number):
            raise ValueError("Metric values must be numbers")
        if '#' in name:
            raise ValueError("Name cannot contain '#'")
        if not name:
            raise ValueError("Metric name cannot be empty")
        super().__init__(name, None)
        self.value = value

    def flatten(self, prefix: str="") -> Dict[str, float]:
        if prefix:
            return {'#'.join([prefix, self.name]): self.value}
        else:
            return {self.name: self.value}


@dataclass
class CompositeMetric(BasicMetric):
    """
    CompositMetric comprising a collection of other `Metric` or `CompositeMetric` instances.
    The collection of metrics is contained in a hierarchy through a branch-leaf style design.

    Lookup of metrics within the hierarchy can be done in several ways.  First, through a single
    key constructed as a path assembled from the names of each metric, in the form
    *"/composite_child_metric_name/composite_grandchild_metric_name#metric_name"*,
    where the '#' is used to indicate the final component is a core `Metric` and not composite
    (otherwise the path separators are all '/').  The can also be referenced by dot-notation:
    given a composite root metric "*root*",
    *root.composite_child_metric_name.composite_grandchild_metric_name.metric_name*
    will yield the same result. The attributes are generated dymically, of course, so IDEs will not
    be able to statically validate code under this convention.

    >>> performance_metrics = CompositeMetric(name="Performance")
    ... overall_metric = CompositeMetric(name='overall_usage')
    ... performance_metrics.add(overall_metric)
    ... orverall_scpu_metric = Metric(name='system_cpu', value='2.1')
    ... orverall_ucpu_metric = Metric(name='user_cpu', value='89.0')
    ... overall_metric.add(overall_scpu_metric)
    ... overall_metric.add(overall_ucpu_metric)
    ... by_test_metrics = CompositeMetric(name='by_test')
    ... test1_metric = CompositeMetric(name='test1_usage')
    ... by_test_metrics.add(test1_metric)
    ... test1_ucpu_metric = Metric(name="user_cpu", value=88.2)
    ... test1_scpu_metric = Mertric(name="system_cpu", value=0.1)
    ... performance_metrics.flatten()
    ... {'Performance/overall_usage#system_cpu': 2.1,
    ...  'Performance/ovaerall_usage#user_cpu': 89.0
    ...  'Performacne/by_test/test1_usage#system_cpu': 0.1,
    ...  'Performance/by_test/test1_usage#user_cpu': 88.2
    ... }
    """

    value: Dict[str, BasicMetric]

    def __init__(self, name: str, values: Optional[Iterable[BasicMetric]] = None):
        if '#' in name or '/' in name:
            raise ValueError("Composite metric names cannot contain '#' or '/'")
        if not name:
            raise ValueError("Metric name cannot be empty")
        super().__init__(name, None)
        if values:
            self.value = {v.name: v for v in values}
            if len(self.value) != values:
                raise ValueError("Child metrics of a composite metric must have unique names")
        else:
            self.value: Dict[str, BasicMetric] = {}

    def __getitem__(self, key: str) -> BasicMetric:
        return self.element(key)

    def __delitem__(self, key: str):
        del self.value[key]

    def __getattr__(self, item):
        try:
            return self.value[item]
        except KeyError:
            raise AttributeError(f"No such attribute: {item}")

    def add(self, value: BasicMetric) -> None:
        """
        Add the given Metric of COmpositeMetric to this one

        :param value: metric to add to thise composite
        """
        self.value[value.name] = value

    def flatten(self, prefix: str = ""):
        """
        Flatten this metric into a simple set of key/value pairs with naming of keys based on
        a file-like hierarchical path

        :param prefix: only to be used internally
        :return: dict of key/value pairs represnting this composite metric
        """
        result: Dict[str, float] = {}
        path = '/'.join([prefix, self.name])
        for value in self.value.values():
            result.update(value.flatten(prefix=path))
        return result

    def keys(self, core_metrics_only: bool = False) -> Set[str]:
        return self._keys(core_metrics_only)

    def _keys(self, core_metrics_only: bool = False, root: str = "") -> Set[str]:
        """
        :param core_metrics_only: whether to return paths to only the core leaf metrics or the full set
        :return: the requested keys which are paths to the child elements of this composite
        """
        result: Set[str] = set()
        for key, value in self.value.items():
            if isinstance(value, Metric):
                # even if no root, include leading '#' since key might itself contain a '/' in the case
                # of a core Metric
                result.add('#'.join([root, key]))
            else:
                new_root = key if not root else '/'.join([root, key])
                if not core_metrics_only:
                    result.add(new_root)
                result.update(value._keys(core_metrics_only, root=new_root))
        return result

    def element(self, key_path: str) -> BasicMetric:
        """
        :param key_path: a path-like key to a sub-metric of this composite.  The path is relative
          to this metric (i.e., should not start with '/' nor contain the root name of this metric)
        :return: requested element or None if it does not exist
        :raises: KeyError if key is not in a proper path-like format
        """
        if key_path.startswith('/'):
            raise KeyError("key path must be relative and not start with '/'")
        if '#' in key_path:
            if key_path.startswith('#'):
                path, metric_name = "", key_path[1:]
            else:
                try:
                    path, metric_name = key_path.split('#')
                except ValueError:
                    raise KeyError("Path must contain at most one '#'")
        else:
            path = key_path
            metric_name = None

        elements = path.split('/') if path else []
        child = self
        try:
            for element in elements:
                child = child.value[element]
                if not isinstance(child, CompositeMetric):
                    raise KeyError("key-path not found for this metric.  A non-composite metric found where"
                                   " a composite was expected")
            if metric_name:
                child = child.value[metric_name]
        except KeyError:
            raise KeyError(f"{key_path} not found in this composite metric")
        return child


class MetricStore(AbstractContextManager):
    """
    Abstract ContextManager class defining interface for storing, retrieving and purging values
    from a data store
    """

    @abstractmethod
    def __enter__(self) -> "MetricStore":
        """
        Enter context of data store, "entering a session"
        :return: self
        """

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()

    @abstractmethod
    def purge_by_date(self, before: datetime.datetime, name: Optional[str] = None):
        """
        purge metrics with timestamp before the given date
        
        :param before: entries before this date will be removed from the data store
        :param name: if specified, only remove older metrics with the given name, otherwise
           remove all older metrics
        """
        
    @abstractmethod
    def purge_by_volume(self, count: int, name: str):
        """
        Remove metrics from the store if the count of a metric with same name exceeds
        the count given, removing the oldest items
        
        :param count: The maximum number of the set of metrics with the same name to keep
        :param name: if specified, only purge metrics with that name, otherwise apply to all 
           subsets of metrics with the same name
        """
        
    @abstractmethod
    def post(self, metric: CompositeMetric, timestamp: datetime.datetime=datetime.datetime.utcnow(),
             project_name: Optional[str] =None,
             uuid: Optional[str] = None):
        """
        Post the given metric to this data store 
        
        :param metric: the metric to post 
        :param timestamp: the timestamp of the metric
        :param project_name: if specified, the project name associated with the metric
        :param uuid: if specified, a unique id associated with the metric that can be used to
           correlate to other external data
        """
        
    @abstractmethod
    def metrics_by_date(self, metric_name: str, oldest: datetime.datetime,
                        newest: datetime.datetime = datetime.datetime.utcnow())\
            -> Union[List[CompositeMetric], List[Metric]]:
        """        
        :param metric_name: name of metric to retrieve
        :param oldest: only retrieve values after this date
        :param newest: only retrieve values before this data, or up until the most recent if unspecified
        :param with_metadata: whether to return metadata with each metric value
        :return: the set of CompositeMetric or Metric values associated with name over a range of dates,
            sorted by date from newest to oldest
        """
        
    @abstractmethod
    def metrics_by_volume(self, metric_name: str, count: int)-> Union[List[CompositeMetric], List[Metric]]:
        """
        :param metric_name: name of metric to retrieve
        :param count: max number of metric values to return
        :param with_metadata: whether to return metadata with each metric value
        :return: at most count more recent values of metric with given name, sorted by date from newest to oldest
        """

    @abstractmethod
    def commit(self):
        """
        Explicit commit of all buffered changes
        """

    class Comparison(Enum):
        EQUAL = "=="
        NOT_EQUAL = "<>"
        LESS_THAN = "<"
        GREATER_THAN = ">"
        LESS_THAN_OR_EQUAL = "<="
        GREATER_THAN_OR_EQUAL = ">="

    def filter_on_metadata(self, name:str, value: Union[str, int, float],
                           operation: "MetricStore.Comparison" = Comparison.EQUAL) -> "MetricStore":
        """
        Filter data on a specific metadata value
        :param name:  which metatdata
        :param value: value to compare to
        :param operation: type of comparison to make
        :return: self
        """
