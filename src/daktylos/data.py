"""
This module contains the classes used to define composite metrics, a hierarchical collection of floating point metric
values.  It also defines the interface for collection and retrieval of those values from a data store.

Top-level (root) :class:`CompositeMetric` instances
provide a general way for describing complex, related metric sets that makes it easy to serialize and deserialize
the data for  storage and rules specifications.  However, when developing an API around
specific composite metrics, the recommended practice is to subclass off of a :class:`CompositeMetric` class to provide
a clean interface:

>>> from daktylos.data import CompositeMetric, MetricDataClass
... @dataclass(frozen=True)
... class SingleTestPerformanceData(MetricDataClass):
...    user_cpu: float
...    system_cpu: float
...    duration: float
...    memory_consumed: float
...
...
... @dataclass
... class TestRunPerofmrnaceData(MetricDataClass):
...    total_user_cpu: float
...    total_system_cpu: float
...    total_duration: float
...    total_memory_consumed_mb: float
...    by_test: Dict[str, SingleTestPerformanceData]
...
...    def add_test_performance(self, test_name: str, performance: SingleTestPerformanceData):
...        self.by_test[test_name] = performance
...
... test_run_performance = TestRunPerofmrnaceData(total_user_cpu=11.2, total_system_cpu=0.1, total_duration=14.0, \
...          total_memory_consumed=12.1)
... test_run_performance.add_test_performance("test1", performance=SingleTestPerformanceData(
...     user_cpu=1.2, system_cpu=0.01, duration=3.8, memory_consumed=.192))
... test_run_metrics = CompositeMetric.from_dataclass(test_run_performance)

or a purely programmatic approach:

>>>  class TestRunPerformanceMetics(CompositeMetric):
...     def __init__(self, total_ucpu_secs: float, total_scpu_secs: float, total_duration: float,
...                  memory_consumed_mb: float):
...        super().__init__(self.__class__.__name__)
...        super().add_key_value("total_user_cpu", total_ucpu_secs))
...        super().add_key_value("total_system_cpu", total_scpu_secs))
...        super().add_key_value("total_duration", total_duration))
...        super().add_key_value("memory_consumed", memory_consumed_mb))
...        self._by_test = super().add(CompositeMetric("by_test"))
...
...     def add_test_performance(self, test_name: str, test_ucpu: float,
...                              test_scpu: float, duration: float, test_memory_consumed: float):
...         test_metrics = CompositeMetric(test_name)
...         test_metrics.add_key_value("user_cpu", test_ucpu))
...         test_metrics.add_key_value("user_spu", test_scpu))
...         test_metrics.add_key_value("duration", duration))
...         test_metrics.add_key_value("memory_consumed", test_memory_consumed))
...         self._by_test.add(test_metrics)
...
...  test_run_metrics = TestRunPerformanceMetics(total_ucpu_secs=11.2, total_scpu_secs=1.2, total_duration=12.3,
...     memory_consumed_mb=12.2)
...  test_run_metrics.add_test_performance("test1", test_ucpu=2.3, test_scpu=0.1, test_memory_consumed=0.191,
...     duration=2.9)


This class provides a target-specific interface for performance metrics for a test run, without the
client having to know the details of the naming and hierarchy of the underlying :class:`CompositeMetric`

This module also provides the data abstraction for storing, retrieiving and purging metrics from an external
data store. A SQL implmementation can be found in :mod:`daktylos.data_stores.sql`.
"""

import datetime
import multiprocessing
import platform
import socket
from abc import abstractmethod, ABC
from contextlib import AbstractContextManager
from dataclasses import dataclass
from enum import Enum
from typing import (List, Dict, Optional, Iterable, Union, Set, TypeVar, Type)
try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol

__all__ = ["Metadata", "Metric", "CompositeMetric", "MetricStore", "MetricDataClass"]

number = Union[float, int]
metric_data_field = Union[
    number, "MetricDataClass", Dict[str, number], Dict[str, "MetricDataClass"],
    Optional[number], Optional["MetricDataClass"], Optional[Dict[str, number]],
    Optional[Dict[str, "MetricDataClass"]]
]


class MetricDataClass(Protocol):
    """
    Inherit a @dataclass from this class so that mypy checks will ensure only float, int or recursive MetricDataClass
    elements are allowed
    """
    __dataclass_fields__: Dict[str, metric_data_field]


MetricDataClassT = TypeVar('MetricDataClassT', bound=MetricDataClass)
T = TypeVar('T')


@dataclass
class Metadata:
    """
    Informational data associated with a (top-level) composite metric
    """

    class Types(Enum):
        """
        Allowed types of metadata
        """
        STRING = 'str'
        INTEGER = 'int'

    values: Dict[str, Union[str, int]]
    """
    The metadata key/value pairs
    """

    # noinspection PyBroadException
    @staticmethod
    def system_info() -> "Metadata":
        """
        :return: a standard set of metadata describing the host system
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
    """
    Base class for simple and composite metrics classes
    """
    name: str

    @abstractmethod
    def flatten(self, prefix: str = "") -> Dict[str, number]:
        """
        Flatten the hierarchy of metrics to a simple Dict

        :param prefix: only to be used internally
        :return: dict of string-path/number pairs
        """

    @classmethod
    def from_flattened(cls, values: Dict[str, number]) -> "BasicMetric":
        """
        Return a metric object from the given str/number value pairs.
        The names must match a specific convention, and this method is only
        meant for use in "unflattening" metrics that have been produced
        from the "flatten" method (either explicitly or restored from a datastore)

        :param values: a set of string-value pairs that representes the metric
        :return: Metric or CompositeMetric equivalent of the given names and values
        :raises ValueError: if values is not conformant with a flattened metric expectation


        >>> from daktylos.data import CompositeMetric, MetricDataClass
        ... # define metrics classes and create a composite metric instance
        ... @dataclass(frozen=True)
        ... class SingleTestPerformanceData(MetricDataClass):
        ...    user_cpu: float
        ...    system_cpu: float
        ...    duration: float
        ...    memory_consumed: float
        ...
        ...
        ... @dataclass
        ... class TestRunPerofmrnaceData(MetricDataClass):
        ...    total_user_cpu: float
        ...    total_system_cpu: float
        ...    total_duration: float
        ...    total_memory_consumed_mb: float
        ...    by_test: Dict[str, SingleTestPerformanceData]
        ...
        ...    def add_test_performance(self, test_name: str, performance: SingleTestPerformanceData):
        ...        self.by_test[test_name] = performance
        ...
        ... test_run_performance = TestRunPerofmrnaceData(total_user_cpu=11.2, total_system_cpu=0.1,
        ...          total_duration=14.0, total_memory_consumed=12.1)
        ... test_run_performance.add_test_performance("test1", performance=SingleTestPerformanceData(
        ...     user_cpu=1.2, system_cpu=0.01, duration=3.8, memory_consumed=.192))
        ... # now flatten then unflatten and validate no effect:
        ... test_run_metrics = CompositeMetric.from_dataclass(test_run_performance)
        ... test_run_metrics_dict = test_run_metrics.flatten()
        ... assert BasicMetric.from_flattened(test_run_metrics_dict) == test_run_metrics
        """
        if len(values) == 0:
            raise ValueError("Empty value set when constructing Metric")
        # if only one element, make a single simple metric
        if len(values.values()) == 1 and type(list(values.values())[0]) in [float, int]:
            name = list(values.keys())[0]
            if '#' in name:
                raise ValueError("Metric names must not contain a '#'")
            return Metric(name=name, value=list(values.values())[0])  # TODO: in 3.8 can use walrus operator in if

        # otherwise process a composite metric...
        root: Optional[BasicMetric] = None

        def process(path_: str, value_: number):
            """
            Process a path/value pair and place in the hierarchy of the root metric

            :param path_: string path of the element
            :param value_: value of the element
            """
            nonlocal root
            if '#' not in path_:
                raise ValueError("Composite metric path must contain one '#' element")
            location, name_ = path_.split('#')
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

            if name_ in base.value:
                raise ValueError("Mixed composite and leaf nodes at same level")
            base.add(Metric(name=name_, value=value_))

        for path, value in values.items():
            process(path, value)

        if not root:
            raise ValueError("No data to process")
        return root

    @classmethod
    def from_dataclass(cls, name: str, values: Union[number, MetricDataClass]):
        """
        Create a metric from the given data class
        :param name: the name of the metric class instance created
        :param values: either a :class:`MetricDataClass` of composite values or a single number value

        :return: a Metric instance if a single number value provided, otherwise a CompositeMetric containing the values
          of the :class:`MetricDataClass` instance provided.
        """
        if isinstance(values, (int, float)):
            return Metric(name, values)
        if isinstance(values, dict):
            composite = CompositeMetric(name)
            for k, v in values.items():
                composite.add(cls.from_dataclass(k, v))
            return composite
        if not hasattr(values, "__dataclass_fields__") or not hasattr(values, "__annotations__"):
            raise TypeError("values provided do not represent a flat, int, dict or data class as expected")
        elif len(values.__dataclass_fields__) == 0:
            raise ValueError("Supplied metrics data class is empty")
        else:
            composite = CompositeMetric(name)
            for k in values.__annotations__.keys():
                val = getattr(values, k)
                if isinstance(val, float) or isinstance(val, int):
                    composite.add_key_value(k, val)
                else:
                    composite.add(cls.from_dataclass(k, val))
            return composite

    @abstractmethod
    def __eq__(self, other: "BasicMetric"):
        """
        Compare two metrics for equality
        :param other: metrics to compare to
        :return: wether metric values are the same by name and all elements are identical in value
        """


@dataclass
class Metric(BasicMetric):
    """
    Leaf class defining an actual key/value pair

    :param name: name of the metric
    :param value: numeric value for the metric (int or float value)
    """
    value: number

    def __init__(self, name: str, value: number):
        if not isinstance(value, (int, float)):
            raise ValueError("Metric values must be numbers")
        if '#' in name:
            raise ValueError("Name cannot contain '#'")
        if not name:
            raise ValueError("Metric name cannot be empty")
        super().__init__(name)
        self.value = value

    def flatten(self, prefix: str = "") -> Dict[str, number]:
        if prefix:
            return {'#'.join([prefix, self.name]): self.value}
        else:
            return {self.name: self.value}

    def __eq__(self, other: BasicMetric):
        if self is other:
            return True
        if not isinstance(other, Metric):
            return False
        # quicker implementation
        return self.name == other.name and self.value == other.value


@dataclass
class CompositeMetric(BasicMetric):
    """
    CompositMetric comprising a collection of other :class:`Metric` or :class:`CompositeMetric` instances.
    The collection of metrics is contained in a hierarchy through a branch-leaf style design.

    Lookup of metrics within the hierarchy can be done in several ways.  First, through a single
    key constructed as a path assembled from the names of each metric, in the form
    *"/composite_child_metric_name/composite_grandchild_metric_name#metric_name"*,
    where the '#' is used to indicate the final component is a core :class:`Metric` and not composite
    (otherwise the path separators are all '/').  The can also be referenced by dot-notation:
    given a composite root metric "*root*",
    *root.composite_child_metric_name.composite_grandchild_metric_name.metric_name*
    will yield the same result. The attributes are generated dymically, of course, so IDEs will not
    be able to statically validate code under this convention.

    >>> performance_metrics = CompositeMetric(name="Performance")
    ... overall_metric = CompositeMetric(name='overall_usage')
    ... performance_metrics.add(overall_metric)
    ... overall_scpu_metric = Metric(name='system_cpu', value=2.1)
    ... overall_ucpu_metric = Metric(name='user_cpu', value=89.0)
    ... overall_metric.add(overall_scpu_metric)
    ... overall_metric.add(overall_ucpu_metric)
    ... by_test_metrics = CompositeMetric(name='by_test')
    ... test1_metric = CompositeMetric(name='test1_usage')
    ... by_test_metrics.add(test1_metric)
    ... test1_ucpu_metric = Metric(name="user_cpu", value=88.2)
    ... test1_scpu_metric = Metric(name="system_cpu", value=0.1)
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
        super().__init__(name)
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

    def __eq__(self, other: "BasicMetric"):
        if self is other:
            return True
        if not isinstance(other, CompositeMetric) or self.name != other.name:
            return False
        for k, v in self.value. items():
            if v != other.value.get(k):
                return False
        return True

    def add(self, value: BasicMetric) -> BasicMetric:
        """
        Add the given Metric of COmpositeMetric to this one

        :param value: metric to add to thise composite
        """
        self.value[value.name] = value
        return value

    def add_key_value(self, key: str, value: number):
        return self.add(Metric(key, value))

    def flatten(self, prefix: str = ""):
        result: Dict[str, number] = {}
        path = '/'.join([prefix, self.name])
        for value in self.value.values():
            result.update(value.flatten(prefix=path))
        return result

    def keys(self, core_metrics_only: bool = False) -> Set[str]:
        """
        :param core_metrics_only:  whether to return keys of only the immediate children or the entire
           hierarchy of contained metrics
        :return: requested keys for this metric
        """
        return self._keys(core_metrics_only)

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
        elif '/' not in key_path:
            return self.value[key_path]
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

    def to_dataclass(self, typ: Type[Union[MetricDataClassT, Dict[str, metric_data_field]]])\
        -> Union[MetricDataClassT, Dict[str, metric_data_field]]:
        """
        Convert to @dataclass MetricDataClass instance

        :param typ: The subclass of MetricDataClass to convert to
        :return: equivalent composit metrics instance of given type
        """
        kwds = {}
        if hasattr(typ, '_name') and typ._name == 'Dict':
            # process a dictionary of fixed type value and return
            field_type = typ.__args__[1]
            for key, value in self.value.items():
                if field_type in (int, float):
                    if not isinstance(value, Metric):
                        raise TypeError(f"Expected simple metric but found composite for field {key}")
                    kwds[key] = field_type(value.value)
                elif isinstance(value, CompositeMetric):
                    kwds[key] = value.to_dataclass(field_type)
                else:
                    raise TypeError(f"Invalid type for field {key}: {field_type}")
            return kwds
        else:
            # return dataclass instantiation
            for key, value in self.value.items():
                if key not in typ.__dataclass_fields__:
                    raise ValueError(f"Given dataclass type {typ} has no field named {key}")
                if isinstance(value, Metric):
                    field_type = typ.__dataclass_fields__[key].type
                    if hasattr(field_type, '__args__') and type(None) in field_type.__args__:
                        field_type = [a for a in field_type.__args__ if a is not type(None)][0]
                    if not field_type in (float, int):
                        raise TypeError(f"Type mismatch in field {key} of {typ}: expected float or int but got "
                                        f"{typ.__dataclass_fields__[key].type}")
                    kwds[key] = value.value
                else:
                    field_type = typ.__dataclass_fields__[key].type
                    if hasattr(field_type, '__args__') and  type(None) in field_type.__args__:
                        field_type = [a for a in field_type.__args__ if a is not type(None)][0]
                    if not isinstance(value, CompositeMetric):
                        raise TypeError(f"Type of composite metrics field named '{key}' is not composite as expected "
                                        f"by {typ}")
                    if hasattr(field_type, '__dataclass_fields__') or hasattr(field_type, '__args__'):
                        kwds[key] = value.to_dataclass(field_type)
                    else:
                        raise TypeError(f"Type of field {key} in {typ} is invalid: {field_type}")
            return typ(**kwds)

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
            elif isinstance(value, CompositeMetric):
                new_root = key if not root else '/'.join([root, key])
                if not core_metrics_only:
                    result.add(new_root)
                result.update(value._keys(core_metrics_only, root=new_root))
            else:
                raise TypeError("Child not of expected type of Metric or CompositeMetric")
        return result


class MetricStore(AbstractContextManager):
    """
    Abstract :class:`ContextManager` class defining interface for storing, retrieving and purging values
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
    def post(self, metric: CompositeMetric, timestamp: datetime.datetime = datetime.datetime.utcnow(),
             metadata: Optional[Metadata] = None,
             project_name: Optional[str] = None,
             uuid: Optional[str] = None):
        """
        Post the given metric to this data store 
        
        :param metric: the metric to post 
        :param timestamp: the timestamp of the metric
        :param metadata: optional metadata associated with metric
        :param project_name: if specified, the project name associated with the metric
        :param uuid: if specified, a unique id associated with the metric that can be used to
           correlate to other external data
        """

    def post_data(self,
                  metric_name: str,
                  metric_data: MetricDataClass,
                  timestamp: datetime.datetime=datetime.datetime.utcnow(),
                  metadata: Optional[Metadata] = None,
                  project_name: Optional[str] =None,
                  uuid: Optional[str] = None):
        """
        Post the given metric data (as a data class instance) to this data store

        :param metric_data: the metric data (in a dataclass instance) to post
        :param timestamp: the timestamp of the metric
        :param metadata: optional metadata associated with metric
        :param project_name: if specified, the project name associated with the metric
        :param uuid: if specified, a unique id associated with the metric that can be used to
           correlate to other external data
        """
        metric = BasicMetric.from_dataclass(metric_name, metric_data)
        self.post(metric, timestamp=timestamp, project_name=project_name, uuid=uuid, metadata=metadata)
        
    @abstractmethod
    def metrics_by_date(self, metric_name: str, oldest: datetime.datetime,
                        newest: datetime.datetime = datetime.datetime.utcnow())\
            -> Union[List[CompositeMetric], List[Metric]]:
        """        
        :param metric_name: name of metric to retrieve
        :param oldest: only retrieve values after this date
        :param newest: only retrieve values before this data, or up until the most recent if unspecified
        :return: the set of CompositeMetric or Metric values associated with name over a range of dates,
            sorted by date from newest to oldest
        """
        
    @abstractmethod
    def metrics_by_volume(self, metric_name: str, count: int) -> Union[List[CompositeMetric], List[Metric]]:
        """
        :param metric_name: name of metric to retrieve
        :param count: max number of metric values to return
        :return: at most count more recent values of metric with given name, sorted by date from newest to oldest
        """

    def metric_data_by_date(self, name: str, typ: T,  oldest: datetime.datetime,
                            newest: datetime.datetime = datetime.datetime.utcnow()) -> List[T]:
        items = self.metrics_by_date(name, oldet=oldest, newest=newest)
        return [metric.to_dataclass(typ) for metric in items]

    def metric_data_by_volume(self, name: str, typ: T, count: int) -> List[T]:
        items = self.metrics_by_volume(name, count=count)
        return [metric.to_dataclass(typ) for metric in items]

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

    @abstractmethod
    def filter_on_metadata(self, name: str, value: Union[str, int],
                           operation: "MetricStore.Comparison" = Comparison.EQUAL) -> "MetricStore":
        """
        Filter data on a specific metadata value
        :param name:  which metadata
        :param value: value to compare to
        :param operation: type of comparison to make
        :return: self
        """