"""
The *daktylos.data* contains the classes used to define composite metrics, a hierarchical collection of floating point
metric values.  It also defines the interface for collection and retrieval of those values from a data store.


This module also provides the data abstraction for storing, retrieiving and purging metrics from an external
data store. A SQL implmementation can be found in :mod:`daktylos.data_stores.sql`.
"""

import datetime
import multiprocessing
import platform
import socket
from abc import abstractmethod, ABC
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from enum import Enum
from typing import (List, Dict, Optional, Iterable, Union, Set, TypeVar, Type, Generic)
try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol

__all__ = ["Metadata", "Metric", "CompositeMetric", "MetricStore", "MetricDataClass", "MDC", "Query", "QueryResult"]

# define convenience types for type hints and such:
number = Union[float, int]
metric_data_field = Union[
    number, "MetricDataClass", Dict[str, number], Dict[str, "MetricDataClass"],
    Optional[number], Optional["MetricDataClass"], Optional[Dict[str, number]],
    Optional[Dict[str, "MetricDataClass"]]
]
MDC = TypeVar('MDC')
# noinspection PyTypeChecker
MetricClass = TypeVar('MetricClass', bound='BasicMetric')


class MetricDataClass(Protocol):
    """
    Base class for composite metrics as dataclass.
    Inherit a @dataclass from this class so that mypy checks will ensure only float, int or recursive MetricDataClass
    elements are allowed

    Field types allowed are:
    #. int, float, str
    #. Any other @dataclass that follows these guidelines
    #. Dict[str, <any type from 1.) or 2.) or 3.)>]
    #. Optional value of any of 1.) 2.) or 3.) provided it has a default value

    >>> from dataclasses import dataclass
    ... from daktylos.data import MetricDataClass
    ...
    ... @dataclass
    ... class CodeCoverageMetricData(MetricDataClass):
    ...     total_cov: float
    ...     by_file: Dict[str, float]
    ...     by_package: Optional[Dict[str, float]] = None

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
        STRING = 0
        INTEGER = 1

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
        elif '#' in name:
            raise ValueError("Name cannot contain '#'")
        elif not name:
            raise ValueError("Metric name cannot be empty")
        super().__init__(name)
        self.value = value

    def flatten(self, prefix: str = "") -> Dict[str, number]:
        return {'#'.join([prefix, self.name]): self.value} if prefix else {self.name: self.value}

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
    Top-level (root) :class:`CompositeMetric` instances
    provide a general way for describing a related set of metric that makes it easy to serialize and deserialize
    the data for storage and evaluation.  When developing an API around
    specific composite metrics, the recommended practice is to subclass off of a :class:`CompositeMetric` class to
    provide a clean interface


    :param name: unique name of type of composit metric
    :param values: optional list of BasicMetric (either `Metric` or `CompositMetric` instances) to initially
       populate this metric

    >>> from daktylos.data import CompositeMetric
    ...
    ...  class TestRunPerformanceMetics(CompositeMetric):
    ...     def __init__(self, total_ucpu_secs: float, total_scpu_secs: float, total_duration: float,
    ...                  memory_consumed_mb: float):
    ...        super().__init__(self.__class__.__name__)
    ...        super().add_key_value("total_user_cpu", total_ucpu_secs))
    ...        super().add_key_value("total_system_cpu", total_scpu_secs))
    ...        super().add_key_value("total_duration", total_duration))
    ...        super().add_key_value("memory_consumed", memory_consumed_mb))
    ...        self._by_test: CompositeMetric = super().add(CompositeMetric("by_test"))
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


    Lookup of metrics within the hierarchy can be done in several ways.  First, through a single
    key constructed as a path assembled from the names of each metric, in the form
    *"/composite_child_metric_name/composite_grandchild_metric_name#metric_name"*.
    The path seaparator is '/' except for the final leaf where'#' is used as the separator.
    The element can also be referenced by dot-notation:
    given a composite root metric "*root*",
    *root.composite_child_metric_name.composite_grandchild_metric_name.metric_name*
    will yield the same result. The attributes are generated dymically, of course, so IDEs will not
    be able to statically validate code under this convention.

    >>> from daktylos.data import CompositeMetric, Metric
    ... performance_metrics = CompositeMetric(name="Performance")
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
    ... performance_metrics["/Performance/overallusage#system_cpu"]
    ... 2.1
    ... performance_metrics.overallusage.system_cpu
    ... 2.1
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
        """
        :param key: simple name of a metric to lookup in the immediate hierarchny

        :return: requested (direct) metric (may be composite or simple metric)
        """
        return self.element(key)

    def __delitem__(self, key: str):
        """
        Delete the direct child of this metric with the given key
        :param key: key to be deleted

        :raises: KeyError if key is not found in this metric
        """
        del self.value[key]

    def __getattr__(self, item: str) -> BasicMetric:
        """
        Allows for dot-notation in looking up item
        :param item: item to return from this metric object

        :return:  the metric instance sought
        :raises: AttributeError if this metric does not contain a metric with the name provided
        """
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

    def add(self, value: MetricClass) -> MetricClass:
        """
        Add the given Metric of CompositeMetric to this one

        :param value: metric to add to thise composite
        """
        self.value[value.name] = value
        return value

    def add_key_value(self, key: str, value: number) -> BasicMetric:
        """
        Add a simple key/value pair to this composite metric
        :param key: name of child metric to add
        :param value: float or int value for the simple metric to be addeed
        """
        return self.add(Metric(key, value))

    def flatten(self, prefix: str = "") -> Dict[str, number]:
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

    # noinspection PyProtectedMember
    def to_dataclass(self, typ: Type[Union[MetricDataClassT, Dict[str, metric_data_field]]]) -> \
            Union[MetricDataClassT, Dict[str, metric_data_field]]:
        """
        Convert to @dataclass MetricDataClass instance

        :param typ: The subclass of MetricDataClass to convert to
        :return: equivalent composit metrics instance of given type
        """
        kwds = {}
        if hasattr(typ, '_name') and 'Dict' == typ._name:
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
                        field_type = [a for a in field_type.__args__ if not isinstance(a, type(None))][0]
                    if field_type not in (float, int):
                        raise TypeError(f"Type mismatch in field {key} of {typ}: expected float or int but got "
                                        f"{typ.__dataclass_fields__[key].type}")
                    kwds[key] = value.value
                else:
                    field_type = typ.__dataclass_fields__[key].type
                    if hasattr(field_type, '__args__') and type(None) in field_type.__args__:
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


@dataclass
class QueryResult(Generic[MDC]):
    """
    Data class to hold list of timestamps, metadtaa and metric data
    """
    metadata: List[Optional[Metadata]] = field(default_factory=list)
    timestamps: List[datetime.datetime] = field(default_factory=list)
    metric_data: MDC = field(default_factory=list)


class Query(Generic[MDC]):
    """
    abstract base Query class

    :param metric_name: name of the compoiste metric to qeury
    :param max_count: max number of entries to query
    """

    def __init__(self, metric_name: str, max_count: Optional[int] = None):
        self._metric_name = metric_name
        self._count = max_count

    @property
    def count(self) -> Optional[int]:
        return self._count

    @abstractmethod
    def execute(self) -> "Union[QueryResult[List[MDC]], QueryResult[Dict[str, List[float]]]]":
        """
        Execute the query
        :return: list of Result from execution of the query
        """

    @abstractmethod
    def filter_on_date(self, oldest: datetime.datetime, newest: datetime.datetime) -> "Query[MDC]":
        """
        Filter results on date range
        :param oldest: oldest date
        :param newest: newest date
        :return: self
        """

    @abstractmethod
    def filter_on_metadata(self, **kwds) -> "Query[MDC]":
        """
        filter on metadata fields matching given keyword/value pairs
        :param kwds: keywords and values to filter on
        :return:  self
        """

    @abstractmethod
    def filter_on_metadata_field(self, name: str, value: int,
                                 op: "MetricStore.Comparison") -> "Query[MDC]":
        """
        filter query on metadata field with given name against provided value
        :param name: name of metadata field
        :param value: value to compare against
        :param op: type of comparison operation to perform
        :return: self
        """


class MetricStore(AbstractContextManager):
    """
    Context manager class defining interface for storing, retrieving and purging values
    from a data store
    """

    class Comparison(Enum):
        """
        Allowed types of comparisons
        """
        EQUAL = "=="
        NOT_EQUAL = "<>"
        LESS_THAN = "<"
        GREATER_THAN = ">"
        LESS_THAN_OR_EQUAL = "<="
        GREATER_THAN_OR_EQUAL = ">="

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
    def post(self, metric: CompositeMetric, timestamp: Optional[datetime.datetime] = None,
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
                  timestamp: Optional[datetime.datetime] = None,
                  metadata: Optional[Metadata] = None,
                  project_name: Optional[str] = None,
                  uuid: Optional[str] = None):
        """
        Post the given metric data (as a data class instance) to this data store

        :param metric_name: top-level name of metric
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
    def start_query(self, metric_name: str, max_results: Optional[int] = None) -> Query[CompositeMetric]:
        """
        :param metric_name: name of metric to query for
        :param max_results: optional max number of results to return
        :return: a Query[CompositeMetric] object used to construct a query and execute it
        """

    @abstractmethod
    def start_dataclass_query(self, typ: Type[MetricDataClass], metric_name: str, max_results: Optional[int])\
            -> Query[MetricDataClass]:
        """
        :param typ: dataclass type that hold a single composite metric value
        :param metric_name: name of metric to query for
        :param max_results: optional max number of results to return
        :return: a Query object used to construct a query and execute it

        :return: a Query[typ] object
        """
    @abstractmethod
    def start_field_query(self, metric_name: str, fields: Optional[List[str]], max_results: Optional[int] = None)\
            -> Query[Dict[str, List[float]]]:
        """
        :param metric_name: name of metric to query for
        :param fields: (wildcard) list of field names to filter on
        :param max_results: optional max number of results to return
        :return: a Query object used to construct a query and execute it

        :return: a Query[Dict[str, List[flost]]] object to embelish/execute
        """

    def composite_metrics_by_date(self, metric_name: str, oldest: datetime.datetime,
                                  newest: Optional[datetime.datetime] = None,
                                  metadata_filter: Optional[Dict[str, str]] = None) \
            -> "QueryResult[List[Union[CompositeMetric, Metric]]]":
        """
        :param metric_name: name of metric to retrieve
        :param oldest: only retrieve values after this date
        :param newest: only retrieve values before this data, or up until the most recent if unspecified
        :param metadata_filter: filter on key/value pairs that match associated metadata of the metrics (optional)
        :return: the set of CompositeMetric or Metric values associated with name over a range of dates,
            sorted by date from newest to oldest
        """
        newest = newest or datetime.datetime.utcnow()
        query = self.start_query(metric_name)
        query.filter_on_date(oldest=oldest, newest=newest)
        if metadata_filter:
            query.filter_on_metadata(**metadata_filter)
        return query.execute()

    def composite_metrics_by_volume(self, metric_name: str, count: int,
                                    metadata_filter: Optional[Dict[str, str]] = None) \
            -> "QueryResult[List[Union[CompositeMetric, Metric]]]":
        """
        :param metric_name: name of metric to retrieve
        :param count: max number of metric values to return
        :param metadata_filter: filter on key/value pairs that match associated metadata of the metrics (optional)

        :return: at most count more recent values of metric with given name, sorted by date from newest to oldest
        """
        query = self.start_query(metric_name, count)
        if metadata_filter:
            query.filter_on_metadata(**metadata_filter)
        return query.execute()

    def dataclass_metrics_by_date(self, name: str, typ: T, oldest: datetime.datetime,
                                  newest: Optional[datetime.datetime] = None,
                                  metadata_filter: Optional[Dict[str, str]] = None) -> "QueryResult[List[T]]":
        """
        query metric data by dataclass-style metric definition
        :param name: name of metric to query
        :param typ: dataclass type to store metrics values
        :param oldest: earliest date to search for
        :param newest: newest date to earch for, or undpsecified for all most recent
        :param metadata_filter: filter on optional metadata

        :return: results of query, ordered by timestamp
        """
        newest = newest or datetime.datetime.utcnow()
        items = self.composite_metrics_by_date(metric_name=name, oldest=oldest, newest=newest,
                                               metadata_filter=metadata_filter)
        items.metric_data = [metric.to_dataclass(typ) for metric in items.metric_data]
        return items

    def dataclass_metrics_by_volume(self, name: str, typ: T, count: int,
                                    metadata_filter: Optional[Dict[str, str]] = None) -> "QueryResult[List[T]]":
        """
        Return results of query stored in dataclass-style for given dataclass type T
        :param name: name of metric
        :param typ: dataclass used to store resulting data
        :param count: max count to return (most recent)
        :param metadata_filter: filter on optional metadata

        :return: QueryResult instance based on given dataclass type, ordered by timestamp
        """
        result = self.composite_metrics_by_volume(name, count=count, metadata_filter=metadata_filter)
        # transform CompositeMetric items to given dataclass type T
        result.metric_data = [metric.to_dataclass(typ) for metric in result.metric_data]
        return result

    def metric_fields_by_date(self, metric_name: str,
                              oldest: datetime.datetime, newest: Optional[datetime.datetime] = None,
                              fields: Optional[Iterable[str]] = None,
                              metadata_filter: Optional[Dict[str, str]] = None)\
            -> QueryResult[Dict[str, List[float]]]:
        """
        query and return metric data, timestamps and metadata for a given metric name based on date range and
        optional filter on metadata
        :param metric_name: name of metric to query
        :param fields: list of path-like fields (within a flattened metrics, e.g.), can be wildcarded
        :param oldest: oldest date to query for
        :param newest: optional newest date to query for, or latest if not specified
        :param metadata_filter: optional filter on metadata

        :return: list of QueryResults with requested data items
        """
        newest = newest or datetime.datetime.utcnow()
        query = self.start_field_query(metric_name=metric_name, fields=fields)
        query.filter_on_date(oldest=oldest, newest=newest)
        if metadata_filter:
            query.filter_on_metadata(**metadata_filter)
        return query.execute()

    def metric_fields_by_volume(self, metric_name: str, count: int,
                                fields: Optional[List[str]] = None,
                                metadata_filter: Optional[Dict[str, str]] = None)\
            -> "QueryResult[Dict[str, List[float]]]":
        """
         Return results of query stored ina per-field dictionary of data for all fields within a composite metric
        :param metric_name: name of metric
        :param fields: list of path-like fields (within a flattened metrics, e.g.), can be wildcarded
        :param count: max count to return (most recent)
        :param metadata_filter: filter on optional metadata

        :return: QueryResult based on dictionary of field-name to float-data-list mapping
        """
        query = self.start_field_query(metric_name=metric_name, max_results=count, fields=fields)
        if metadata_filter:
            query.filter_on_metadata(**metadata_filter)
        return query.execute()

    @abstractmethod
    def commit(self) -> None:
        """
        Explicit commit of all buffered changes
        """
