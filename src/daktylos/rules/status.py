"""
Status of rules applied across a composite metrics is provided to clients at two levels: alert or failure.
Alerts are warnings to the client,
signalled when based on user rules definitions, a metric value is outside of the defined threshold for
the rule -- as an indication that the metric has not yet crossed a failure point but is close.  Failures
are signalled when a metric value has crossed a user-defined threshold that puts it out of bounds of
acceptable criteria.
"""

from enum import Enum
from typing import Iterable, Dict

from daktylos.data import CompositeMetric, Metric


class ValidationStatus:
    """
    A status provided to the client, as an alert or failure on a metric.

    :param level: The level of the status provided (Level.ALERT or Level.FAILURE)
    :param text: A message to provide to the client on the nature of the alert or failure
    :param metric:  The associated root composite metric that failed
    """

    class Level(Enum):
        """
        Types of status that can be provided to client
        """
        ALERT = "alert"
        FAILURE = "failure"

    def __init__(self, level: "ValidationStatus.Level",
                 text: str,
                 metric: CompositeMetric,
                 offending_elements: Iterable[str]):
        self._level = level
        self._text = text
        self._parent_metric = metric
        self._offending_elements = offending_elements

    @property
    def text(self):
        """
        :return: The text message indicating the details of this alert or failure
        """
        return self._text

    @property
    def level(self):
        """
        :return: The level of this status (Level.ALERT or Level.FAILURE)
        """
        return self._level

    @property
    def parent_metric(self)-> CompositeMetric:
        """
        :return: The metric associated with this status
        """
        return self._parent_metric

    def offending_metrics(self) -> Dict[str, Metric]:
        """
        :return: dictionary of key-path, `Metric` pairs that are the core metrics that failed validation
         (or showed improvement)
        """
        return {key: self._parent_metric for key in self._offending_elements}
