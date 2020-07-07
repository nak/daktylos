from enum import Enum
from typing import Iterable, Dict

from daktylos.data import CompositeMetric, Metric


class ValidationStatus:

    class Level(Enum):
        IMPROVEMENT = "improvement"
        ALERT = "alert"
        FAILURE = "failure"

    def __init__(self, level: "Violation.Level",
                 text: str,
                 metric: CompositeMetric,
                 offending_elements: Iterable[str]):
        self._level = level
        self._text = text
        self._parent_metric = metric
        self._offending_elements = offending_elements

    @property
    def text(self):
        return self._text

    @property
    def level(self):
        return self._level

    @property
    def parent_metric(self)-> CompositeMetric:
        return self._parent_metric

    def offending_metrics(self) -> Dict[str, Metric]:
        """
        :return: dictionary of key-path, `Metric` pairs that are the core metrics that failed validation
         (or showed improvement)
        """
        return {key: self._parent_metric for key in self._offending_elements}
