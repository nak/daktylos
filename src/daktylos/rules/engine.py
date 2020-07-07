"""
This package contains the logic for creating and applying validation rules for `CompositeMetric`'s
"""

import fnmatch
from enum import Enum
from pathlib import Path
from typing import List, Optional, Iterator, Iterable, Set

import yaml

from daktylos.data import CompositeMetric
from daktylos.rules.status import ValidationStatus


class Rule:
    """
    A single rule than can be applied to any given composite metric
    Rules are based on the string-based hiearchical name that can be found from the result of
    calling `CompositMetric.flattned()`

    :param pattern:  a file-name like pattern used to match the key from a "flattened" `CompositeMetric`
    :param operation: a `Rule.Evalutaion` enum indicating the type of simple comparison to make
    :param limiting_value: the float value representing a therhold/limiting value for the metric
    """
    class Evaluation(Enum):
        LESS_THAN = "<"
        GREATER_THAN = ">"
        LESS_THAN_OR_EQUAL = "<="
        GREATER_THAN_OR_EQUAL = ">="

        @classmethod
        def from_string(cls, op: str):
            return {'<': cls.LESS_THAN,
                    '>': cls.GREATER_THAN,
                    '<=': cls.LESS_THAN_OR_EQUAL,
                    '>=': cls.GREATER_THAN_OR_EQUAL}[op]

    class ThresholdViolation(ValueError):
        """
        Exception raised on failure to validate a composite metric's children against this rule

        :param msg: exception message
        :param parent: The parent composite metric that contains violations of this rule
        :param offending_element: the key-paths to the core `Metric` (containing the values) that violated thresholds
        """

        def __init__(self, msg: str, parent: CompositeMetric, offending_elements: Iterable[str]):
            super().__init__(msg)
            self._parent_metric = parent
            self._offending_elements = offending_elements

        @property
        def parent_metric(self):
            return self._parent_metric

        @property
        def offending_elements(self):
            return self._offending_elements

    def __init__(self, pattern: str, operation: "Rule.Evaluation", limiting_value: float,
                 description: Optional[str] = None):
        self._pattern = pattern
        self._operation = operation
        self._limit = limiting_value
        self._description = description or f"{self._pattern} {self._operation.value} {self._limit}"

    @property
    def description(self):
        return self._description

    def validate(self, composite_metric: CompositeMetric, exclusions: Optional[Iterable[str]] = None) -> None:
        """
        Valide a composite metric for any and all matching key-names for each of its components

        :param composite_metric: the `CompositMetric` to validate
        :raises: ValueError with a message containing the rules violated if the metric fails to validate
           against this rule
        """
        failed_elements: List[str] = []
        msg = ""

        def apply_root(key: str) -> str:
            if not key.startswith('#'):
                return '/' + '/'.join([composite_metric.name, key])
            else:
                return '/' + ''.join([composite_metric.name, key])

        def excluded(key: str) -> bool:
            for exclusion in exclusions:
                if fnmatch.fnmatchcase(key, exclusion):
                    return True
            return False

        for key in [k for k in composite_metric.keys(core_metrics_only=True) if
                    not excluded(k) and self._pattern == '*'
                    or fnmatch.fnmatchcase(apply_root(k), self._pattern)]:
            if self._operation == Rule.Evaluation.LESS_THAN:
                if composite_metric[key].value >= self._limit:
                    msg += f"\n   {apply_root(key)} >= {self._limit}"
                    failed_elements.append(key)
            elif self._operation == Rule.Evaluation.GREATER_THAN:
                if composite_metric[key].value <= self._limit:
                    msg += f"\n   {apply_root(key)} <= {self._limit}"
                    failed_elements.append(key)
            elif self._operation == Rule.Evaluation.LESS_THAN_OR_EQUAL:
                if composite_metric[key].value > self._limit:
                    failed_elements.append(key)
                    msg += f"\n  {apply_root(key)} > {self._limit}"
            elif self._operation == Rule.Evaluation.GREATER_THAN_OR_EQUAL:
                if composite_metric[key].value < self._limit:
                    failed_elements.append(key)
                    msg += f"\n  {apply_root(key)} < {self._limit}"
        if failed_elements:
            raise Rule.ThresholdViolation(msg=msg,
                                          parent=composite_metric,
                                          offending_elements=failed_elements)


class RulesEngine:
    """
    An engine, i.e. a composed set of rules, to apply to given `CompositeMetric`s
    """

    def __init__(self):
        self._alerts: Set[Rule] = set()
        self._validations: Set[Rule] = set()
        self._exclusions: Set[str] = set()

    def add_alert(self, rule: Rule) -> None:
        """
        Add the given rule to this engine for reporting alerts

        :param rule: rule to add
        """
        self._alerts.add(rule)

    def add_validation(self, rule: Rule):
        """
        Add the given rule to this engine for reporting validation failures

        :param rule: rule to add
        """
        self._validations.add(rule)

    def add_exclusion(self, pattern: str):
        """
        Exclude metrics from rules if they match the given pattern
        :param pattern: a path-like object that indicates a relative path to a core metric within this composite
        """
        self._exclusions.add(pattern)

    def process(self, composite_metric: CompositeMetric) -> Iterator[ValidationStatus]:
        """
        Validate the composite metric aginst this rules engine

        :param composite_metric: metric to validate
        :returns: generator yielding alerts and validation failures as a list of `Status`
        """
        for rule in self._alerts:
            try:
                rule.validate(composite_metric, self._exclusions)
            except Rule.ThresholdViolation as e:
                failure = f"\n--------------------------------\nALERT: For rule '{rule.description}':\n{e}"
                yield ValidationStatus(level=ValidationStatus.Level.ALERT,
                                       text=failure,
                                       metric=composite_metric,
                                       offending_elements=e.offending_elements)
        for rule in self._validations:
            try:
                rule.validate(composite_metric, self._exclusions)
            except Rule.ThresholdViolation as e:
                failure = f"\n--------------------------------\nVALIDATION FAILURE: For rule '{rule.description}':\n {e}"
                yield ValidationStatus(level=ValidationStatus.Level.FAILURE,
                                       text=failure,
                                       metric=composite_metric,
                                       offending_elements=e.offending_elements)

    @classmethod
    def from_yaml_file(cls, path: Path) -> "RulesEngine":
        """
        :param path: a path to a yaml file to process for rules
        :return: a RulesEngine instance based on the content of the yaml file
        """
        if not path.exists() or path.is_dir():
            raise FileNotFoundError(f"Provided path '{path}' does not exit or is a directory")

        with open(path) as stream:
            document = yaml.load(stream)
            rules_engine = RulesEngine()

            def process_rule(action: str, rule: str):
                if action not in ['confirm', 'validate']:
                    raise ValueError(f"Invalid action specified: '{action}'")
                try:
                    pattern, operation, value = rule.split()
                    value = float(value)
                    operation = Rule.Evaluation.from_string(operation)
                except (ValueError, KeyError):
                    raise ValueError(f"Invalid rule specified in {path}.  Must be in format 'pattern [<, >, <=, >=] float-value:" +
                                     rule)
                rule_to_add = Rule(pattern, operation, value)
                if action == 'confirm':
                    rules_engine.add_alert(rule_to_add)
                else:
                    rules_engine.add_validation(rule_to_add)

            content = document.get('content', None)
            if not content:
                raise LookupError(f"Rules file {path} does not contain any top-level content element")
            try:
                for item in content:
                    ruleset = item.get('ruleset')
                    if not ruleset:
                        raise LookupError(f"Rules file {path} contains content devoid of ruleset elements")
                    description = ruleset.get('description', "<<none>>")
                    exclusions = ruleset.get('exclusions', [])
                    for exclusion in exclusions:
                        rules_engine.add_exclusion(exclusion['exclusion'])
                    rules = ruleset.get('rules', [])
                    if not rules:
                        raise LookupError(
                            f"Rules file {path} contains empty set of rules for set with description: {description}")
                    for rule in rules:
                        action = rule['action']
                        validation_rule = rule['rule']
                        process_rule(action, validation_rule)
            except KeyError:
                raise ValueError(f"Invalid rule in file {path}; it can only contain 'action' and 'rule' elements, but"
                                 f" got keys of {list(rule.keys())}")
            return rules_engine

