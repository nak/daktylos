"""
The `daktylos.rules.engine` package contains the logic for creating and applying validation rules across
the values contained in a `CompositeMetric`.
"""

import fnmatch
from enum import Enum
from pathlib import Path
from typing import List, Optional, Iterable, Set

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
    :param is_relative: whether the rule applies to a comparison (difference with) the previous metric
        value or is absolute, evaluating only against the latest value
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
        :param offending_elements: the key-paths to the core `Metric` (containing the values) that violated thresholds
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
                 description: Optional[str] = None, is_relative: bool = False):
        self._pattern = pattern
        self._operation = operation
        self._limit = limiting_value
        self._description = description or f"{self._pattern} {self._operation.value} {self._limit}"
        self._is_relative = is_relative

    @property
    def description(self):
        """
        :return: The description of this rule
        """
        return self._description

    @staticmethod
    def _prepend_root(key: str, composite_metric: CompositeMetric) -> str:
        if not key.startswith('#'):
            return '/' + '/'.join([composite_metric.name, key])
        else:
            return '/' + ''.join([composite_metric.name, key])

    @staticmethod
    def _excluded(key: str, exclusions: Optional[Iterable[str]]) -> bool:
        """
        :param key: to test
        :param exclusions: list of pattern exclusions
        :return: whether key pattern provided matches any pattern exclusion list
        """
        for exclusion in exclusions or []:
            if fnmatch.fnmatchcase(key, exclusion):
                return True
        return False

    def validate(self, composite_metric: CompositeMetric, previous_metric: Optional[CompositeMetric] = None,
                 exclusions: Optional[Iterable[str]] = None) -> None:
        """
        Valide a composite metric for any and all matching key-names for each of its components

        :param composite_metric: the `CompositMetric` to validate
        :param previous_metric: previous value of the metric (for relative rule) or None if N/A or non-existent
        :raises: ValueError with a message containing the rules violated if the metric fails to validate
           against this rule
        """
        failed_elements: List[str] = []
        msg = ""

        for key in [k for k in composite_metric.keys(core_metrics_only=True) if
                    not self._excluded(self._prepend_root(k, composite_metric), exclusions) and
                    (self._pattern == '*'
                     or fnmatch.fnmatchcase(self._prepend_root(k, composite_metric), self._pattern))]:
            if self._is_relative:
                if not previous_metric:
                    continue
                try:
                    value = composite_metric[key].value - previous_metric[key].value
                except KeyError:
                    continue  # prev metric does not contain this key, so nothing to compare to
            else:
                value = composite_metric[key].value
            if self._operation == Rule.Evaluation.LESS_THAN:
                if value >= self._limit:
                    msg += f"\n   {self._prepend_root(key, composite_metric)} >= {self._limit}"
                    failed_elements.append(key)
            elif self._operation == Rule.Evaluation.GREATER_THAN:
                if value <= self._limit:
                    msg += f"\n   {self._prepend_root(key, composite_metric)} <= {self._limit}"
                    failed_elements.append(key)
            elif self._operation == Rule.Evaluation.LESS_THAN_OR_EQUAL:
                if value > self._limit:
                    failed_elements.append(key)
                    msg += f"\n  {self._prepend_root(key, composite_metric)} > {self._limit}"
            elif self._operation == Rule.Evaluation.GREATER_THAN_OR_EQUAL:
                if value < self._limit:
                    failed_elements.append(key)
                    msg += f"\n  {self._prepend_root(key, composite_metric)} < {self._limit}"
        if failed_elements:
            raise Rule.ThresholdViolation(msg=msg,
                                          parent=composite_metric,
                                          offending_elements=failed_elements)


class RulesEngine:
    """
    An engine, i.e. a composed set of rules, to apply to given `CompositeMetric`s
    """

    class RuleSet:

        def __init__(self):
            self._alerts: Set[Rule] = set()
            self._validations: Set[Rule] = set()
            self._exclusions: Set[str] = set()

        def add_validation(self, rule: Rule):
            """
            Add the given rule to this engine for reporting validation failures

            :param rule: rule to add
            """
            self._validations.add(rule)

        def add_alert(self, rule: Rule):
            """
            Add the given rule to this engine for reporting alerts

            :param rule: rule to add
            """
            self._alerts.add(rule)

        def add_exclusion(self, exclusion: str):
            """
            Exclude metrics from rules if they match the given pattern
            :param pattern: a path-like object that indicates a relative path to a core metric within this composite
            """
            self._exclusions.add(exclusion)

        def process(self, composite_metric: CompositeMetric, previous_metric: Optional[CompositeMetric] = None):
            """
            Validate the composite metric aginst this rules engine

            :param composite_metric: metric to validate
            :returns: generator yielding alerts and validation failures as a list of `Status`
            """
            for rule in self._alerts:

                try:
                    rule.validate(composite_metric, previous_metric, self._exclusions)
                except Rule.ThresholdViolation as e:
                    failure = f"\n--------------------------------\nALERT: For rule '{rule.description}':\n{e}"
                    yield ValidationStatus(level=ValidationStatus.Level.ALERT,
                                           text=failure,
                                           metric=composite_metric,
                                           offending_elements=e.offending_elements)
            for rule in self._validations:
                try:
                    rule.validate(composite_metric, previous_metric, self._exclusions)
                except Rule.ThresholdViolation as e:
                    failure = \
                        f"\n--------------------------------\nVALIDATION FAILURE: For rule '{rule.description}':\n {e}"
                    yield ValidationStatus(level=ValidationStatus.Level.FAILURE,
                                           text=failure,
                                           metric=composite_metric,
                                           offending_elements=e.offending_elements)

    def __init__(self):
        self._rulesets: Set["RulesEngine.RuleSet"] = set()

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

            def process_rule(ruleset: cls.RuleSet, action: str, rule: str):
                if action not in ['confirm', 'validate']:
                    raise ValueError(f"Invalid action specified: '{action}'")
                try:
                    pattern, operation, value = rule.split()
                    value = float(value)
                    operation = Rule.Evaluation.from_string(operation)
                except (ValueError, KeyError):
                    raise ValueError(
                        f"Invalid rule specified in {path}.  Must be in format 'pattern [<, >, <=, >=] float-value:" +
                        rule)
                pattern = pattern.strip()
                is_relative = False
                if pattern.startswith("delta("):
                    if not pattern.endswith(')'):
                        raise ValueError("Invalid construct in delta() clause: missing end parenthesis")
                    pattern = pattern[6:-1]
                    is_relative = True
                rule_to_add = Rule(pattern, operation, value, is_relative=is_relative)
                if action == 'confirm':
                    ruleset.add_alert(rule_to_add)
                else:
                    ruleset.add_validation(rule_to_add)

            content = document.get('content', None)
            if not content:
                raise LookupError(f"Rules file {path} does not contain any top-level content element")
            try:
                for item in content:
                    if len(item) == 0:
                        raise LookupError(f"Rules file {path} contains content devoid of ruleset elements")
                    elif len(item) != 1:
                        raise LookupError(f"Rules file {path} contains more elements then expected: {list(item.keys())}")
                    ruleset_defn = item.get('ruleset')
                    if not ruleset_defn:
                        raise LookupError(f"Rules file {path} contains content devoid of ruleset elements")
                    description = ruleset_defn.get('description', "<<none>>")
                    exclusions = ruleset_defn.get('exclusions', [])
                    ruleset = cls.RuleSet()
                    rules_engine._rulesets.add(ruleset)
                    for exclusion in exclusions:
                        ruleset.add_exclusion(exclusion['exclusion'])
                    rules = ruleset_defn.get('rules', [])
                    if not rules:
                        raise LookupError(
                            f"Rules file {path} contains empty set of rules for set with description: {description}")
                    for rule in rules:
                        action = rule['action']
                        validation_rule = rule['rule']
                        process_rule(ruleset, action, validation_rule)
            except KeyError:
                raise ValueError(f"Invalid rule in file {path}; it can only contain 'action' and 'rule' elements, but"
                                 f" got keys of {list(rule.keys())}")
            return rules_engine

    def process(self, composite_metric: CompositeMetric, previous_metric: Optional[CompositeMetric] = None):
        """
        process the given composite metric (against previous metric if provided) and generate all failures against
        the rules

        :param composite_metric: the metric to test
        :param previous_metric: if not None, previous value for evaluating comparison rules if any

        :return: a generator of any alerts against  or violations of the rules
        """
        for ruleset in self._rulesets:
            for failure in ruleset.process(composite_metric, previous_metric):
                yield failure
