import fnmatch
import os
from pathlib import Path

import pytest

from daktylos.rules.engine import Rule, RulesEngine
from daktylos.rules.status import ValidationStatus


class TestRulesEngine:
    def test_add_alert(self):
        rule = Rule(pattern="/TestMetrics/child1#metric1",
                    operation=Rule.Evaluation.GREATER_THAN,
                    limiting_value=0.12)
        rules_set = RulesEngine.RuleSet()
        rules_set.add_alert(rule)
        assert rule in rules_set._alerts
        assert rule not in rules_set._validations

    def test_add_validation(self):
        rule = Rule(pattern="/TestMetrics/child1#metric1",
                    operation=Rule.Evaluation.GREATER_THAN,
                    limiting_value=0.12)
        rules_set = RulesEngine.RuleSet()
        rules_set.add_validation(rule)
        assert rule not in rules_set._alerts
        assert rule in rules_set._validations

    def test_process(self, monkeypatch):
        validations = []

        def mock_validate(self, composite_metric, previous_metric=None, exclusions=None):
            nonlocal validations
            assert exclusions == {"/CodeCoverage/by_file*test_excluded.py"} or len(exclusions) == 0
            if fnmatch.fnmatchcase(self._pattern, "/CodeCoverage/by_file*test_excluded.py"):
                return
            validations.append((self, composite_metric))
            if self._pattern == "/CodeCoverage#overall" and self._limit > 81.0:
                raise Rule.ThresholdViolation(msg="failure_validation_codecov", parent=composite_metric,
                                              offending_elements=['/CodeCoverage#overall'])
            elif self._pattern == "/Performance#overall_cpu":
                raise Rule.ThresholdViolation(msg="second_alert_performance", parent=composite_metric,
                                              offending_elements=['/Performance#overall_cpu'])

        monkeypatch.setattr("daktylos.rules.engine.Rule.validate", mock_validate)
        resources_path = os.path.join(os.path.dirname(__file__), "resources")
        rules_path = Path(os.path.join(resources_path, "test_rules.yaml"))
        rules_engine = RulesEngine.from_yaml_file(rules_path)
        alert_count = 0
        failure_count = 0
        for failure in rules_engine.process("dummy_composite_metric_value"):
            if failure.level == ValidationStatus.Level.ALERT:
                alert_count += 1
                assert list(failure.offending_metrics().keys()) == ['/Performance#overall_cpu']
                assert failure.parent_metric == "dummy_composite_metric_value"
                assert "'/Performance#overall_cpu < 70.0" in failure.text
                assert "ALERT" in failure.text
                assert "second_alert_performance" in failure.text
            elif failure.level == ValidationStatus.Level.FAILURE:
                failure_count += 1
                assert list(failure.offending_metrics().keys()) == ['/CodeCoverage#overall']
                assert failure.parent_metric == "dummy_composite_metric_value"
                assert "/CodeCoverage#overall >= 85.0" in failure.text
                assert "VALIDATION FAILURE" in failure.text
                assert "failure_validation_codecov" in failure.text
            else:
                assert False, f"Unexpected failure level {failure.level}"
        assert alert_count == 1
        assert failure_count == 1
        assert len(validations) == 8

    def test_from_yaml_file(self):
        resources_path = os.path.join(os.path.dirname(__file__), "resources")
        rules_path = Path(os.path.join(resources_path, "test_rules.yaml"))
        rules_engine = RulesEngine.from_yaml_file(rules_path)
        assert len(rules_engine._rulesets) == 2
        for ruleset in rules_engine._rulesets:
            if len(ruleset._alerts) == 1:
                assert len(ruleset._validations) == 2
                for alert in ruleset._alerts:
                    assert alert._pattern in {"/CodeCoverage#overall",
                                              "/Performance#overall_cpu"
                                              }
                    if alert._pattern == "/CodeCoverage#overall":
                        assert alert._operation == Rule.Evaluation.GREATER_THAN
                        assert pytest.approx(alert._limit, 80.0)
                    elif alert._pattern == "/Performance#overall_cpu":
                        assert alert._operation == Rule.Evaluation.LESS_THAN
                        assert pytest.approx(alert._limit, 70.0)
                    else:
                        assert False  # should never get here based on logic
            elif len(ruleset._alerts) == 2:
                assert len(ruleset._exclusions) == 1
                assert ruleset._exclusions == {"/CodeCoverage/by_file*test_excluded.py"}
                assert len(ruleset._validations) == 5
                for validation in ruleset._validations:
                    assert validation._pattern in {
                        "/CodeCoverage#overall",
                        "/CodeCoverage/by_file#test/test_composite_metric.py",
                        "/CodeCoverage/by_file#test/test_excluded.py",
                        "/Performance/by_test#test_SQLMetricsStore.test_metrics_by_date_with_filter",
                        "/Performance/by_test#test_SQLMetricsStore.test_metrics_by_volume_with_filter"
                    }
                    if validation._pattern == "/CodeCoverage#overall":
                        assert validation._operation == Rule.Evaluation.GREATER_THAN_OR_EQUAL
                        assert pytest.approx(validation._limit, 85.0)
                    elif validation._pattern == "/CodeCoverage/by_file#test/test_composite_metric.py":
                        assert validation._operation == Rule.Evaluation.GREATER_THAN
                        assert pytest.approx(validation._limit, 90.0)
                    elif validation._pattern == "/Performance/by_test#test_SQLMetricsStore.test_metrics_by_date_with_filter":
                        assert validation._operation == Rule.Evaluation.LESS_THAN
                        assert pytest.approx(validation._limit, 10.0)
                    elif validation._pattern == "/Performance/by_test#test_SQLMetricsStore.test_metrics_by_volume_with_filter":
                        assert validation._operation == Rule.Evaluation.LESS_THAN_OR_EQUAL
                        assert pytest.approx(validation._limit, 11.0)
                    elif validation._pattern == "/CodeCoverage/by_file#test/test_excluded.py":
                        pass
                    else:
                        assert False
            else:
                assert False, f"Unexpected ruleset with {len(ruleset._alerts)} alerts"

