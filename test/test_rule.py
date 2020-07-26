import pytest

from daktylos.data import CompositeMetric, Metric
from daktylos.rules.engine import Rule, RulesEngine


class TestRule:

    def test_validate_lt(self):
        threshold = 100.0
        composite_metric = CompositeMetric(name="Test_Metric")
        metric = Metric(name="fail1", value=110.0)
        composite_metric.add(metric)
        metric = Metric(name="fail2", value=threshold)
        composite_metric.add(metric)
        metric = Metric(name="pass", value=99.0)
        composite_metric.add(metric)
        with pytest.raises(Rule.ThresholdViolation) as e:
            rule = Rule("/Test_Metric#fail1", operation=Rule.Evaluation.LESS_THAN, limiting_value=threshold)
            rule.validate(composite_metric)
        assert f"/Test_Metric#fail1 >= {threshold}" in str(e)
        assert "#fail2" not in str(e)
        assert "#pass" not in str(e)
        with pytest.raises(Rule.ThresholdViolation) as e:
            rule = Rule("/Test_Metric#fail2", operation=Rule.Evaluation.LESS_THAN, limiting_value=threshold)
            rule.validate(composite_metric)
        assert f"/Test_Metric#fail2 >= {threshold}" in str(e)
        assert "#fail1" not in str(e)
        assert "#pass" not in str(e)
        with pytest.raises(Rule.ThresholdViolation) as e:
            rule = Rule("/Test_Metric#*", operation=Rule.Evaluation.LESS_THAN, limiting_value=threshold)
            rule.validate(composite_metric)
        assert f"/Test_Metric#fail1 >= {threshold}" in f"{e}"
        assert f"/Test_Metric#fail2 >= {threshold}" in f"{e}"
        assert "#pass" not in f"{e}"

    def test_validate_lte(self):
        threshold = 100.0
        composite_metric = CompositeMetric(name="Test_Metric")
        metric = Metric(name="fail1", value=110.0)
        composite_metric.add(metric)
        metric = Metric(name="pass1", value=threshold)
        composite_metric.add(metric)
        metric = Metric(name="pass2", value=99.0)
        composite_metric.add(metric)
        with pytest.raises(Rule.ThresholdViolation) as e:
            rule = Rule("/Test_Metric#fail1", operation=Rule.Evaluation.LESS_THAN_OR_EQUAL, limiting_value=threshold)
            rule.validate(composite_metric)
        assert f"/Test_Metric#fail1 > {threshold}" in str(e)
        assert "#pass1" not in str(e)
        assert "#pass2" not in str(e)
        rule = Rule("/Test_Metric#fail2", operation=Rule.Evaluation.LESS_THAN_OR_EQUAL, limiting_value=threshold)
        # should not raise exception:
        rule.validate(composite_metric)
        with pytest.raises(Rule.ThresholdViolation) as e:
            rule = Rule("/Test_Metric#*", operation=Rule.Evaluation.LESS_THAN_OR_EQUAL, limiting_value=threshold)
            rule.validate(composite_metric)
        assert f"/Test_Metric#fail1 > {threshold}" in f"{e}"
        assert "#pass1" not in f"{e}"
        assert "#pass2" not in f"{e}"

    def test_validate_gt(self):
        threshold = 100.0
        composite_metric = CompositeMetric(name="Test_Metric")
        metric = Metric(name="fail1", value=90.0)
        composite_metric.add(metric)
        metric = Metric(name="fail2", value=threshold)
        composite_metric.add(metric)
        metric = Metric(name="pass", value=110.0)
        composite_metric.add(metric)
        with pytest.raises(Rule.ThresholdViolation) as e:
            rule = Rule("/Test_Metric#fail1", operation=Rule.Evaluation.GREATER_THAN, limiting_value=threshold)
            rule.validate(composite_metric)
        assert f"/Test_Metric#fail1 <= {threshold}" in str(e)
        assert "#fail2" not in str(e)
        assert "#pass" not in str(e)
        with pytest.raises(Rule.ThresholdViolation) as e:
            rule = Rule("/Test_Metric#fail2", operation=Rule.Evaluation.GREATER_THAN, limiting_value=threshold)
            rule.validate(composite_metric)
        assert f"/Test_Metric#fail2 <= {threshold}" in str(e)
        assert "#fail1" not in str(e)
        assert "#pass" not in str(e)
        with pytest.raises(Rule.ThresholdViolation) as e:
            rule = Rule("/Test_Metric#*", operation=Rule.Evaluation.GREATER_THAN, limiting_value=threshold)
            rule.validate(composite_metric)
        assert f"/Test_Metric#fail1 <= {threshold}" in f"{e}"
        assert f"/Test_Metric#fail2 <= {threshold}" in f"{e}"
        assert "#pass" not in f"{e}"

    def test_validate_gte(self):
        threshold = 100.0
        composite_metric = CompositeMetric(name="Test_Metric")
        metric = Metric(name="fail1", value=99.0)
        composite_metric.add(metric)
        metric = Metric(name="pass1", value=threshold)
        composite_metric.add(metric)
        metric = Metric(name="pass2", value=110.0)
        composite_metric.add(metric)
        with pytest.raises(Rule.ThresholdViolation) as e:
            rule = Rule("/Test_Metric#fail1", operation=Rule.Evaluation.GREATER_THAN_OR_EQUAL, limiting_value=threshold)
            rule.validate(composite_metric)
        assert f"/Test_Metric#fail1 < {threshold}" in str(e)
        assert "#pass1" not in str(e)
        assert "#pass2" not in str(e)
        rule = Rule("/Test_Metric#fail2", operation=Rule.Evaluation.GREATER_THAN_OR_EQUAL, limiting_value=threshold)
        # should not raise exception:
        rule.validate(composite_metric)
        with pytest.raises(Rule.ThresholdViolation) as e:
            rule = Rule("/Test_Metric#*", operation=Rule.Evaluation.GREATER_THAN_OR_EQUAL, limiting_value=threshold)
            rule.validate(composite_metric)
        assert f"/Test_Metric#fail1 < {threshold}" in f"{e}"
        assert "#pass1" not in f"{e}"
        assert "#pass2" not in f"{e}"

    def test_validate_exclusion(self):
        threshold = 100.0
        composite_metric = CompositeMetric(name="TestMetric")
        metric = Metric(name="fail1", value=99.0)
        composite_metric.add(metric)
        metric = Metric(name="pass1", value=threshold)
        composite_metric.add(metric)
        metric = Metric(name="pass2", value=110.0)
        composite_metric.add(metric)
        # Same test as above, but with exclusions:
        rule = Rule("/TestMetric#fail1", operation=Rule.Evaluation.GREATER_THAN_OR_EQUAL, limiting_value=threshold)
        rule.validate(composite_metric, exclusions={"/TestMetric*fail1"})
        # does not throw exception

