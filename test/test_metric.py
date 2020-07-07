import pytest

from daktylos.data import Metric


class TestMetric:

    def test__invalid_name_raises_ValueError(self):
        with pytest.raises(ValueError):
            Metric('some_name_with_#', 0.0)

    def test_flatten(self):
        value = 1.234
        metric = Metric("test_metric", value=value)
        assert metric.value == value
        assert metric.flatten() == {"test_metric": value}
        assert metric.flatten(prefix="/root") == {"/root#test_metric": value}

