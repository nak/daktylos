import pytest

from daktylos.data import BasicMetric, CompositeMetric, Metric


class TestCompositeMetric:

    def test_invalid_name_raises_ValueError(self):
        with pytest.raises(ValueError):
            CompositeMetric("name_with_#")
        with pytest.raises(ValueError):
            CompositeMetric("name_with_/")

    def test_flatten(self):
        root = CompositeMetric("root_test_metric")
        child = CompositeMetric("child_test_metric")
        test_value2 = 3490.223
        test_value3 = -123.872
        root.add(child)
        root.add(Metric("pathed/test_metric2", test_value2))
        grandchild = CompositeMetric("grandchild_test_metric")
        child.add(grandchild)
        grandchild.add(Metric("test_metric3", test_value3))
        result = root.flatten()
        assert len(root.value) == 2
        assert '/root_test_metric/child_test_metric/grandchild_test_metric#test_metric3' in result
        assert result['/root_test_metric/child_test_metric/grandchild_test_metric#test_metric3'] == test_value3
        assert '/root_test_metric#pathed/test_metric2' in result
        assert result['/root_test_metric#pathed/test_metric2'] == test_value2

    def test_from_dict(self):
        root = CompositeMetric("root_test_metric")
        child = CompositeMetric("child_test_metric")
        test_value2 = 3490.223
        test_value3 = -123.872
        root.add(child)
        root.add(Metric("pathed/test_metric2", test_value2))
        grandchild = CompositeMetric("grandchild_test_metric")
        child.add(grandchild)
        grandchild.add(Metric("test_metric3", test_value3))
        result = root.flatten()
        new_root = BasicMetric.from_flattened(result)
        assert result == new_root.flatten()

    def test_keys(self):
        root = CompositeMetric("root_test_metric")
        child = CompositeMetric("child_test_metric")
        test_value2 = 3490.223
        test_value3 = -123.872
        root.add(child)
        root.add(Metric("pathed/test_metric2", test_value2))
        grandchild = CompositeMetric("grandchild_test_metric")
        child.add(grandchild)
        grandchild.add(Metric("test_metric3", test_value3))
        assert set(root.keys(core_metrics_only=True)) == {
            "child_test_metric/grandchild_test_metric#test_metric3",
            "#pathed/test_metric2"}
        assert set(root.keys()) == {
            "child_test_metric/grandchild_test_metric#test_metric3",
            "#pathed/test_metric2",
            "child_test_metric",
            "child_test_metric/grandchild_test_metric"}
        for key in root.keys(core_metrics_only=False):
            if '#' in key:
                assert isinstance(root[key], Metric)
            else:
                assert isinstance(root[key], CompositeMetric)
        assert root["child_test_metric"] == child
        with pytest.raises(KeyError):
            root["no_such_child"]
