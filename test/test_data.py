from dataclasses import dataclass, field
from typing import Dict, Optional

from daktylos.data import BasicMetric, Metric, CompositeMetric, MetricDataClass


class TestBasicMetricConversions:

    def test_from_dataclass_simple_float(self):
        float_value = 902.341234
        metric = BasicMetric.from_dataclass("TestFloatMetric", float_value)
        assert isinstance(metric, Metric)
        assert metric.value == float_value

    def test_from_dataclass_simple_int(self):
        int_value = 90
        metric = BasicMetric.from_dataclass("TestIntMetric", int_value)
        assert isinstance(metric, Metric)
        assert metric.value == int_value

    def test_from_dataclass_complex(self):
        @dataclass
        class TestMetric:
            first_value: float
            second_value: float

        data_value = TestMetric(1.2, -93.2)
        metric = BasicMetric.from_dataclass("TestDataClassMetric", data_value)
        assert isinstance(metric, CompositeMetric)
        assert metric["first_value"].value == data_value.first_value
        assert metric["second_value"].value == data_value.second_value

    def test_from_dataclass_complex_with_dict(self):
        @dataclass
        class TestMetric(MetricDataClass):
            first_value: float
            second_value: float
            values: Dict[str, float]

        data_value = TestMetric(1.2, -93.2, {'one': 0.29354, 'two': -9923.22})
        metric = BasicMetric.from_dataclass("TestDataClassMetric", data_value)
        assert isinstance(metric, CompositeMetric)
        assert metric["first_value"].value == data_value.first_value
        assert metric["second_value"].value == data_value.second_value
        assert isinstance(metric["values"], CompositeMetric)
        assert isinstance(metric["values"].value["one"], Metric)
        assert isinstance(metric["values"].value["two"], Metric)
        assert metric["values"]["one"].value == data_value.values["one"]

    def test_from_dataclass_complex_hierarchy(self):
        @dataclass
        class Inner(MetricDataClass):
            one: float
            two: float

        @dataclass
        class TestMetric(MetricDataClass):
            first_value: float
            second_value: float
            values: Inner

        data_value = TestMetric(1.2, -93.2, Inner(0.29354, -9923.22))
        metric = BasicMetric.from_dataclass("TestDataClassMetric", data_value)
        assert isinstance(metric, CompositeMetric)
        assert metric["first_value"].value == data_value.first_value
        assert metric["second_value"].value == data_value.second_value
        assert isinstance(metric["values"], CompositeMetric)
        assert isinstance(metric["values"].value["one"], Metric)
        assert isinstance(metric["values"].value["two"], Metric)
        assert metric["values"]["one"].value == data_value.values.one
        assert metric["values"]["two"].value == data_value.values.two

    def test_to_dataclass_simple(self):
        metric = CompositeMetric("TestMetric")
        metric.add(Metric("one", 93.224556768))
        metric.add(Metric("two", 1.0))

        @dataclass
        class TestDataClass(MetricDataClass):
            one: float
            two: float

        data = metric.to_dataclass(TestDataClass)
        assert data.one == metric["one"].value
        assert data.two == metric["two"].value

    def test_to_dataclass_simple_with_dict(self):
        metric = CompositeMetric("TestMetric")
        metric.add(Metric("one", 93.224556768))
        metric.add(Metric("two", 1.0))
        inner = CompositeMetric("values")
        comp_inner1 = CompositeMetric("comp_one")
        comp_inner1.add(Metric("in_one", 23.354))
        comp_inner1.add(Metric("in_two", -23.354))
        inner.add(comp_inner1)
        comp_inner2 = CompositeMetric("comp_two")
        comp_inner2.add(Metric("in_one", 3455623.354))
        comp_inner2.add(Metric("in_two", -56576823.354))
        inner.add(comp_inner2)
        metric.add(inner)

        @dataclass
        class Inner(MetricDataClass):
            in_one: float
            in_two: float

        @dataclass
        class TestDataClass(MetricDataClass):
            one: float
            two: float
            values: Dict[str, Inner]

        data = metric.to_dataclass(TestDataClass)
        assert data.one == metric["one"].value
        assert data.two == metric["two"].value
        assert isinstance(data.values["comp_one"], Inner)
        assert data.values["comp_one"].in_one == inner.value["comp_one"].value["in_one"].value
        assert data.values["comp_one"].in_two == inner.value["comp_one"].value["in_two"].value
        assert data.values["comp_two"].in_one == inner.value["comp_two"].value["in_one"].value
        assert data.values["comp_two"].in_two == inner.value["comp_two"].value["in_two"].value

    def test_to_dataclass_simple_with_dict_and_optionals(self):
        @dataclass
        class Inner(MetricDataClass):
            in_one: float
            in_two: Optional[float] = None

        @dataclass
        class TestDataClass(MetricDataClass):
            one: float
            two: Optional[float] = None
            values: Optional[Dict[str, Inner]] = None

        metric = CompositeMetric("TestMetric")
        metric.add(Metric("one", 93.224556768))

        data = metric.to_dataclass(TestDataClass)
        assert data.one == metric["one"].value
        assert data.two is None
        assert data.values is None

        metric.add(Metric("two", 1.0))
        data = metric.to_dataclass(TestDataClass)
        assert data.one == metric["one"].value
        assert data.two == metric["two"].value
        assert data.values is None

        inner = CompositeMetric("values")
        comp_inner1 = CompositeMetric("comp_one")
        comp_inner1.add(Metric("in_one", 23.354))
        comp_inner1.add(Metric("in_two", -23.354))
        inner.add(comp_inner1)
        metric.add(inner)
        metric.add(Metric("two", 1.0))
        data = metric.to_dataclass(TestDataClass)
        assert data.one == metric["one"].value
        assert data.two == metric["two"].value
        assert isinstance(data.values["comp_one"], Inner)
        assert data.values["comp_one"].in_one == inner.value["comp_one"].value["in_one"].value
        assert data.values["comp_one"].in_two == inner.value["comp_one"].value["in_two"].value


        comp_inner2 = CompositeMetric("comp_two")
        comp_inner2.add(Metric("in_one", 3455623.354))
        inner.add(comp_inner2)

        data = metric.to_dataclass(TestDataClass)
        assert data.one == metric["one"].value
        assert data.two == metric["two"].value
        assert isinstance(data.values["comp_one"], Inner)
        assert data.values["comp_one"].in_one == inner.value["comp_one"].value["in_one"].value
        assert data.values["comp_one"].in_two == inner.value["comp_one"].value["in_two"].value
        assert data.values["comp_two"].in_one == inner.value["comp_two"].value["in_one"].value
        assert data.values["comp_two"].in_two == None

