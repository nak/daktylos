import datetime

import sqlalchemy

import pytest
from typing import Optional

from daktylos.data import CompositeMetric, Metric, Metadata
from daktylos.data_stores.sql import SQLMetricStore, SQLCompositeMetric, SQLMetadataSet, SQLMetadata, SQLMetric


def data_generator():
    seed = [1, 28832.12993, 0.00081238, 291]
    for index in range(100):
        top = CompositeMetric(name="TestMetric")
        child1 = Metric("child1", seed[0])
        child2 = CompositeMetric("child2")
        child3 = CompositeMetric("child3")
        top.add(child1)
        top.add(child2)
        top.add(child3)
        grandchild2_1 = Metric("grandchild2.1", seed[1])
        grandchild2_2 = Metric("grandchild2.2", seed[2])
        child2.add(grandchild2_1)
        child2.add(grandchild2_2)
        grandchild3_1 = Metric("grandchild3.1", seed[3])
        child3.add(grandchild3_1)
        yield top
        seed[0] += 1
        seed[1] *= 0.9992
        seed[2] *= 1.2
        seed[3] -= 2


@pytest.fixture(scope='session')
def engine():
    return sqlalchemy.create_engine("sqlite:///:memory:")


@pytest.fixture(scope='function')
def datastore(engine):
    with SQLMetricStore(engine=engine, create=True) as store:
        try:
            yield store
        finally:
            store._session.query(SQLCompositeMetric).delete()
            store._session.query(SQLMetadataSet).delete()
            store._session.query(SQLMetadata).delete()
            store._session.query(SQLMetric).delete()
            store._session.commit()


@pytest.fixture(scope='function')
def preloaded_datastore(engine):
    metadata = Metadata.system_info()
    timestamp = datetime.datetime.utcnow()
    with SQLMetricStore(engine=engine, create=True) as datastore:
        try:
            index = 0
            for metric in data_generator():
                datastore.post(metric, timestamp - datetime.timedelta(seconds=index),
                               metadata=metadata)
                index += 1
            assert datastore._session.query(SQLCompositeMetric).count() == 100
            datastore.base_timestamp = timestamp
            yield datastore
        finally:
            datastore._session.query(SQLCompositeMetric).delete()
            datastore._session.query(SQLMetadataSet).delete()
            datastore._session.query(SQLMetadata).delete()
            datastore._session.query(SQLMetric).delete()
            datastore._session.commit()


class CodeCoverageMetrics(CompositeMetric):

    def __init__(self):
        super().__init__(name="CodeCoverage")
        self._by_file_metrics: Optional[CompositeMetric] = None
        self._by_pkg_metrics: Optional[CompositeMetric] = None

    @property
    def by_file_composite(self):
        if self._by_file_metrics is None:
            self._by_file_metrics = CompositeMetric(name="by_file")
        return self._by_file_metrics

    @property
    def by_pkg_composite(self):
        if self._by_pkg_metrics is None:
            self._by_pkg_metrics = CompositeMetric(name="by_file")
        return self._by_pkg_metrics

    def add_overall(self, value: float) -> None:
        metric = Metric("overall", value)
        self.add(metric)

    def add_by_file(self, file_name: str, value: float) -> None:
        self.by_file_composite.add(Metric(name=file_name, value=value))

    def add_by_pkg(self, pkg_name: str, value: float) -> None:
        self.by_pkg_composite.add(Metric(name=pkg_name, value=value))


class CodeCoverageFactory:

    def generate(self, count: int):
        overall = 80.0
        by_files = {'test/test1.py': 72.0,
                    'test/test2.py': 92.1}
        by_pkgs = {'daktylos.test.pkg1': 82.92,
                   'daktylos.test.pkg2': 65.122}
        for _ in range(count):
            composite = CodeCoverageMetrics()
            composite.add_overall(overall)
            for file_name, value in by_files.items():
                composite.add_by_file(file_name, value)
                by_files[file_name] += 0.5
            for pkg_name, value in by_pkgs.items():
                composite.add_by_pkg(pkg_name, value)
                by_files[pkg_name] += 0.23
            overall += 0.1
            yield composite


class PerformanceMetrics(CompositeMetric):

    def __init__(self):
        super().__init__(name="Performance")
        self._by_test: Optional[CompositeMetric] = None

    @property
    def by_test(self):
        if self._by_test is None:
            self._by_test = CompositeMetric(name="by_test")
        return self._by_test

    def add_overall_cpu(self, user_cpu_secs: float, system_cpu_secs: float, duration: float):
        self.add(Metric("overall_ucpu", user_cpu_secs))
        self.add(Metric("overall_scpu", system_cpu_secs))
        self.add(Metric("overall_duration", duration))

    def add_by_test(self, test_name: str, user_cpu_secs: float, system_cpu_secs: float, duration: float):
        composite = CompositeMetric(name=test_name)
        composite.add(Metric(name="user_cpu", value=user_cpu_secs))
        composite.add(Metric(name="sys_cpu", value=system_cpu_secs))
        composite.add(Metric(name="duration", value=duration))
        self.by_test.add(composite)


class PerformanceMetricsFactory:

    def generate(self, count: int):
        for index in range(count):
            ovarall_duration = 10.0 - 0.05* (index % 100)
            overall_ucpu = (5.0 + 0.1 * (index % 100) - 0.001 * (index % 50))* overall_duration / 100.0
            overall_scpu = (0.1 + 0.02 * (index % 100) - 0.0005 * (index % 50)) * ovarall_duration / 100.0
            perf_metric = PerformanceMetrics()
            perf_metric.add_overall_cpu(user_cpu_secs=overall_ucpu, system_cpu_secs=overall_scpu,
                                        duration=ovarall_duration)
            by_tests = {
                'some.pkg.test1' : (overall_ucpu*0.82, overall_scpu*1.05, ovarall_duration*0.2 ),
                'some.pkg2.test2': (overall_ucpu * 1.2, overall_scpu * .65, ovarall_duration * 0.72),
            }
            for test_name, (ucpu, scpu, duration) in by_tests.items():
                perf_metric.add_by_test(test_name, ucpu, scp, duration)
            yield perf_metric


@pytest.fixture
def code_cov_metrics_generator():
    return CodeCoverageFactory()


@pytest.fixture
def performance_metrics_generator():
    return PerformanceMetricsFactory()
