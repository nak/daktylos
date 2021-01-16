import datetime
from contextlib import suppress

import sqlalchemy

import pytest
from typing import Optional

from daktylos.data import CompositeMetric, Metric, Metadata
import os

# import logging
# logging.basicConfig()
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


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
        grandchild2_1 = Metric("grandchild1", seed[1])
        grandchild2_2 = Metric("grandchild2", seed[2])
        child2.add(grandchild2_1)
        child2.add(grandchild2_2)
        grandchild3_1 = Metric("grandchild1", seed[3])
        child3.add(grandchild3_1)
        yield top
        seed[0] += 1
        seed[1] *= 0.9992
        seed[2] *= 1.2
        seed[3] -= 2


@pytest.fixture(scope='function')
def engine():
    return sqlalchemy.create_engine("sqlite:///:memory:")


@pytest.fixture(scope='function')
def redshift_engine():
    return sqlalchemy.create_engine()


MYSQL_USER = os.environ.get("MYSQL_USER")
MYSQL_PSWD = os.environ.get("MYSQL_PSWD")
REDSHIFT_USER = os.environ.get("REDSHIFT_USER")
REDSHIFT_PSWD = os.environ.get("REDSHIFT_PSWD")


db_urls = [
    "sqlite:///:memory:"
]
if REDSHIFT_PSWD and REDSHIFT_USER:
    db_urls.append(f"redshift+psycopg2://{REDSHIFT_USER}:{REDSHIFT_PSWD}@redshift-cluster-2.czb6gvb6mhmn.us-west-1.redshift.amazonaws.com:5439/dev")
if MYSQL_PSWD and MYSQL_USER:
    db_urls.append(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PSWD}@localhost/daktylos?charset=utf8mb4")


def clear_store(datastore, dburl):
    if 'redshift' in dburl:
        from daktylos.data_stores.sql_crippled import (
            SQLMetricStore,
            SQLCompositeMetric,
            SQLMetadataSet,
            SQLMetadata,
            SQLMetric,
        )
    else:
        from daktylos.data_stores.sql import (
            SQLMetricStore,
            SQLCompositeMetric,
            SQLMetadataSet,
            SQLMetadata,
            SQLMetric,
        )
    # datastore._session.rollback()
    datastore._session.commit()
    datastore._session.query(SQLMetric).delete()
    datastore._session.query(SQLCompositeMetric).delete()
    for item in datastore._session.query(SQLMetadataSet):
        item.data.clear()
    datastore._session.commit()
    datastore._session.query(SQLMetadata).delete()
    datastore._session.query(SQLMetadataSet).delete()
    datastore._session.commit()


@pytest.fixture(scope='function', params=db_urls)
def datastore(request):
    engine = sqlalchemy.create_engine(request.param)
    if 'redshift' in request.param:
        from daktylos.data_stores.sql_crippled import (
            SQLMetricStore,
            SQLCompositeMetric,
            SQLMetadataSet,
            SQLMetadata,
            SQLMetric,
        )
    else:
        from daktylos.data_stores.sql import (
            SQLMetricStore,
            SQLCompositeMetric,
            SQLMetadataSet,
            SQLMetadata,
            SQLMetric,
        )
    with SQLMetricStore(engine=engine, create=True) as store:
        store.SQLCompositeMetric = SQLCompositeMetric
        store.SQLMetadataSet = SQLMetadataSet
        store.SQLMetadata = SQLMetadata
        store.SQLMetric = SQLMetric
        try:
            yield store
        finally:
            clear_store(store, request.param)


@pytest.fixture(scope='function', params=db_urls)
def preloaded_datastore(request):
    engine = sqlalchemy.create_engine(request.param)
    metadata = Metadata.system_info()
    timestamp = datetime.datetime.utcnow()
    # import daktylos.data_stores.sql as sql
    # sql.Base.metadata.drop_all(engine)
    if 'redshift' in request.param:
        from daktylos.data_stores.sql_crippled import (
            SQLMetricStore,
            SQLCompositeMetric,
            SQLMetadataSet,
            SQLMetadata,
            SQLMetric,
        )
    else:
        from daktylos.data_stores.sql import (
            SQLMetricStore,
            SQLCompositeMetric,
            SQLMetadataSet,
            SQLMetadata,
            SQLMetric,
        )
    with SQLMetricStore(engine=engine, create=True) as datastore:
        datastore.SQLCompositeMetric = SQLCompositeMetric
        datastore.SQLMetadataSet = SQLMetadataSet
        datastore.SQLMetadata = SQLMetadata
        datastore.SQLMetric = SQLMetric
        clear_store(datastore, request.param)
        try:
            index = 0
            for metric in data_generator():
                datastore.post(metric, timestamp - datetime.timedelta(seconds=index),
                               metadata=metadata)
                index += 1
            datastore.commit()
            assert datastore._session.query(SQLCompositeMetric).count() == 100
            datastore.base_timestamp = timestamp
            datastore.commit()
            yield datastore
        finally:
            with suppress(Exception):
                clear_store(datastore, request.param)


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
