import datetime

import sqlalchemy

import pytest

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
    metatdata = Metadata.system_info()
    try:
        with SQLMetricStore(engine=engine, create=True, metadata=metatdata) as store:
            yield store
    finally:
        store._session.query(SQLCompositeMetric).delete()
        store._session.query(SQLMetadataSet).delete()
        store._session.query(SQLMetadata).delete()
        store._session.query(SQLMetric).delete()
        store._session.commit()


@pytest.fixture(scope='function')
def preloaded_datastore(engine):
    metatdata = Metadata.system_info()
    with SQLMetricStore(engine=engine, create=True, metadata=metatdata) as datastore:
        try:
            index = 0
            for metric in data_generator():
                datastore.post(metric, datetime.datetime.utcnow() - datetime.timedelta(seconds=index))
                index += 1
            assert datastore._session.query(SQLCompositeMetric).count() == 100
            yield datastore
        finally:
            datastore._session.query(SQLCompositeMetric).delete()
            datastore._session.query(SQLMetadataSet).delete()
            datastore._session.query(SQLMetadata).delete()
            datastore._session.query(SQLMetric).delete()
            datastore._session.commit()
