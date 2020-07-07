import datetime

import pytest

from daktylos.data import CompositeMetric, Metric, Metadata
from daktylos.data_stores.sql import SQLMetricStore, SQLCompositeMetric, SQLMetadataSet, SQLMetadata


class TestSQLMetricStore:

    def test_purge_by_date(self, preloaded_datastore: SQLMetricStore):
        preloaded_datastore.purge_by_date(before=datetime.datetime.utcnow() - datetime.timedelta(days=1))
        assert preloaded_datastore._session.query(SQLCompositeMetric).all() == []
        assert preloaded_datastore._session.query(SQLMetadataSet).all() == []
        assert preloaded_datastore._session.query(SQLMetadata).all() == []

    def test_purge_by_volume(self, preloaded_datastore: SQLMetricStore):
        assert preloaded_datastore._session.query(SQLCompositeMetric).count() == 100
        preloaded_datastore.purge_by_volume(count=50, name="TestMetric")
        assert preloaded_datastore._session.query(SQLCompositeMetric).count() == 50
        assert preloaded_datastore._session.query(SQLMetadataSet).count() == 1
        assert preloaded_datastore._session.query(SQLMetadata).count() == 6
        for item in preloaded_datastore._session.query(SQLCompositeMetric).all():
            assert int(item.children[0].value) < 51

    def test_post(self, datastore: SQLMetricStore):

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

        compare = {}
        for item in data_generator():
            compare[int(item['#child1'].value)] = item
            datastore.post(item, datetime.datetime.utcnow(), project_name="TestProject", uuid="test_uuid")
        assert datastore._session.query(SQLCompositeMetric).count() == 100
        for item in datastore._session.query(SQLCompositeMetric).all():
            index = int(item.children[0].value)
            assert item.children[1].value == compare[index]['child2']['grandchild2.1'].value
            assert item.children[2].value == compare[index]['child2']['#grandchild2.2'].value
            assert item.children[3].value == compare[index]['child3#grandchild3.1'].value

    def test_metrics_by_date(self, preloaded_datastore: SQLMetricStore):
        assert preloaded_datastore.metrics_by_date(metric_name="TestMetric",
                                                   oldest=datetime.datetime.utcnow()) == []

        oldest = datetime.datetime.utcnow() - datetime.timedelta(seconds=10)
        items = preloaded_datastore.metrics_by_date(metric_name="TestMetric",
                                                    oldest=oldest)
        all_items = preloaded_datastore.metrics_by_date(metric_name="TestMetric",
                                                        oldest=oldest - datetime.timedelta(days=100))
        assert len(items) > 0
        for item in items:
            assert item.timestamp >= oldest
        for item in all_items:
            if item.timestamp >= oldest:
                assert item in items
            else:
                assert item not in items

    def test_metrics_by_volume(self, preloaded_datastore: SQLMetricStore):
        items = preloaded_datastore.metrics_by_volume(metric_name="TestMetric", count=5)
        all_items = preloaded_datastore.metrics_by_volume(metric_name="TestMetric", count=200)
        assert len(items) == 5
        assert len(all_items) == 100
        sorted_items = sorted(all_items, key=lambda x: x.timestamp)[-5:]
        for index, item in enumerate(items):
            assert item in sorted_items
            if index > 0:
                assert items[index-1].timestamp > item.timestamp

    def test_metrics_by_volume_with_filter(self, preloaded_datastore: SQLMetricStore):
        system_info = Metadata.system_info()
        preloaded_datastore.filter_on_metadata(name="platform", value=system_info.values["platform"])
        items = preloaded_datastore.metrics_by_volume(metric_name="TestMetric", count=5)
        all_items = preloaded_datastore.metrics_by_volume(metric_name="TestMetric", count=200)
        assert len(items) == 5
        assert len(all_items) == 100
        sorted_items = sorted(all_items, key=lambda x: x.timestamp)[-5:]
        for index, item in enumerate(items):
            assert item in sorted_items
            if index > 0:
                assert items[index-1].timestamp > item.timestamp
        with pytest.raises(ValueError):
            preloaded_datastore.filter_on_metadata(name="platform", value=system_info.values["platform"])
        preloaded_datastore.clear_filter("platform")
        preloaded_datastore.filter_on_metadata(name="platform", value="fail")
        items = preloaded_datastore.metrics_by_volume(metric_name="TestMetric", count=5)
        assert items == []

    def test_metrics_by_date_with_filter(self, preloaded_datastore: SQLMetricStore):
        system_info = Metadata.system_info()
        preloaded_datastore.filter_on_metadata(name="platform", value=system_info.values["platform"])
        oldest = datetime.datetime.utcnow() - datetime.timedelta(seconds=10)
        items = preloaded_datastore.metrics_by_date(metric_name="TestMetric",
                                                    oldest=oldest)
        all_items = preloaded_datastore.metrics_by_date(metric_name="TestMetric",
                                                        oldest=oldest - datetime.timedelta(days=100))
        assert len(items) > 0
        assert len(all_items) == 100
        sorted_items = sorted(all_items, key=lambda x: x.timestamp)[-5:]
        for index, item in enumerate(sorted_items):
            assert item in items
            if index > 0:
                assert items[index-1].timestamp > items[index].timestamp
        with pytest.raises(ValueError):
            preloaded_datastore.filter_on_metadata(name="platform", value=system_info.values["platform"])
        preloaded_datastore.clear_filter("platform")
        preloaded_datastore.filter_on_metadata(name="platform", value="fail")
        items = preloaded_datastore.metrics_by_date(metric_name="TestMetric",
                                                    oldest=oldest)
        assert items == []
