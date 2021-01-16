import datetime
from dataclasses import dataclass
from typing import Optional

import pytest

from daktylos.data import CompositeMetric, Metric, Metadata, MetricDataClass
from daktylos.data_stores.sql import SQLMetricStore

metadata = Metadata.system_info()


@dataclass
class SubMetricData:
    grandchild1: float
    grandchild2: Optional[float] = -1.0


@dataclass
class TestMetricData:
    child1: int
    child2: SubMetricData
    child3: SubMetricData


class TestSQLMetricStore:

    def test_purge_by_date(self, preloaded_datastore: SQLMetricStore):
        SQLCompositeMetric = preloaded_datastore.SQLCompositeMetric
        SQLMetadataSet = preloaded_datastore.SQLMetadataSet
        SQLMetadata = preloaded_datastore.SQLMetadata
        preloaded_datastore.purge_by_date(before=datetime.datetime.utcnow() - datetime.timedelta(days=1))
        assert preloaded_datastore._session.query(SQLCompositeMetric).all() == []
        assert preloaded_datastore._session.query(SQLMetadataSet).all() == []
        assert preloaded_datastore._session.query(SQLMetadata).all() == []

    def test_purge_by_volume(self, preloaded_datastore: SQLMetricStore):
        SQLCompositeMetric = preloaded_datastore.SQLCompositeMetric
        SQLMetadataSet = preloaded_datastore.SQLMetadataSet
        SQLMetadata = preloaded_datastore.SQLMetadata
        assert preloaded_datastore._session.query(SQLCompositeMetric).count() == 100
        preloaded_datastore.purge_by_volume(count_=50, name="TestMetric")
        assert preloaded_datastore._session.query(SQLCompositeMetric).count() == 50
        assert preloaded_datastore._session.query(SQLMetadataSet).count() == 1
        assert preloaded_datastore._session.query(SQLMetadata).count() == 6
        for item in preloaded_datastore._session.query(SQLCompositeMetric).all():
            for child in item.children + [None]:
                if child is None:
                    assert False, "Expected child but found none"
                if child.name == '/TestMetric#child1':
                    assert int(child.value) < 51
                    break

    def test_post_data(self, datastore: SQLMetricStore):
        SQLCompositeMetric = datastore.SQLCompositeMetric
        @dataclass
        class ChildMetric(MetricDataClass):
            grandchlid1: float
            grandchild2: float

        @dataclass
        class TestMetric(MetricDataClass):
            child1: int
            child2: ChildMetric
            child3: float

        def data_generator():
            seed = [1, 28832.12993, 0.00081238, 291]
            for index in range(100):
                test_metric = TestMetric(seed[0], ChildMetric(seed[1], seed[2]), seed[3])
                yield test_metric
                seed[0] += 1
                seed[1] *= 0.9992
                seed[2] *= 1.2
                seed[3] -= 2

        compare = {}
        for item in data_generator():
            compare[item.child1] = item
            datastore.post_data("TestMetric", item,
                                metadata=metadata,
                                timestamp=datetime.datetime.utcnow(), project_name="TestProject", uuid="test_uuid")
        datastore.commit()
        assert datastore._session.query(SQLCompositeMetric).count() == 100
        for item in datastore._session.query(SQLCompositeMetric).all():
            children = {child.name: child.value for child in item.children}
            index = int(children['/TestMetric#child1'])
            assert children['/TestMetric/child2#grandchlid1'] == pytest.approx(compare[index].child2.grandchlid1, 0.000001)
            assert children['/TestMetric/child2#grandchild2'] == pytest.approx(compare[index].child2.grandchild2, 0.000001)
            assert children['/TestMetric#child3'] == pytest.approx(compare[index].child3, 0.000001)

    def test_post(self, datastore: SQLMetricStore):
        SQLCompositeMetric = datastore.SQLCompositeMetric
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
            datastore.post(item, datetime.datetime.utcnow(), metadata=metadata,
                           project_name="TestProject", uuid="test_uuid")
        datastore.commit()
        assert datastore._session.query(SQLCompositeMetric).count() == 100
        for item in datastore._session.query(SQLCompositeMetric).all():
            children = {child.name: child.value for child in item.children}
            index = int(children['/TestMetric#child1'])
            assert children['/TestMetric/child2#grandchild2.1'] == pytest.approx(compare[index]['child2']['grandchild2.1'].value, 0.000001)
            assert children['/TestMetric/child2#grandchild2.2'] == pytest.approx(compare[index]['child2']['#grandchild2.2'].value, 0.000001)
            assert children['/TestMetric/child3#grandchild3.1'] == pytest.approx(compare[index]['child3#grandchild3.1'].value, 0.000001)

    def test_metrics_by_date(self, preloaded_datastore: SQLMetricStore):
        timestamp = preloaded_datastore.base_timestamp
        assert preloaded_datastore.composite_metrics_by_date(metric_name="TestMetric",
                                                             oldest=datetime.datetime.utcnow()).metric_data == []

        oldest = timestamp - datetime.timedelta(seconds=10)
        items = preloaded_datastore.composite_metrics_by_date(metric_name="TestMetric", oldest=oldest)
        all_items = preloaded_datastore.composite_metrics_by_date(metric_name="TestMetric",
                                                                  oldest=oldest - datetime.timedelta(days=100))
        assert len(items.metric_data) > 0
        metadata = items.metadata[0]
        assert len(items.metadata) == len(items.metric_data) == len(items.timestamps)
        for index in range(len(items.metadata)):
            assert items.timestamps[index] >= oldest
            assert items.metadata[index] == metadata
        for timestamp in all_items.timestamps:
            assert oldest - datetime.timedelta(days=100) <= timestamp <= preloaded_datastore.base_timestamp

    def test_metrics_by_volume(self, preloaded_datastore: SQLMetricStore):
        items = preloaded_datastore.composite_metrics_by_volume(metric_name="TestMetric", count=5)
        all_items = preloaded_datastore.composite_metrics_by_volume(metric_name="TestMetric", count=200)
        assert len(items.metric_data) == 5
        assert len(all_items.metric_data) == 100  # there are only 100 items in preloaded datastore
        assert len(items.metric_data) == len(items.metadata) == len(items.timestamps)
        assert len(all_items.metric_data) == len(all_items.metadata) == len(all_items.timestamps)
        sorted_items = all_items.metric_data[-5:]
        for index, item in enumerate(items.metric_data):
            assert item == sorted_items[index]
        for index in range(1, len(all_items.timestamps)):
            assert all_items.timestamps[index-1] < all_items.timestamps[index]

    def test_metrics_by_volume_with_filter(self, preloaded_datastore: SQLMetricStore):
        system_info = Metadata.system_info()
        items = preloaded_datastore.composite_metrics_by_volume(metric_name="TestMetric", count=5)
        for metadata in items.metadata:
            assert metadata.values['platform'] == system_info.values['platform']
        items = preloaded_datastore.composite_metrics_by_volume(metric_name="TestMetric", count=5,
                                                                metadata_filter={'platform': system_info.values['platform']})
        all_items = preloaded_datastore.composite_metrics_by_volume(metric_name="TestMetric", count=200)
        assert len(all_items.metric_data) == 100
        assert len(items.metadata) == 5
        sorted_items = all_items.metric_data[-5:]
        for index, item in enumerate(items.metric_data):
            assert item in sorted_items
        for index in range(1, len(items.timestamps)):
            assert items.timestamps[index-1] < items.timestamps[index]
        items = preloaded_datastore.composite_metrics_by_volume(metric_name="TestMetric", count=5,
                                                                metadata_filter={'platform': 'fail'})
        assert items.timestamps == items.metric_data == items.metadata == []

    def test_metrics_by_date_with_filter(self, preloaded_datastore: SQLMetricStore):
        system_info = Metadata.system_info()
        timestamp = preloaded_datastore.base_timestamp
        oldest = timestamp - datetime.timedelta(seconds=10)
        items = preloaded_datastore.dataclass_metrics_by_date(name="TestMetric",
                                                              typ=TestMetricData,
                                                              oldest=oldest,
                                                              metadata_filter={'platform': system_info.values['platform']})
        all_items = preloaded_datastore.dataclass_metrics_by_date(name="TestMetric", typ=TestMetricData,
                                                                  oldest=oldest - datetime.timedelta(days=100))
        assert len(items.metric_data) > 0
        assert len(all_items.timestamps) == 100
        sorted_items = all_items.metric_data[-len(items.metric_data):]
        for index, item in enumerate(sorted_items):
            assert item in sorted_items
        for index in range(1, len(all_items.timestamps)):
            assert all_items.timestamps[index-1] < all_items.timestamps[index]
        items = preloaded_datastore.dataclass_metrics_by_date(name="TestMetric",
                                                              typ=TestMetricData,
                                                              oldest=oldest,
                                                              metadata_filter={'platform': 'fail'})
        assert items.timestamps == items.metric_data == items.metadata == []

    def test_metrics_data_by_date_with_filter(self, preloaded_datastore: SQLMetricStore):
        system_info = Metadata.system_info()
        timestamp = preloaded_datastore.base_timestamp
        oldest = timestamp - datetime.timedelta(seconds=10)
        items = preloaded_datastore.dataclass_metrics_by_date(name="TestMetric",
                                                              typ=TestMetricData,
                                                              oldest=oldest,
                                                              metadata_filter={'platform': system_info.values['platform'],
                                                                          'system': system_info.values['system']})
        all_items = preloaded_datastore.dataclass_metrics_by_date(name="TestMetric",
                                                                  typ=TestMetricData,
                                                                  oldest=oldest - datetime.timedelta(days=100))
        assert len(items.metric_data) > 0
        assert len(all_items.timestamps) == 100
        sorted_items = all_items.metric_data[-len(items.metric_data):]
        for index, item in enumerate(sorted_items):
            assert item in sorted_items
        for index in range(1, len(all_items.timestamps)):
            assert all_items.timestamps[index-1] < all_items.timestamps[index]
        items = preloaded_datastore.dataclass_metrics_by_date(name="TestMetric",
                                                              typ=TestMetricData,
                                                              oldest=oldest,
                                                              metadata_filter={'platform': 'fail'})
        assert items.timestamps == items.metric_data == items.metadata == []

    def test_metrics_fields_by_date(self, preloaded_datastore: SQLMetricStore):
        timestamp = preloaded_datastore.base_timestamp
        assert preloaded_datastore.metric_fields_by_date(metric_name="TestMetric",
                                                         oldest=datetime.datetime.utcnow()).metric_data == {}

        oldest = timestamp - datetime.timedelta(seconds=10)
        items = preloaded_datastore.metric_fields_by_date(metric_name="TestMetric", oldest=oldest)
        all_items = preloaded_datastore.metric_fields_by_date(metric_name="TestMetric",
                                                              oldest=oldest - datetime.timedelta(days=100))
        assert len(items.metric_data) > 0
        metadata = items.metadata[0]
        assert len(items.metadata) == len(items.metric_data[list(items.metric_data.keys())[0]]) == len(items.timestamps)
        for index in range(len(items.metadata)):
            assert items.timestamps[index] >= oldest
            assert items.metadata[index] == metadata
        for timestamp in all_items.timestamps:
            assert oldest - datetime.timedelta(days=100) <= timestamp <= preloaded_datastore.base_timestamp

    def test_metric_fields_by_volume(self, preloaded_datastore: SQLMetricStore):
        items = preloaded_datastore.metric_fields_by_volume(metric_name="TestMetric", count=5)
        all_items = preloaded_datastore.metric_fields_by_volume(metric_name="TestMetric", count=200)
        items_key0 = list(items.metric_data.keys())[0]
        all_items_key0 = list(all_items.metric_data.keys())[0]
        assert len(items.metric_data[items_key0]) == 5
        assert len(all_items.metric_data[all_items_key0]) == 100  # there are only 100 items in preloaded datastore
        assert len(items.metric_data[items_key0]) == len(items.metadata) == len(items.timestamps)
        assert len(all_items.metadata) == len(all_items.metric_data[all_items_key0]) == len(all_items.timestamps)
        for key in all_items.metric_data.keys():
            sorted_items = all_items.metric_data[key][-5:]
            for index, item in enumerate(items.metric_data[key]):
                assert item == sorted_items[index]
        for index in range(1, len(all_items.timestamps)):
            assert all_items.timestamps[index-1] < all_items.timestamps[index]

    def test_metric_subfields_by_volume(self, preloaded_datastore: SQLMetricStore):
        system_info = Metadata.system_info()
        items = preloaded_datastore.metric_fields_by_volume(metric_name="TestMetric", count=5, fields=['%grandchild%'],
                                                            metadata_filter={'platform': system_info.values['platform']})
        for key in items.metric_data.keys():
            assert 'grandchild' in key
        items_key0 = list(items.metric_data.keys())[0]
        assert len(items.metric_data[items_key0]) == 5
        assert len(items.metric_data[items_key0]) == len(items.metadata) == len(items.timestamps)
        for index in range(1, len(items.timestamps)):
            assert items.timestamps[index-1] < items.timestamps[index]

    def test_metric_fields_by_volume_with_filter(self, preloaded_datastore: SQLMetricStore):
        system_info = Metadata.system_info()
        items = preloaded_datastore.metric_fields_by_volume(metric_name="TestMetric", count=5)
        for metadata in items.metadata:
            assert metadata.values['platform'] == system_info.values['platform']
        items = preloaded_datastore.metric_fields_by_volume(metric_name="TestMetric", count=5,
                                                            metadata_filter={'platform': system_info.values['platform']})
        all_items = preloaded_datastore.metric_fields_by_volume(metric_name="TestMetric", count=200)
        all_items_key0 = list(all_items.metric_data.keys())[0]
        assert len(all_items.metric_data[all_items_key0]) == 100
        assert len(items.metadata) == 5
        sorted_items = all_items.metric_data[all_items_key0][-5:]
        for index, item in enumerate(items.metric_data[all_items_key0]):
            assert item in sorted_items
        for index in range(1, len(items.timestamps)):
            assert items.timestamps[index-1] < items.timestamps[index]
        items = preloaded_datastore.metric_fields_by_volume(metric_name="TestMetric", count=5,
                                                            metadata_filter={'platform': 'fail'})
        assert items.timestamps == items.metadata == []
        assert items.metric_data == {}

    def test_metric_fields_by_date_with_filter(self, preloaded_datastore: SQLMetricStore):
        system_info = Metadata.system_info()
        timestamp = preloaded_datastore.base_timestamp
        oldest = timestamp - datetime.timedelta(seconds=10)
        items = preloaded_datastore.metric_fields_by_date(metric_name="TestMetric",
                                                          oldest=oldest,
                                                          metadata_filter={'platform': system_info.values['platform']})
        all_items = preloaded_datastore.metric_fields_by_date(metric_name="TestMetric",
                                                              oldest=oldest - datetime.timedelta(days=100))
        items_key0 = list(items.metric_data.keys())[0]
        assert len(items.metric_data[items_key0]) > 0
        assert len(items.metric_data[items_key0]) < 100
        assert len(all_items.timestamps) == 100
        sorted_items = all_items.metric_data[items_key0][-len(items.metric_data[items_key0]):]
        for index, item in enumerate(sorted_items):
            assert item in items.metric_data[items_key0]
        for index in range(1, len(all_items.timestamps)):
            assert all_items.timestamps[index-1] < all_items.timestamps[index]
        items = preloaded_datastore.metric_fields_by_date(metric_name="TestMetric",
                                                          oldest=oldest,
                                                          metadata_filter={'platform': 'no_such_platform'})
        assert items.timestamps == items.metadata
        assert items.metric_data == {}
