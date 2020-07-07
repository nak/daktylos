import datetime
import hashlib
import logging
from collections import OrderedDict
from typing import Union, List, Optional, Tuple

import sqlalchemy
from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, ForeignKey, Table, Float, desc, UniqueConstraint, insert, \
    exists
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

from daktylos.data import MetricStore, Metadata, Metric, CompositeMetric

__all__ = ['SQLMetricStore']

Base = declarative_base()
Session = sessionmaker()
log = logging.getLogger("sql data store")
log.setLevel(logging.WARNING)


class SQLMetadata(Base):

    __tablename__ = "metrics_metadata"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    typ = Column(sqlalchemy.Enum(Metadata.Types))
    value = Column(Text)
    __table_args__ = (UniqueConstraint('name', 'value', name='unique1'),)


SQLMetadataAssociationTable = Table(
    "metadata_associations", Base.metadata,
    Column("metadata_set_id", Integer, ForeignKey('metadata_sets.uuid')),
    Column("metadata_id", Integer, ForeignKey('metrics_metadata.id'))
)


class SQLMetadataSet(Base):
    __tablename__ = "metadata_sets"
    uuid = Column(String, primary_key=True)
    data = relationship("SQLMetadata", secondary=SQLMetadataAssociationTable, cascade="all, delete")


class SQLMetric(Base):

    __tablename__ = "metric_values"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    value = Column(Float)
    parent_id = Column(Integer, ForeignKey("composite_metrics.id"))


class SQLCompositeMetric(Base):

    __tablename__ = 'composite_metrics'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    timestamp = Column(TIMESTAMP)
    project = Column(String, nullable=True)
    uuid = Column(String, nullable=True)
    children = relationship(SQLMetric, cascade="all, delete, delete-orphan")
    metrics_metadata_id = Column(String, ForeignKey(SQLMetadataSet.uuid))
    metrics_metadata = relationship("SQLMetadataSet", cascade="all, delete")


class SQLMetricStore(MetricStore):
    """
    Concrete data store class for metrics storage and retrieval, based on SQL and SqlAlchemy

    :param engine: The *sqlalchemy* engine to use, as a uri
    """
    singleton = None

    def __init__(self, engine, create: bool = False, metadata: Optional[Metadata] = None):
        if SQLMetricStore.singleton is not None:
            raise RuntimeError("Can only instantiate one instance of SQLMetricStore")
        SQLMetricStore.singleton = self
        if create:
            Base.metadata.create_all(engine)
        self._session = None
        self._engine = engine
        self._metadata = metadata
        self._filters: OrderedDict[str, Tuple[MetricStore.Comparison, str]] = {}

    def __enter__(self):
        Session.configure(bind=self._engine)
        self._session = Session()
        self._sqlmetadata = self._post_metadata(self._metadata) if self._metadata is not None else None
        return self

    def filter_on_metadata(self, name: str, value: Union[str, int, float],
                           operation: "MetricStore.Comparison" = MetricStore.Comparison.EQUAL) -> "MetricStore":
        if name in self._filters and operation in self._filters[name]:
            raise ValueError(f"Filter on {name} with operation {operation} already exists")
        self._filters[name] = (operation, value)

    def clear_filter(self, name: Optional[str] = None):
        if name is None:
            self._filters = []
        else:
            if name in self._filters:
                del self._filters[name]

    def _apply_filters(self, statement):
        for name, (op, value) in self._filters.items():
            if op == MetricStore.Comparison.EQUAL:
                statement = statement.filter(SQLMetadata.name == name,
                                SQLMetadata.value == value)
            elif op == MetricStore.Comparison.NOT_EQUAL:
                statement = statement.filter(SQLMetadata.name == name,
                                SQLMetadata.value != value)
            elif op == MetricStore.Comparison.LESS_THAN:
                statement = statement.filter(SQLMetadata.name == name,
                                SQLMetadata.value < value)
            elif op == MetricStore.Comparison.GREATER_THAN:
                statement = statement.filter(SQLMetadata.name == name,
                                SQLMetadata.value > value)
            elif op == MetricStore.Comparison.LESS_THAN_OR_EQUAL:
                statement = statement.filter(SQLMetadata.name == name,
                                SQLMetadata.value <= value)
            elif op == MetricStore.Comparison.GREATER_THAN_OR_EQUAL:
                statement = statement.filter(SQLMetadata.name == name,
                                SQLMetadata.value >= value)
            else:
                raise ValueError(f"Invalid operations: {op}")
        return statement

    def _post_metadata(self, metadata_set: Metadata):

        m = hashlib.sha256()
        for name, value in metadata_set.values.items():
            m.update(f"{name} : {value}".encode('utf-8'))
        uuid = m.digest()
        if self._session.query(SQLMetadataSet).filter(SQLMetadataSet.uuid == uuid).scalar() is not None:
            return

        names = list(metadata_set.values.keys())
        existing = self._session.query(SQLMetadata).filter(SQLMetadata.name.in_(names)).all()
        existing_name_values = [(item.name, item.value) for item in existing]
        sql_metadata_set = SQLMetadataSet(uuid=uuid)
        self._session.add(sql_metadata_set)
        for name, value in metadata_set.values.items():
            if type(value) not in [str, int, float]:
                raise ValueError(f"Invalid type for metadata named {name} with type {type(value).__name__}")
            type_enum = {str: Metadata.Types.STRING,
                         float: Metadata.Types.FLOAT,
                         int: Metadata.Types.INTEGER}[type(value)]
            if (name, value) not in existing_name_values:
                metadata = SQLMetadata(name=name, value=str(value), typ=type_enum)
                sql_metadata_set.data.append(metadata)
        self._session.commit()
        return sql_metadata_set

    def _purge_orphaned_metadatsets(self):
        orphaned = self._session.query(SQLMetadataSet).filter(~ exists().where(
            SQLMetadataSet.uuid == SQLCompositeMetric.metrics_metadata_id
        )).all()
        for orphan in orphaned:
            self._session.delete(orphan)

    def purge_by_date(self, before: datetime.datetime, name: Optional[str] = None):
        if not self._session:
            raise RuntimeError("Not in context of data store.  please use 'with' statement")
        stmnt = self._session.query(SQLCompositeMetric)
        if self._filters:
            stmnt.join(SQLCompositeMetric.metrics_metadata_id).join(SQLMetadata.parent_id)
        if name is None:
            stmnt.filter(SQLCompositeMetric.timestamp < before)
        else:
            stmnt =self._session.query(SQLCompositeMetric).filter(SQLCompositeMetric.timestamp < before,
                                                           SQLCompositeMetric.name == name)
        stmnt = self._apply_filters(stmnt)
        stmnt.delete()
        self._purge_orphaned_metadatsets()
        self._session.commit()

    def purge_by_volume(self, count: int, name: str):
        if not self._session:
            raise RuntimeError("Not in context of data store.  please use 'with' statement")
        try:
            purge_date = self._session.query(SQLCompositeMetric.timestamp).filter(SQLCompositeMetric.name == name).\
                order_by(SQLCompositeMetric.timestamp).limit(count).all()[-1]
        except IndexError:
            return
        stmnt = self._session.query(SQLCompositeMetric)
        if self._filters:
            stmnt = stmnt.join(SQLCompositeMetric.metrics_metadata_id)#.join(SQLMetadata.parent_id)
        stmnt = stmnt.filter(SQLCompositeMetric.name == name,
                             SQLCompositeMetric.timestamp <= purge_date[0])
        stmnt = self._apply_filters(stmnt)
        stmnt.delete()
        self._session.execute(stmnt)
        self._purge_orphaned_metadatsets()
        self._session.commit()

    def post(self, metric: Union[Metric, CompositeMetric], timestamp: datetime.datetime = datetime.datetime.utcnow(),
             project_name: Optional[str] = None, uuid: Optional[str] = None):
        if not self._session:
            raise RuntimeError("Not in context of data store.  please use 'with' statement")
        key_values = metric.flatten()
        metrics = []
        for key, value in key_values.items():
            metrics.append(SQLMetric(name=key, value=str(value)))
        metric_item = SQLCompositeMetric(name=metric.name, children=metrics, timestamp=timestamp,
                                         metrics_metadata=self._sqlmetadata,
                                         metrics_metadata_id=self._sqlmetadata.uuid if self._metadata else None)
        self._session.add(metric_item)

    def metrics_by_date(self, metric_name: str, oldest: datetime.datetime,
                        newest: datetime.datetime = datetime.datetime.utcnow()) -> Union[
        List[CompositeMetric], List[Metric]]:
        if not self._session:
            raise RuntimeError("Not in context of data store.  please use 'with' statement")
        stmnt = self._session.query(SQLCompositeMetric)
        if self._filters:
            stmnt.join(SQLCompositeMetric.metrics_metadata).join(SQLMetadataSet.data)
        stmnt = stmnt.filter(
            SQLCompositeMetric.name == metric_name,
            SQLCompositeMetric.timestamp >= oldest
        )
        stmnt = self._apply_filters(stmnt)
        result = stmnt.order_by(desc(SQLCompositeMetric.timestamp)).all()
        return result

    def metrics_by_volume(self, metric_name: str, count: int) -> Union[
        List[SQLCompositeMetric], List[SQLMetric]]:
        """
        Get a set of at most *count* most recent metrics

        :param metric_name: name of metric
        :param count: maximum number of items to fetch
        :return: list of requested [composite] metrics
        """
        if not self._session:
            raise RuntimeError("Not in context of data store.  please use 'with' statement")
        stmnt = self._session.query(SQLCompositeMetric)
        if self._filters:
            stmnt.join(SQLCompositeMetric.metrics_metadata).join(SQLMetadataSet.data)
        stmnt.filter(
            SQLCompositeMetric.name == metric_name,
        )
        stmnt = self._apply_filters(stmnt)
        result = stmnt.order_by(desc(SQLCompositeMetric.timestamp)).limit(count).all()
        return result

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            super().__exit__(exc_type, exc_val, exc_tb)
        finally:
            self.__class__.singleton = None

    def commit(self) -> None:
        """
        Commit all changes accumulated thus far
        """
        if not self._session:
            raise RuntimeError("Not in context of data store.  please use 'with' statement")
        self._session.commit()
