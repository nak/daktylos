"""
SQL implementation of `MetricStore` class
"""

import datetime
import hashlib
import logging
import sqlalchemy

from collections import OrderedDict
from daktylos.data import MetricStore, Metadata, Metric, CompositeMetric
from sqlalchemy import (
    Column,
    desc,
    exists,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    TIMESTAMP,
    Table,
    UniqueConstraint,
)
from sqlalchemy.orm import (
    relationship,
    sessionmaker,
)
from sqlalchemy.ext.declarative import declarative_base
from typing import (
    List,
    Optional,
    Tuple,
    Union, Dict,
)
__all__ = ['SQLMetricStore']

Base = declarative_base()
Session = sessionmaker()
log = logging.getLogger("SQLMetricStore")
log.setLevel(logging.WARNING)


class SQLMetadata(Base):
    """
    Metadata class representing SQL table for same
    """
    __tablename__ = "metrics_metadata"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    typ = Column(sqlalchemy.Enum(Metadata.Types))
    value = Column(Text)
    __table_args__ = (UniqueConstraint('name', 'value', name='unique1'),)

# association between metrics and metadata tables
SQLMetadataAssociationTable = Table(
    "metadata_associations", Base.metadata,
    Column("metadata_set_id", Integer, ForeignKey('metadata_sets.uuid')),
    Column("metadata_id", Integer, ForeignKey('metrics_metadata.id'))
)


class SQLMetadataSet(Base):
    """
    Class representing SQL metadata-set table
    """
    __tablename__ = "metadata_sets"
    uuid = Column(String, primary_key=True)
    data = relationship("SQLMetadata", secondary=SQLMetadataAssociationTable, cascade="all, delete")


class SQLMetric(Base):
    """
    Class representing SQL table of key/value metric pairs
    """
    __tablename__ = "metric_values"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    value = Column(Float)
    parent_id = Column(Integer, ForeignKey("composite_metrics.id"))


class SQLCompositeMetric(Base):
    """
    Class representing SQL table for composite metric sets
    """
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
    :param create: whether to create tables if the do not exist in SQL database
    :param metadata: optional set of common metadata to be applied to composite metrics or None.
    """

    singleton = None

    def __init__(self, engine, create: bool = False):
        if SQLMetricStore.singleton is not None:
            raise RuntimeError("Can only instantiate one instance of SQLMetricStore")
        SQLMetricStore.singleton = self
        if create:
            Base.metadata.create_all(engine)
        self._session = None
        self._engine = engine
        self._filters: OrderedDict[str, Tuple[MetricStore.Comparison, str]] = {}

    def __enter__(self):
        Session.configure(bind=self._engine)
        self._session = Session()
        return self

    class Query(MetricStore.Query):
        
        def __init__(self, store: "SQLMetricStore", metric_name: str, max_count: Optional[int] = None):
            super().__init__(metric_name=metric_name, count=max_count)
            self._session = store._session
            self._statement = self._session.query(SQLCompositeMetric).filter(SQLCompositeMetric.name == metric_name)
            self._joined = False
            self._max_count = max_count

        def filter_on_date(self, oldest: datetime.datetime, newest: datetime.datetime) -> "MetricStore.Query":
            self._statement = self._statement.filter(
                SQLCompositeMetric.timestamp >= oldest,
                SQLCompositeMetric.timestamp <= newest
            )

        def filter_on_metadata(self, **kwds) -> "MetricStore.Query":
            for name, value in kwds.items():
                if not self._joined:
                    self._statement = self._statement.join(SQLCompositeMetric.metrics_metadata).join(
                        SQLMetadataSet.data)
                    self._joined = True
                self._statement = self._statement.filter(SQLMetadata.name == name, SQLMetadata.value == value)
            return self
        
        def filter_on_metadata_field(self, name: str, value: int, op: MetricStore.Comparison):
            if not self._joined:
                self._statement = self._statement.join(SQLCompositeMetric.metrics_metadata).join(SQLMetadataSet.data)
                self._joined = True
            if op == MetricStore.Comparison.EQUAL:
                self._statement = self._statement.filter(SQLMetadata.name == name,
                                SQLMetadata.value == value)
            elif op == MetricStore.Comparison.NOT_EQUAL:
                self._statement = self._statement.filter(SQLMetadata.name == name,
                                SQLMetadata.value != value)
            elif op == MetricStore.Comparison.LESS_THAN:
                self._statement = self._statement.filter(SQLMetadata.name == name,
                                SQLMetadata.value < value)
            elif op == MetricStore.Comparison.GREATER_THAN:
                self._statement = self._statement.filter(SQLMetadata.name == name,
                                SQLMetadata.value > value)
            elif op == MetricStore.Comparison.LESS_THAN_OR_EQUAL:
                self._statement = self._statement.filter(SQLMetadata.name == name,
                                SQLMetadata.value <= value)
            elif op == MetricStore.Comparison.GREATER_THAN_OR_EQUAL:
                self._statement = self._statement.filter(SQLMetadata.name == name,
                                SQLMetadata.value >= value)
            else:
                raise ValueError(f"Invalid operations: {op}")
            return self
        
        def execute(self) -> MetricStore.QueryResult:
            # we order timestamps for query in descending order to filter out "the top" which are the newest items
            self._statement = self._statement.order_by(desc(SQLCompositeMetric.timestamp))
            if self._max_count:
                self._statement = self._statement.limit(self._max_count)
            sql_result: List[SQLCompositeMetric] = self._statement.all()
            result: MetricStore.QueryResult[Union[CompositeMetric, Metric]] = MetricStore.QueryResult()
            for item in reversed(sql_result):  # order timestamps from oldest to newest when returning to client
                flattened: Dict[str, float] = {}
                for child in item.children:
                    flattened[child.name] = child.value
                metadata = Metadata({data.name: data.value for data in item.metrics_metadata.data})
                result.metadata.append(metadata)
                result.timestamps.append(item.timestamp)
                result.metric_data.append(CompositeMetric.from_flattened(flattened))
            return result

    def start_query(self, metric_name: str, max_results: Optional[int] = None) -> Query:
        query = SQLMetricStore.Query(store=self, metric_name=metric_name, max_count=max_results)
        return query

    def _post_metadata(self, metadata_set: Metadata) -> SQLMetadataSet:

        m = hashlib.sha256()
        for name, value in metadata_set.values.items():
            m.update(f"{name} : {value}".encode('utf-8'))
        uuid = m.digest()
        existing =  self._session.query(SQLMetadataSet).filter(SQLMetadataSet.uuid == uuid).scalar()
        if existing is not None:
            return existing

        names = list(metadata_set.values.keys())
        existing = self._session.query(SQLMetadata).filter(SQLMetadata.name.in_(names)).all()
        existing_name_values = [(item.name, item.value) for item in existing]
        sql_metadata_set = SQLMetadataSet(uuid=uuid)
        self._session.add(sql_metadata_set)
        for name, value in metadata_set.values.items():
            if type(value) not in [str, int, float]:
                raise ValueError(f"Invalid type for metadata named {name} with type {type(value).__name__}")
            type_enum = {str: Metadata.Types.STRING,
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
        statement = self._session.query(SQLCompositeMetric)
        if self._filters:
            statement.join(SQLCompositeMetric.metrics_metadata_id).join(SQLMetadata.parent_id)
        if name is None:
            statement.filter(SQLCompositeMetric.timestamp < before)
        else:
            statement =self._session.query(SQLCompositeMetric).filter(SQLCompositeMetric.timestamp < before,
                                                           SQLCompositeMetric.name == name)
        statement.delete()
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
        statement = self._session.query(SQLCompositeMetric)
        if self._filters:
            statement = statement.join(SQLCompositeMetric.metrics_metadata_id)#.join(SQLMetadata.parent_id)
        statement = statement.filter(SQLCompositeMetric.name == name,
                             SQLCompositeMetric.timestamp <= purge_date[0])
        statement.delete()
        self._session.execute(statement)
        self._purge_orphaned_metadatsets()
        self._session.commit()

    def post(self,
             metric: Union[Metric, CompositeMetric],
             timestamp: Optional[datetime.datetime] = None,
             metadata: Optional[Metadata] = None,
             project_name: Optional[str] = None,
             uuid: Optional[str] = None):
        timestamp = timestamp or datetime.datetime.utcnow()
        if not self._session:
            raise RuntimeError("Not in context of data store.  please use 'with' statement")
        metadata_set: Optional[SQLMetadataSet] = None
        if metadata:
            metadata_set = self._post_metadata(metadata)
        key_values = metric.flatten()
        metrics = []
        for key, value in key_values.items():
            metrics.append(SQLMetric(name=key, value=str(value)))
        metric_item = SQLCompositeMetric(name=metric.name,
                                         children=metrics,
                                         timestamp=timestamp,
                                         project=project_name,
                                         uuid=uuid,
                                         metrics_metadata=metadata_set,
                                         metrics_metadata_id=metadata_set.uuid if metadata_set.uuid else None)
        self._session.add(metric_item)

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
