"""
SQL implementation of `MetricStore` class
"""

import datetime
import hashlib
import logging
import sqlalchemy

from collections import OrderedDict

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.functions import count, max as max_

from daktylos.data import MetricStore, Metadata, Metric, CompositeMetric, MDC, MetricDataClass, MetricDataClassT
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
    UniqueConstraint, or_,
)
from sqlalchemy.orm import (
    relationship,
    sessionmaker, aliased,
)
from sqlalchemy.ext.declarative import declarative_base
from typing import (
    List,
    Optional,
    Tuple,
    Union, Dict, Type,
)
__all__ = ['SQLMetricStore']

Base = declarative_base()
Session = sessionmaker()
log = logging.getLogger("SQLMetricStore")
log.setLevel(logging.WARNING)


class SQLMetadata(Base):
    """
    Class representing SQL table for metadata.
    This is a lowlevel table, with id being the unique key to cross-reference
    into an association table for sets of metadata
    """
    __tablename__ = "metrics_metadata"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    typ = Column(sqlalchemy.Enum(Metadata.Types))
    value = Column(Text)
    __table_args__ = (UniqueConstraint('name', 'value', name='unique1'),)


# association between metrics and sets of metadata
# Each metric will reference a set of metadata and this tble refernnces the id for
# each set into the child metadata items in `SQLMetaddata`.  Note the SQLMetadata key/value
# pairs are constrained to be unique, minimizing overall storage needs
SQLMetadataAssociationTable = Table(
    "metadata_associations", Base.metadata,
    Column("metadata_set_id", Integer, ForeignKey('metadata_sets.uuid')),
    Column("metadata_id", Integer, ForeignKey('metrics_metadata.id'))
)


class SQLMetadataSet(Base):
    """
    Class representing a set of key/value pairs of metadata in a SQL table.
    The uuid is constructed from a hash across all key/value pairs so that like sets are
    not duplicated in this table (small storage savings)
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
    __table_args__ = (UniqueConstraint('name', 'value', name='unique_metric'),)


class SQLCompositeMetric(Base):
    """
    Class representing SQL table for composite metric (a collection of key/value metric pairs
    indexed int `SqlMetric` table)
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
    """

    singleton = None

    def __init__(self, engine: sqlalchemy.engine.base.Engine, create: bool = False):
        if SQLMetricStore.singleton is not None:
            raise RuntimeError("Can only instantiate one instance of SQLMetricStore")
        SQLMetricStore.singleton = self
        if create:
            Base.metadata.create_all(engine)
        self._session = None
        self._engine = engine
        self._filters: OrderedDict[str, Tuple[MetricStore.Comparison, str]] = OrderedDict()

    def __enter__(self):
        """
        configure session (binding to engine) and start SQL session

        :return: self
        """
        Session.configure(bind=self._engine)
        self._session = Session()
        return self

    class BaseQuery(MetricStore.Query[MDC]):
        """
        Concrtete implementation of a SQL query interface
        """
        
        def __init__(self, store: "SQLMetricStore", metric_name: str, max_count: Optional[int] = None):
            super().__init__(metric_name=metric_name, max_count=max_count)
            self._session = store._session
            self._statement = self._session.query(SQLCompositeMetric).filter(SQLCompositeMetric.name == metric_name)
            self._max_count = max_count
            self._joined = False

        def filter_on_date(self, oldest: datetime.datetime, newest: datetime.datetime) \
                -> MetricStore.Query[MDC]:
            self._statement = self._statement.filter(
                SQLCompositeMetric.timestamp >= oldest,
                SQLCompositeMetric.timestamp <= newest
            )
            return self

        def filter_on_metadata(self, **kwds) -> "MetricStore.Query":
            alias = aliased(SQLMetadata, name="metadata_1")
            alias_set = aliased(SQLMetadataSet, name="metadata_set_1")
            if not self._joined:
                self._statement = self._statement.join(alias_set,
                                                       alias_set.uuid == SQLCompositeMetric.metrics_metadata_id)
                self._joined = True
            for name, value in kwds.items():
                self._statement = self._statement.filter(
                    self._session.query(SQLMetadataAssociationTable).join(
                        alias,
                        SQLMetadataAssociationTable.columns.metadata_set_id == alias_set.uuid).filter(
                        SQLMetadataAssociationTable.columns.metadata_id == alias.id).filter(
                        alias.name == name).filter(
                        alias.value == value).exists()
                )
            return self
        
        def filter_on_metadata_field(self, name: str, value: int, op: MetricStore.Comparison):
            alias = aliased(SQLMetadata, name='metadata_field_' + name)
            alias_set = aliased(SQLMetadataSet, name="metadata_set_1")
            if not self._joined:
                self._statement = self._statement.join(alias_set,
                                                       alias_set.uuid == SQLCompositeMetric.metrics_metadata_id)
                self._joined = True
            query = self._session.query(SQLMetadataAssociationTable).join(
                    alias,
                    SQLMetadataAssociationTable.columns.metadata_set_id == alias_set.uuid).filter(
                    SQLMetadataAssociationTable.columns.metadata_id == alias.id)
            if op == MetricStore.Comparison.EQUAL:
                query = query.filter(alias.name == name, alias.value == value)
            elif op == MetricStore.Comparison.NOT_EQUAL:
                query = query.filter(alias.name == name, alias.value != value)
            elif op == MetricStore.Comparison.LESS_THAN:
                query = query.filter(alias.name == name, alias.value < value)
            elif op == MetricStore.Comparison.GREATER_THAN:
                query = query.filter(alias.name == name, alias.value > value)
            elif op == MetricStore.Comparison.LESS_THAN_OR_EQUAL:
                query = query.filter(alias.name == name, alias.value <= value)
            elif op == MetricStore.Comparison.GREATER_THAN_OR_EQUAL:
                query = query.filter(alias.name == name, alias.value >= value)
            else:
                raise ValueError(f"Invalid operations: {op}")
            self._statement = self._statement.filter(query.exists())
            return self

    class CompositeQuery(BaseQuery[CompositeMetric]):
        def execute(self) -> MetricStore.QueryResult[List[CompositeMetric]]:
            # we order timestamps for query in descending order to filter out "the top" which are the newest items
            self._statement = self._statement.order_by(desc(SQLCompositeMetric.timestamp))
            if self._max_count:
                self._statement = self._statement.limit(self._max_count)
            sql_result: List[SQLCompositeMetric] = self._statement.all()
            result: MetricStore.QueryResult[List[Union[CompositeMetric, Metric]]] =\
                MetricStore.QueryResult()
            for item in reversed(sql_result):  # order timestamps from oldest to newest when returning to client
                flattened: Dict[str, float] = {}
                for child in item.children:
                    flattened[child.name] = child.value
                metadata = Metadata({data.name: data.value for data in item.metrics_metadata.data})
                result.metadata.append(metadata)
                result.timestamps.append(item.timestamp)
                result.metric_data.append(CompositeMetric.from_flattened(flattened))
            return result

    class DataclassQuery(BaseQuery[MetricDataClassT]):

        def __init__(self, store: "SQLMetricStore", typ: Type[MetricDataClass], metric_name: str,
                     max_count: Optional[int] = None):
            super().__init__(store=store, metric_name=metric_name, max_count=max_count)
            self._type = typ

        def execute(self) -> MetricStore.QueryResult[MetricDataClassT]:
            # we order timestamps for query in descending order to filter out "the top" which are the newest items
            self._statement = self._statement.order_by(desc(SQLCompositeMetric.timestamp))
            if self._max_count:
                self._statement = self._statement.limit(self._max_count)
            sql_result: List[SQLCompositeMetric] = self._statement.all()
            result: MetricStore.QueryResult[MetricDataClassT] = MetricStore.QueryResult()
            for item in reversed(sql_result):  # order timestamps from oldest to newest when returning to client
                flattened: Dict[str, float] = {}
                for child in item.children:
                    flattened[child.name] = child.value
                metadata = Metadata({data.name: data.value for data in item.metrics_metadata.data})
                result.metadata.append(metadata)
                result.timestamps.append(item.timestamp)
                result.metric_data.append(CompositeMetric.from_flattened(flattened).to_dataclass(self._type))
            return result

    class FieldQuery(BaseQuery[Dict[str, List[float]]]):
        """
        Concrete SQL implementation of a database query interface
        """

        def __init__(self, store: "SQLMetricStore", metric_name: str, fields: Optional[List[str]] = None,
                     max_count: Optional[int] = None):
            super().__init__(store=store, metric_name=metric_name, max_count=max_count)
            # override:
            self._statement = self._session.query(SQLMetric.name, SQLMetric.value, SQLCompositeMetric.timestamp,
                                                  SQLCompositeMetric.metrics_metadata_id, SQLCompositeMetric.id,
                                                  ).join(
                    SQLCompositeMetric,
                    SQLCompositeMetric.id == SQLMetric.parent_id
                ).filter(SQLCompositeMetric.name == metric_name)
            if fields:
                def condition(f: str):
                    if any(['*' in f, '_' in f, '%' in f, '[' in f and ']' in f,  '^' in f]):
                        if f.startswith('!'):
                            return SQLMetric.name.notlike(f[1:])
                        else:
                            return SQLMetric.name.like(f)
                    else:
                        return SQLMetric.name == f
                queries = [condition(field) for field in fields]
                self._statement = self._statement.filter(or_(*queries))
            self._metadata_filter = {}

        def execute(self) -> MetricStore.QueryResult[Dict[str, List[float]]]:
            self._statement = self._statement.order_by(desc(SQLCompositeMetric.timestamp))
            if self._max_count:
                query = self._session.query(SQLCompositeMetric.id).order_by(desc(SQLCompositeMetric.timestamp)).limit(self._max_count)
                self._statement = self._statement.filter(SQLCompositeMetric.id.in_(query))
            sql_result: List[SQLCompositeMetric] = self._statement.all()
            result: MetricStore.QueryResult[Dict[str, List[float]]] = MetricStore.QueryResult()
            result.metric_data = {}  # correction on default type/value
            by_id = OrderedDict()
            for name, value, timestamp, uuid, group_id in reversed(sql_result):
                alias = aliased(SQLMetadata, name='metadata_field_' + name)
                if group_id not in by_id:
                    query = self._session.query(SQLMetadataSet).filter(SQLMetadataSet.uuid == uuid)
                    for metadata_name, (metadata_value, op) in self._metadata_filter.items():
                        if op == MetricStore.Comparison.EQUAL:
                            query = query.filter(alias.name == metadata_name, alias.value == metadata_value)
                        elif op == MetricStore.Comparison.NOT_EQUAL:
                            query = query.filter(alias.name == metadata_name, alias.value != metadata_value)
                        elif op == MetricStore.Comparison.LESS_THAN:
                            query = query.filter(alias.name == metadata_name, alias.value < metadata_value)
                        elif op == MetricStore.Comparison.GREATER_THAN:
                            query = query.filter(alias.name == metadata_name, alias.value > metadata_value)
                        elif op == MetricStore.Comparison.LESS_THAN_OR_EQUAL:
                            query = query.filter(alias.name == metadata_name, alias.value <= metadata_value)
                        elif op == MetricStore.Comparison.GREATER_THAN_OR_EQUAL:
                            query = query.filter(alias.name == metadata_name, alias.value >= metadata_value)
                        else:
                            raise ValueError(f"Invalid operations: {op}")
                    try:
                        sql_metadata = query.one()
                        metadata = Metadata({})
                        for r in sql_metadata.data:
                            metadata.values[r.name] = r.value
                        by_id[group_id] = timestamp, metadata, {name: value}
                    except NoResultFound:
                        pass
                else:
                    by_id[group_id][2][name] = value
            for (timestamp, metadata, metrics_table) in by_id.values():
                result.timestamps.append(timestamp)
                result.metadata.append(metadata)
                for name, value in metrics_table.items():
                    result.metric_data.setdefault(name, [])
                    result.metric_data[name].append(value)

            return result

        def filter_on_metadata(self, **kwds) -> "MetricStore.Query":
            self._metadata_filter.update({name: (value, MetricStore.Comparison.EQUAL) for name, value in kwds.items()})
            return self

        def filter_on_metadata_field(self, name: str, value: int, op: MetricStore.Comparison):
            self._metadata_filter.update({name: (value, op)})
            return self

    def start_query(self, metric_name: str, max_results: Optional[int] = None) -> CompositeQuery:
        query = SQLMetricStore.CompositeQuery(store=self, metric_name=metric_name, max_count=max_results)
        return query

    def start_dataclass_query(self, typ: Type[MetricDataClass], metric_name: str, max_results: Optional[int])\
            -> DataclassQuery:
        query = SQLMetricStore.DataclassQuery(store=self, metric_name=metric_name, max_count=max_results)
        return query

    def start_field_query(self, metric_name: str, fields: Optional[List[str]], max_results: Optional[int] = None)\
            -> FieldQuery:
        query = SQLMetricStore.FieldQuery(store=self, metric_name=metric_name, max_count=max_results, fields=fields)
        return query

    def _post_metadata(self, metadata_set: Metadata) -> SQLMetadataSet:
        """
        Private method to post a set of metadata
        :param metadata_set: set of metadata to post to database

        :return: The SQLMetdataSet object created after successful entry into database
        """
        # derive a unique hash value across all name/value pairs
        m = hashlib.sha256()
        for name, value in metadata_set.values.items():
            m.update(f"{name} : {value}".encode('utf-8'))
        uuid = m.digest()
        existing = self._session.query(SQLMetadataSet).filter(SQLMetadataSet.uuid == uuid).scalar()
        if existing is not None:
            # if uuid exists in database, we are done
            # Todo: probably should query for key/value pairs on existing id o ensure no collisions(?)
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

    def _purge_orphaned_metadatsets(self) -> None:
        """
        purge any metadata sets not referenced by a composit metric
        """
        orphaned = self._session.query(SQLMetadataSet).filter(~ exists().where(
            SQLMetadataSet.uuid == SQLCompositeMetric.metrics_metadata_id
        )).all()
        for orphan in orphaned:
            self._session.delete(orphan)

    def purge_by_date(self, before: datetime.datetime, name: Optional[str] = None) -> None:
        if not self._session:
            raise RuntimeError("Not in context of data store.  please use 'with' statement")
        statement = self._session.query(SQLCompositeMetric)
        if self._filters:
            statement.join(SQLCompositeMetric.metrics_metadata_id).join(SQLMetadata.parent_id)
        if name is None:
            statement.filter(SQLCompositeMetric.timestamp < before)
        else:
            statement = self._session.query(SQLCompositeMetric).filter(SQLCompositeMetric.timestamp < before,
                                                                       SQLCompositeMetric.name == name)
        statement.delete()
        self._purge_orphaned_metadatsets()
        self._session.commit()

    def purge_by_volume(self, count: int, name: str) -> None:
        if not self._session:
            raise RuntimeError("Not in context of data store.  please use 'with' statement")
        try:
            purge_date = self._session.query(SQLCompositeMetric.timestamp).filter(SQLCompositeMetric.name == name).\
                order_by(SQLCompositeMetric.timestamp).limit(count).all()[-1]
        except IndexError:
            return
        statement = self._session.query(SQLCompositeMetric)
        if self._filters:
            statement = statement.join(SQLCompositeMetric.metrics_metadata_id)
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
