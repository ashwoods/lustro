# -*- coding: utf-8 -*-

import attr
import logging
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory

from collections import OrderedDict

from sqlalchemy import MetaData, Table, Column,  Integer, TIMESTAMP, DateTime, String, create_engine
from sqlalchemy.orm import Session, mapper
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import desc
from sqlalchemy.inspection import inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker


from .utils import get_or_create, create_or_update

logging.basicConfig()
configure(logger_factory=LoggerFactory())
logger = get_logger(__name__)

BASIC_TYPES = OrderedDict()
BASIC_TYPES[String] = ['length']
BASIC_TYPES[TIMESTAMP] = []
BASIC_TYPES[DateTime] = []
# BASIC_TYPES[DATETIME] = []
BASIC_TYPES[Integer] = []


@attr.s
class DB(object):
    """Facade for the low level DB operations"""

    _dsn = attr.ib()
    schema = attr.ib(default=None)

    base = attr.ib(init=False)
    meta = attr.ib(init=False)
    engine = attr.ib(init=False)
    session = attr.ib(init=False)

    def __attrs_post_init__(self):
        self.engine = create_engine(self.dsn)

        meta = MetaData()
        meta.reflect(bind=self.engine, schema=self.schema)
        self.meta = meta

        base = automap_base(metadata=self.meta)
        base.prepare()
        self.base = base
        self.session = self.get_scoped_session()

    @property
    def views(self):
        insp = Inspector.from_engine(self.engine)
        return insp.get_view_names(schema=self.schema)

    @property
    def tables(self):
        insp = Inspector.from_engine(self.engine)
        return insp.get_table_names(schema=self.schema)

    def _get_scoped_session(self):
        session_factory = sessionmaker(bind=self.engine)
        return scoped_session(session_factory)

    def _get_table_cls(self, key):
        table = self._table_factory(key)
        if table.primary_key:
            return table
        else:
            return self._table_factory(key, force_key='id')

    def table_factory(self, key, force_key=None):
        if force_key:
            force_key = Column(force_key, Integer, primary_key=True)

            return Table(
                key,
                self.meta,
                force_key,
                extend_existing=True,
                autoload=True,
                autoload_with=self.engine,
                schema=self.schema
            )
        else:
            return Table(
                key,
                self.meta,
                extend_existing=True,
                autoload=True,
                autoload_with=self.engine,
                schema=self.schema
            )

    def get_rows(self, cls, modified=None, field='modified'):
        cls = self._get_table_cls(cls)
        q = self.session.query(cls)
        if modified:
            q = q.filter(cls.columns.modified >= modified)
        if filter:
            q = q.filter(cls.columns.eigentuemer_id == 1)
        return q.all(), q.count()

    def get_latest_by(self, cls, field='modified'):
        latest = self.session.query(cls).order_by(getattr(cls,field).desc()).limit(1)
        return getattr(latest[0], field)

    @staticmethod
    def mapped_table_factory(table, meta=None):
        """Substitute db specific types for generic ones"""
        if meta is None:
            meta = MetaData()
        columns = []
        for col in table.columns.values():
            for col_type in BASIC_TYPES.keys():
                if isinstance(col.type, col_type):
                    kwarg_types = BASIC_TYPES[col_type]
                    kwargs = {}
                    for kwarg_type in kwarg_types:
                        kwargs[kwarg_type] = getattr(col.type, kwarg_type)
                    new_col = Column(
                        col.name,
                        col_type(**kwargs),
                        primary_key=col.primary_key,
                        nullable=col.nullable
                    )
                    columns.append(new_col)
        return Table(table.name, meta, *columns)


@attr.s
class Mirror(object):
    """API for cli mirroring operations"""

    _source_dsn = attr.ib()
    _target_dsn = attr.ib()

    _source_schema = attr.ib(default=None)
    _target_schema = attr.ib(default=None)

    source = attr.ib(init=False)
    target = attr.ib(init=False)

    def __attrs_post_init__(self):
        self.source = DB(self._source_dsn, self._source_schema)
        self.target = DB(self._target_dsn, self._target_schema)

    def diff(self, tables, views, modified=None):
        logger.info("Starting diff ...")
        src_session = self.source.get_session()
        trg_session = self.target.get_session()
        session_rows = []

        modified_rows = []
        created_rows = []
        for key in self.source.get_base_names():
            src_cls = self.source.get_base_class(key)
            trg_cls = self.target.get_base_class(key)
            try:
                assert trg_cls.__table__.columns.keys() == src_cls.__table__.columns.keys()
            except AssertionError:
                logger.error("Source and target database have different schemas.", exc_info=True)

            if modified is None:
                modified = self.target.get_latest_by(trg_session, cls=trg_cls)

            src_rows, count, src_session = self.source.get_rows(session=src_session, cls=src_cls, modified=modified)
            logger.info("Found %s rows modified since %s, starting diff" % (count, modified))
            for row in src_rows:
                row_dict = row.__dict__
                del row_dict['_sa_instance_state']
                pk_name = inspect(src_cls).primary_key[0].name
                pk=row_dict.pop(pk_name)
                instance = trg_session.query(trg_cls).filter_by(**{pk_name:pk}).first()
                if not instance:
                    created = True
                    instance = trg_cls(**{pk_name:pk})
                else:
                    created = False
                for k, v in row_dict.items():
                    setattr(instance, k, v)
                if created:
                    created_rows.append(instance)
                else:
                    modified_rows.append(instance)
            trg_session.add_all(modified_rows)
            trg_session.commit()

        logger.info(
                "Commiting %s/%s created/modified rows in target db" % (
                    len(created_rows), len(modified_rows)
                 )
        )

        return len(created_rows), len(modified_rows)

    def create(self, tables, views):
        all_tables = True if tables == '' else False
        meta = MetaData(bind=self.target.engine)
        for table in self.source.meta.sorted_tables:
            if all_tables or table.name in tables:
                self.source.safe_generate_table(table, meta)

        if views:
            for view in self.source.get_views():
                table = self.source.get_meta_view(view)
                self.source.safe_generate_table(table, meta)
        meta.create_all()
        self.target.set_meta(meta)

    def mirror(self, tables, views):
        src_session = self.source.get_session()
        trg_session = self.target.get_session()
        session_rows = []
        logger.info("Starting mirror db")
        for key in self.source.get_base_names():

            src_cls = self.source.get_base_class(key)
            trg_cls = self.target.get_base_class(key)
            try:
                assert trg_cls.__table__.columns.keys() == src_cls.__table__.columns.keys()
            except AssertionError:
                logger.error("Source and target database have different schemas.", exc_info=True)

            src_rows, count = self.source.get_rows(session=src_session, cls=src_cls)
            trg_rows = []
            for row in src_rows:
                row_dict = row.__dict__
                del row_dict['_sa_instance_state']
                session_rows.append(trg_cls(**row_dict))

        logger.info("Commiting %s rows to target DB" % len(session_rows))
        trg_session.add_all(session_rows)
        trg_session.commit()
        return session_rows

