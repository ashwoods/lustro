# -*- coding: utf-8 -*-

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



class DB(object):
    """Facade for the low level DB operations"""
    def __init__(self, dsn, schema=None):
        self.engine = create_engine(dsn)
        self.base = None
        self.meta = None
        self.schema = schema

        meta = MetaData()
        meta.reflect(bind=self.engine, schema=schema)

        self.set_meta(meta)



    def get_views(self):
        insp = Inspector.from_engine(self.engine)
        return insp.get_view_names(schema=self.schema)

    def get_meta_table(self, key):
        return self.meta.tables.get(key)

    def get_meta_view(self, key, force_key=None):
        #if force_key:
        #    force_key = Column(force_key, Integer, primary_key=True)
        return Table(key, self.meta, Column('id', Integer, primary_key=True), extend_existing=True, autoload=True, autoload_with=self.engine, schema=self.schema)

    def set_meta(self, meta):
        self.meta = meta
        base = automap_base(metadata=self.meta)
        base.prepare()
        self.base = base

    def get_meta_names(self):
        return self.meta.classes.keys()

    def get_base_class(self, key):
        return self.base.classes.get(key)

    def get_base_names(self):
        return self.base.classes.keys()

    def get_session(self):
        return Session(self.engine)

    def get_scoped_session(self):
        session_factory = sessionmaker(bind=self.engine)
        return scoped_session(session_factory)

    def get_rows(self, session, cls, modified=None, field='modified'):
        if modified:
            query = session.query(cls).filter(getattr(cls, field) >= modified)
        else:
            query = session.query(cls)
        return query.all(), query.count(), session

    def get_latest_by(self, session, cls, field='modified'):
        latest = session.query(cls).order_by(getattr(cls,field).desc()).limit(1)
        return getattr(latest[0], field)

    def generate_table(self, table, meta=None):
        if meta is None:
            meta = MetaData()
        columns = []
        for col in table.columns.values():
            # This is an ugly hack but worked for the three types I
            # had to deal with. To make it more robust, you would want
            # to go up the class __mro__ until you get a match for the types
            # that you want swap
            new_col = col.copy()
            if isinstance(col.type, Integer):
                new_col.type = Integer
            if isinstance(col.type, DATETIME):
                new_col.type = DATETIME
            if isinstance(col.type, TIMESTAMP):
                new_col.type = TIMESTAMP
            columns.append(new_col)
        return Table(table.name, meta, *columns)

    def safe_generate_table(self, table, meta=None):
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


class Mirror(object):
    """API for cli mirroring operations"""
    def __init__(self, source, target, source_schema=None, target_schema=None):
        self.source = DB(source, source_schema)
        self.target = DB(target, target_schema)

    def diff(self, tables, modified=None):
        logger.info("Starting diff ...")
        src_session = self.source.get_session()
        trg_session = self.target.get_scoped_session()
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

            #if modified is None:
            #    modified = self.target.get_latest_by(trg_session, cls=trg_cls)
            import datetime
            modified = datetime.datetime(2017, 4 ,24)

            src_rows, count, src_session = self.source.get_rows(session=src_session, cls=src_cls, modified=modified)
            logger.info("Found %s rows modified since %s, starting diff" % (count, modified))
            for row in src_rows:

                row_dict = row.__dict__
                del row_dict['_sa_instance_state']
                if 'veroeff_status' in row_dict.keys():
                    if row_dict['veroeff_status'] == 3:
                        break
                pk_name = inspect(src_cls).primary_key[0].name
                pk=row_dict.pop(pk_name)
                obj, created = create_or_update(
                        trg_cls,
                        trg_session,
                        values=row_dict,
                        **{pk_name:pk})

                if created:
                    created_rows.append(obj)
                else:
                    modified_rows.append(obj)
        logger.info(
            "Commiting %s/%s created/modified rows in target db" % (
                len(created_rows), len(modified_rows)
            )
        )
        import ipdb; ipdb.set_trace()
        trg_session.add_all(created_rows)
        trg_session.add_all(modified_rows)
        trg_session.commit()
        return len(created_rows), len(modified_rows)


    def diff_views(self, tables, modified=None):
        logger.info("Starting diff ...")
        src_session = self.source.get_session()
        trg_session = self.target.get_session()
        session_rows = []

        modified_rows = []
        created_rows = []
        for key in tables:
            src_cls = self.source.get_meta_view(key)
            trg_cls = self.target.get_base_class(key)
            #try:
            #    assert trg_cls.columns.keys() == src_cls.columns.keys()
            #except AssertionError:
            #    logger.error("Source and target database have different schemas.", exc_info=True)

            if modified is None:
                try:
                    modified = self.target.get_latest_by(trg_session, cls=trg_cls)
                except AttributeError:
                    pass
            src_rows, count, trg_session = self.source.get_rows(session=src_session, cls=src_cls, modified=modified)

            logger.info("Found %s rows modified since %s, starting diff" % (count, modified))
            for row in src_rows:
                row_dict = dict(zip(src_cls.columns.keys(), row))
                pk_name='id'
                pk=row_dict.pop(pk_name)
                obj, created, trg_session = create_or_update(
                        trg_cls,
                        trg_session,
                        values=row_dict,
                        **{pk_name:pk})

                if created:
                    created_rows.append(obj)
                else:
                    modified_rows.append(obj)
        logger.info(
            "Commiting %s/%s created/modified rows in target db" % (
                len(created_rows), len(modified_rows)
            )
        )
        trg_session.commit()
        return len(created_rows), len(modified_rows)


    def create(self, tables, views=False):
        meta = MetaData(bind=self.target.engine)
        for table in self.source.meta.sorted_tables:
            self.source.safe_generate_table(table, meta)

        if views:
            for view in self.source.get_views():
                pass
        meta.create_all()
        self.target.set_meta(meta)

    def recreate(self, tables, views=True):
        all_tables = True if tables == '' else False
        meta = MetaData(bind=self.target.engine)
        for table in self.source.meta.sorted_tables:
            if all_tables or table.name in tables:
                self.source.safe_generate_table(table, meta)

        if views:
            for view in self.source.get_views():
                if all_tables or view in tables:
                    table = self.source.get_meta_view(view)
                    self.source.safe_generate_table(table, meta)
        meta.create_all()
        self.target.set_meta(meta)

    def mirror(self, tables):
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

