# -*- coding: utf-8 -*-

from sqlalchemy import MetaData, Table, Column,  Integer, TIMESTAMP, DATETIME, String, create_engine
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base


BASIC_TYPES = {
    String: ['length'],
  #  TIMESTAMP: [],
    DATETIME: [],
    Integer: [],
}


class DB(object):
    """Facade for the low level DB operations"""
    def __init__(self, dsn, schema=None):
        self.engine = create_engine(dsn)
        self.base = None
        self.meta = None

        meta = MetaData()
        meta.reflect(bind=self.engine, schema=schema)

        self.set_meta(meta)

    def get_meta_table(self, key):
        return self.meta.tables.get(key)

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

    def get_rows(self, session, cls, modified=None):
        return session.query(cls).all()

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

    def diff(self, tables, modified):
        pass

    def create(self, tables):
        meta = MetaData(bind=self.target.engine)
        for table in self.source.meta.sorted_tables:
            self.source.safe_generate_table(table, meta)
        meta.create_all()
        self.target.set_meta(meta)

    def recreate(self, tables):
        pass

    def mirror(self, tables):
        src_session = self.source.get_session()
        trg_session = self.target.get_session()

        for key in self.source.get_base_names():
            src_cls = self.source.get_base_class(key)
            trg_cls = self.target.get_base_class(key)

            rows = self.source.get_rows(session=src_session, cls=src_cls)

            trg_cls.__table__.columns.keys() == src_cls.__table__.columns.keys()

