# -*- coding: utf-8 -*-

from sqlalchemy import MetaData, Table, Integer, create_engine
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.dialects.oracle.base import NUMBER

class DB(object):
    """Facade for the low level DB operations"""
    def __init__(self, dsn, schema=None):
        self.engine = create_engine(dsn)
        meta = MetaData()
        meta.reflect(bind=self.engine, schema=schema)
        base = automap_base(metadata=meta)
        base.prepare()
        self.base = base
        self.meta = meta

    def get_meta_table(self, key):
        return self.meta.tables.get(key)

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
            new_col = col.copy()
            if isinstance(new_col.type, NUMBER):
                new_col.type = Integer
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
        meta = MetaData()
        for table in self.source.meta.sorted_tables:
            self.source.generate_table(table, meta)
        meta.create_all(self.target.engine)

    def recreate(self, tables):
        pass

    def mirror(self, tables):
        pass

