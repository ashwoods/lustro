# -*- coding: utf-8 -*-

from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base


class DB(object):
    """Facade for the low level DB operations"""
    def __init__(self, dsn, schema=None):
        self.engine = create_engine(dsn)
        self.meta = MetaData()
        self.meta.reflect(bind=self.engine, schema=schema)
        self.base = automap_base(metadata=self.meta)
        self.base.prepare()

    def get_classes(self):
        return self.base.classes

    def get_session(self):
        return Session(self.engine)

    def get_rows(self, session, cls, modified=None):
        return session.query(cls).all()


class Mirror(object):
    """API for cli mirroring operations"""
    def __init__(self, source, target, source_schema=None, target_schema=None):
        self.source = DB(source, source_schema)
        self.target = DB(target, target_schema)

    def diff(self, tables, modified):
        pass

    def create(self, tables):
        pass

    def recreate(self, tables):
        pass

    def mirror(self, tables):
        pass

