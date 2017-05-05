# -*- coding: utf-8 -*-

import re
import cx_Oracle


def oracle_qualified_dsn(oracle_dsn):
    """Return a qualified orcale DSN for SQLALCHEMY; return str"""
    cred, service = oracle_dsn.rsplit('@', 1)
    host, port, db = re.split("[@/:]+", service)
    oracle_dsn = cx_Oracle.makedsn(host, port, db)
    oracle_dsn = oracle_dsn.replace('SID', 'SERVICE_NAME')
    return cred + '@' + oracle_dsn


def get_or_create(cls, session, **filters):
    """Retrieve or add object; return a tuple ``(object, is_new)``.
    ``is_new`` is True if the object already exists in the database.
    """
    instance = session.query(cls).filter_by(**filters).first()
    is_new = not instance
    if is_new:
        instance = cls(**filters)
    return instance, is_new


def create_or_update(cls, session, values={}, **filters):
    """First obtains either an existing object or a new one, based on
    ``filters``. Then applies ``values`` and returns a tuple (object, is_new).
    """
    instance, is_new = get_or_create(cls, session, **filters)
    for k, v in values.items():
        setattr(instance, k, v)
    return instance, is_new


