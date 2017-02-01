# -*- coding: utf-8 -*-

import re
import os
import sys

import cx_Oracle


def oracle_qualified_dsn(oracle_dsn):
    cred, service = oracle_dsn.rsplit('@', 1)
    host, port, db = re.split("[@/:]+", service)
    oracle_dsn = cx_Oracle.makedsn(host, port, db)
    oracle_dsn = oracle_dsn.replace('SID', 'SERVICE_NAME')
    return cred + '@' + oracle_dsn





