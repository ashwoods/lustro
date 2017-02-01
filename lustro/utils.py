# -*- coding: utf-8 -*-

import re
import os
import sys

import cx_Oracle


def oracle_qualified_dsn(oracle_dsn):
    *cred, host, port, service = re.split("[@/:]+", oracle_dsn)[3:]
    cred = '%s://%s:%s@' % tuple(cred)
    return cred + cx_Oracle.makedsn(host, port, service)





