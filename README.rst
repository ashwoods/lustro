lustro
======

Small python library and command line utility to introspect and mirror an Oracle database to a PostgreSQL using
`SQLAlchemy <https://www.sqlalchemy.org/>`_.

This code is in alpha state, and although it is currently being used to export a small DB from oracle to postgres,
it probably doesn't handle most types and Foreignkey dependencies correctly. It was published as a possible
starting point for anyone wanting to import or mirror data from Oracle to PostgreSQL in python.

The introspection might also work on another DB, although the type maps currently probably only work with
Oracle -> PostgreSQL.




