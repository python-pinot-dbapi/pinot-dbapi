"""
WARNING: If you're using an IDE that tinkers with the PYTHONPATH this test
module might not run at all inside of it, like in PyCharm.

This is probably related to the name we have for module "pinotdb.sqlalchemy",
which might be confusing PyCharm between that module and "sqlalchemy" (the main
module for the SQLAlchemy library).

So it's better to stick to running unit tests from the CLI instead, at least
for now.
"""

from unittest import TestCase

from sqlalchemy import Column, Integer, MetaData, Table, column, select
from sqlalchemy.engine import make_url

from pinotdb import sqlalchemy as ps


class PinotTestCase(TestCase):
    def setUp(self) -> None:
        self.dialect = ps.PinotDialect()


class PinotDialectTest(PinotTestCase):
    def test_creates_connection_args(self):
        url = make_url(
            'pinot://localhost:8000/query/sql?controller='
            'http://localhost:9000/')

        cargs, cparams = self.dialect.create_connect_args(url)

        self.assertEqual(cargs, [])
        self.assertEqual(cparams, {
            'debug': False,
            'scheme': 'http',
            'host': 'localhost',
            'port': 8000,
            'path': 'query/sql',
            'username': None,
            'password': None,
            'verify_ssl': True,
        })


class PinotCompilerTest(PinotTestCase):
    def test_can_do_simple_select(self):
        metadata = MetaData()
        table = Table(
            'some_table', metadata,
            Column('some_column', Integer)
        )
        statement = select(column('some_column')).select_from(table)

        compiler = self.dialect.statement_compiler(self.dialect, statement)

        self.assertEqual(
            str(compiler),
            'SELECT some_column \nFROM some_table',
        )

    def test_can_do_select_with_reserved_words(self):
        metadata = MetaData()
        table = Table(
            'some_table', metadata,
            Column('order', Integer)
        )
        statement = select(column('order')).select_from(table)

        compiler = self.dialect.statement_compiler(self.dialect, statement)

        self.assertEqual(
            str(compiler),
            'SELECT "order" \nFROM some_table',
        )

    def test_can_do_select_with_labels(self):
        metadata = MetaData()
        table = Table(
            'some_table', metadata,
            Column('order', Integer)
        )
        statement = select(column('order').label('ord')).select_from(table)

        compiler = self.dialect.statement_compiler(self.dialect, statement)

        self.assertEqual(
            str(compiler),
            'SELECT "order" AS ord \nFROM some_table',
        )
