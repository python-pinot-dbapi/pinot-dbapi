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

import responses
from sqlalchemy import Column, Integer, MetaData, Table, column, select
from sqlalchemy.engine import make_url

import pinotdb
from pinotdb import exceptions, sqlalchemy as ps


class PinotTestCase(TestCase):
    def setUp(self) -> None:
        self.dialect = ps.PinotDialect(server='http://localhost:9000')


class PinotDialectTest(PinotTestCase):
    def test_gets_pinot_db_module_as_dbapi(self):
        """
        The dialect exports the symbols necessary to implement the DBAPI 2
        protocol, so this is what's being tested here.
        """

        api = ps.PinotDialect.dbapi()

        self.assertIs(api, pinotdb)

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

    def test_creates_connection_args_without_query(self):
        url = make_url('pinot://localhost:8000/query/sql')

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

    def test_can_instantiate_with_server(self):
        dialect = ps.PinotDialect(server='http://localhost:9000/')

        self.assertEqual(dialect._controller, 'http://localhost:9000/')

    def test_gets_insecure_broker_port_by_default(self):
        self.assertEqual(self.dialect.get_default_broker_port(), 8000)

    def test_gets_secure_broker_port_from_https_dialect(self):
        self.assertEqual(ps.PinotHTTPSDialect().get_default_broker_port(), 443)

    @responses.activate
    def test_gets_metadata_from_controller(self):
        url = f'{self.dialect._controller}/some-path'
        responses.get(url, json={'foo': 'bar'})

        metadata = self.dialect.get_metadata_from_controller('some-path')

        self.assertEqual(metadata, {'foo': 'bar'})


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


class PinotTypeCompilerTest(PinotTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.compiler: ps.PinotTypeCompiler = self.dialect.type_compiler

    def test_compiles_real(self):
        self.assertEqual(self.compiler.visit_REAL(None), 'DOUBLE')

    def test_compiles_numeric(self):
        self.assertEqual(self.compiler.visit_NUMERIC(None), 'LONG')

    def test_compiles_decimal(self):
        self.assertEqual(self.compiler.visit_DECIMAL(None), 'LONG')

    def test_compiles_integer(self):
        self.assertEqual(self.compiler.visit_INTEGER(None), 'LONG')

    def test_compiles_smallint(self):
        self.assertEqual(self.compiler.visit_SMALLINT(None), 'LONG')

    def test_compiles_bigint(self):
        self.assertEqual(self.compiler.visit_BIGINT(None), 'LONG')

    # TODO: Check if this is correct (seems strange to have boolean as long).
    def test_compiles_boolean(self):
        self.assertEqual(self.compiler.visit_BOOLEAN(None), 'LONG')

    def test_compiles_timestamp(self):
        self.assertEqual(self.compiler.visit_TIMESTAMP(None), 'LONG')

    def test_compiles_date(self):
        self.assertEqual(self.compiler.visit_DATE(None), 'LONG')

    def test_compiles_char(self):
        self.assertEqual(self.compiler.visit_CHAR(None), 'STRING')

    def test_compiles_nchar(self):
        self.assertEqual(self.compiler.visit_NCHAR(None), 'STRING')

    def test_compiles_varchar(self):
        self.assertEqual(self.compiler.visit_VARCHAR(None), 'STRING')

    def test_compiles_nvarchar(self):
        self.assertEqual(self.compiler.visit_NVARCHAR(None), 'STRING')

    def test_compiles_text(self):
        self.assertEqual(self.compiler.visit_TEXT(None), 'STRING')

    def test_compiles_binary(self):
        self.assertEqual(self.compiler.visit_BINARY(None), 'BYTES')

    def test_compiles_varbinary(self):
        self.assertEqual(self.compiler.visit_VARBINARY(None), 'BYTES')

    def test_compiles_datetime(self):
        self.assertEqual(self.compiler.visit_DATETIME(None), 'TIMESTAMP')

    def test_cannot_compile_time(self):
        with self.assertRaises(exceptions.NotSupportedError):
            self.compiler.visit_TIME(None)

    def test_cannot_compile_blob(self):
        with self.assertRaises(exceptions.NotSupportedError):
            self.compiler.visit_BLOB(None)

    def test_cannot_compile_clob(self):
        with self.assertRaises(exceptions.NotSupportedError):
            self.compiler.visit_CLOB(None)

    def test_cannot_compile_nclob(self):
        with self.assertRaises(exceptions.NotSupportedError):
            self.compiler.visit_NCLOB(None)
