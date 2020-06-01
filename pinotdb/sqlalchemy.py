from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from six.moves.urllib import parse

import requests
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from sqlalchemy.sql import elements
from sqlalchemy import types

import pinotdb
from pinotdb import exceptions
import logging
logger = logging.getLogger(__name__)


class PinotCompiler(compiler.SQLCompiler):
    def visit_select(self, select, **kwargs):
        return super().visit_select(select, **kwargs)


    def visit_column(self, column, result_map=None, **kwargs):
        # Pinot does not support table aliases
        if column.table:
            column.table.named_with_column = False
        result_map = result_map or kwargs.pop('add_to_result_map', None)
        # This is a hack to modify the original column, but how do I clone it ?
        column.is_literal = True
        return super().visit_column(column, result_map, **kwargs)

    def visit_label(self, label,
                    add_to_result_map=None,
                    within_label_clause=False,
                    within_columns_clause=False,
                    render_label_as_label=None,
                    **kw):
        if kw:
            render_label_as_label = kw.pop('render_label_as_label', None)
        render_label_as_label = None
        return super().visit_label(label, add_to_result_map, within_label_clause, within_columns_clause, render_label_as_label, **kw)


class PinotTypeCompiler(compiler.GenericTypeCompiler):
    def visit_REAL(self, type_, **kwargs):
        return "DOUBLE"

    def visit_NUMERIC(self, type_, **kwargs):
        return "LONG"

    visit_DECIMAL = visit_NUMERIC
    visit_INTEGER = visit_NUMERIC
    visit_SMALLINT = visit_NUMERIC
    visit_BIGINT = visit_NUMERIC
    visit_BOOLEAN = visit_NUMERIC
    visit_TIMESTAMP = visit_NUMERIC
    visit_DATE = visit_NUMERIC

    def visit_CHAR(self, type_, **kwargs):
        return "STRING"

    visit_NCHAR = visit_CHAR
    visit_VARCHAR = visit_CHAR
    visit_NVARCHAR = visit_CHAR
    visit_TEXT = visit_CHAR

    def visit_DATETIME(self, type_, **kwargs):
        raise exceptions.NotSupportedError('Type DATETIME is not supported')

    def visit_TIME(self, type_, **kwargs):
        raise exceptions.NotSupportedError('Type TIME is not supported')

    def visit_BINARY(self, type_, **kwargs):
        raise exceptions.NotSupportedError('Type BINARY is not supported')

    def visit_VARBINARY(self, type_, **kwargs):
        raise exceptions.NotSupportedError('Type VARBINARY is not supported')

    def visit_BLOB(self, type_, **kwargs):
        raise exceptions.NotSupportedError('Type BLOB is not supported')

    def visit_CLOB(self, type_, **kwargs):
        raise exceptions.NotSupportedError('Type CBLOB is not supported')

    def visit_NCLOB(self, type_, **kwargs):
        raise exceptions.NotSupportedError('Type NCBLOB is not supported')


class PinotDialect(default.DefaultDialect):

    name = 'pinot'
    scheme = 'http'
    driver = 'rest'
    preparer = compiler.IdentifierPreparer
    statement_compiler = PinotCompiler
    type_compiler = PinotTypeCompiler
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True
    supports_simple_order_by_label = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._server = None
        self._debug = False
        self.update_from_kwargs(kwargs)

    def update_from_kwargs(self, givenkw):
        kwargs = givenkw.copy() if givenkw else {}
        if 'server' in kwargs:
            self._server = kwargs.pop('server')
        kwargs['debug'] = self._debug = bool(kwargs.get('debug', False))
        logger.info(f"Updated pinot dialect args from {kwargs}: {self._server} and {self._debug}")
        return kwargs

    @classmethod
    def dbapi(cls):
        return pinotdb

    def create_connect_args(self, url):
        kwargs = {
            'host': url.host,
            'port': url.port or 9000,
            'path': url.database,
            'scheme': self.scheme,
        }
        if url.query:
            kwargs.update(url.query)

        kwargs = self.update_from_kwargs(kwargs)
        return ([], kwargs)

    def get_metadata_from_controller(self, path):
        url = parse.urljoin(self._server, path)
        r = requests.get(url, headers={'Accept':'application/json'})
        try:
            result = r.json()
        except ValueError as e:
            raise exceptions.DatabaseError(f'Got invalid json response from {self._server}:{path}: {r.text}') from e
        if self._debug:
            logger.info(f"metadata get on {self._server}:{path} returned {result}")
        return result

    def get_schema_names(self, connection, **kwargs):
        return self.get_metadata_from_controller('/schemas')

    def has_table(self, connection, table_name, schema=None):
        return table_name in self.get_table_names(connection, schema)

    def get_table_names(self, connection, schema=None, **kwargs):
        return self.get_metadata_from_controller('/tables')['tables']

    def get_view_names(self, connection, schema=None, **kwargs):
        return []

    def get_table_options(self, connection, table_name, schema=None, **kwargs):
        return {}

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        payload = self.get_metadata_from_controller(f'/tables/{table_name}/schema')

        logger.info(f"Getting columns for {table_name} from {self._server}: {payload}")
        specs = payload.get('dimensionFieldSpecs', []) + payload.get('metricFieldSpecs', []) + payload.get('dateTimeFieldSpecs', [])

        timeFieldSpec = payload.get('timeFieldSpec')
        if timeFieldSpec:
            specs.append(timeFieldSpec.get("outgoingGranularitySpec", timeFieldSpec["incomingGranularitySpec"]))

        columns = [
            {
                'name': spec['name'],
                'type': get_type(spec['dataType'], spec.get('fieldSize')),
                'nullable': True,
                'default': get_default(spec.get('defaultNullValue', 'null')),
            }
            for spec in specs
        ]

        return columns

    def get_pk_constraint(self, connection, table_name, schema=None, **kwargs):
        return {'constrained_columns': [], 'name': None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_check_constraints(
        self,
        connection,
        table_name,
        schema=None,
        **kwargs
    ):
        return []

    def get_table_comment(self, connection, table_name, schema=None, **kwargs):
        return {'text': ''}

    def get_indexes(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_unique_constraints(
        self,
        connection,
        table_name,
        schema=None,
        **kwargs
    ):
        return []

    def get_view_definition(
        self,
        connection,
        view_name,
        schema=None,
        **kwargs
    ):
        pass

    def do_rollback(self, dbapi_connection):
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        return True

    def _check_unicode_description(self, connection):
        return True


PinotHTTPDialect = PinotDialect


class PinotHTTPSDialect(PinotDialect):

    scheme = 'https'


def get_default(pinot_column_default):
    if pinot_column_default == 'null':
        return None
    else:
        return str(pinot_column_default)


def get_type(data_type, field_size):
    type_map = {
        'string': types.String,
        'int': types.BigInteger,
        'long': types.BigInteger,
        'float': types.Float,
        'double': types.Numeric,
        'bytes': types.LargeBinary,
        'boolean': types.Boolean
    }
    return type_map[data_type.lower()]
