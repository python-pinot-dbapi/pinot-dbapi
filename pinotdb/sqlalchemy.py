from urllib import parse

import requests
from requests.auth import HTTPBasicAuth
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from sqlalchemy import types

import pinotdb
from pinotdb import exceptions
from pinotdb import keywords
import logging

import json

logger = logging.getLogger(__name__)


class PinotCompiler(compiler.SQLCompiler):
    def visit_select(self, select, **kwargs):
        return super().visit_select(select, **kwargs)

    def visit_column(self, column, result_map=None, **kwargs):
        # Pinot does not support table aliases
        if column.table is not None:
            column.table.named_with_column = False
        result_map = result_map or kwargs.pop("add_to_result_map", None)
        # This is a hack to modify the original column, but how do I clone it ?
        column.is_literal = True
        return super().visit_column(column, result_map, **kwargs)

    def escape_literal_column(self, text):
        # This is a hack to quote column names that conflict with reserved
        # words since 'column.is_literal = True'
        if text in self.preparer.reserved_words:
            return self.preparer.quote(super().escape_literal_column(text))
        return super().escape_literal_column(text)

    def visit_label(
        self,
        label,
        add_to_result_map=None,
        within_label_clause=False,
        within_columns_clause=False,
        render_label_as_label=None,
        **kw,
    ):
        return super().visit_label(
            label,
            add_to_result_map,
            within_label_clause,
            within_columns_clause,
            # Note: We force not to render labels in non-select clauses
            render_label_as_label=None,
            **kw,
        )


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

    def visit_BINARY(self, type_, **kwargs):
        return "BYTES"

    visit_VARBINARY = visit_BINARY

    def visit_DATETIME(self, type_, **kwargs):
        return "TIMESTAMP"

    def visit_TIME(self, type_, **kwargs):
        raise exceptions.NotSupportedError("Type TIME is not supported")

    def visit_BLOB(self, type_, **kwargs):
        raise exceptions.NotSupportedError("Type BLOB is not supported")

    def visit_CLOB(self, type_, **kwargs):
        raise exceptions.NotSupportedError("Type CBLOB is not supported")

    def visit_NCLOB(self, type_, **kwargs):
        raise exceptions.NotSupportedError("Type NCBLOB is not supported")


class PinotIdentifierPareparer(compiler.IdentifierPreparer):
    reserved_words = set(
        [e.lower()
         for e in (keywords.CALCITE_KEYWORDS ^ keywords.SUPERSET_KEYWORDS)]
    )

    def __init__(
        self,
        dialect,
        initial_quote='"',
        final_quote=None,
        escape_quote='"',
        omit_schema=True,
    ):
        super(PinotIdentifierPareparer, self).__init__(
            dialect,
            initial_quote,
            final_quote,
            escape_quote,
            omit_schema,
        )


def extract_table_name(fqn):
    split = fqn.split(".", 2)
    return fqn if len(split) == 1 else split[1]


class PinotDialect(default.DefaultDialect):

    name = "pinot"
    scheme = "http"
    driver = "rest"
    engine_type = "v1"
    preparer = PinotIdentifierPareparer
    statement_compiler = PinotCompiler
    type_compiler = PinotTypeCompiler
    supports_schemas = False
    supports_statement_cache = False
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
    broker_http_port = 8000
    broker_https_port = 443

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._controller = None
        self._username = None
        self._password = None
        self._debug = False
        self._verify_ssl = True
        self._database = None
        self.update_from_kwargs(kwargs)

    def update_from_kwargs(self, givenkw):
        kwargs = givenkw.copy() if givenkw else {}
        # For backward compatible
        if "server" in kwargs:
            self._controller = kwargs.pop("server")
        if "controller" in kwargs:
            self._controller = kwargs.pop("controller")
        if "username" in kwargs:
            kwargs["username"] = self._username = kwargs.pop("username")
        if "password" in kwargs:
            kwargs["password"] = self._password = kwargs.pop("password")
        if "database" in kwargs:
            kwargs["database"] = self._database = kwargs.pop("database")
        kwargs["debug"] = self._debug = bool(kwargs.get("debug", False))
        kwargs["verify_ssl"] = self._verify_ssl = (str(kwargs.get("verify_ssl", "true")).lower() in ['true'])
        logger.info(
            "Updated pinot dialect args from %s: %s and %s",
            kwargs,
            self._controller,
            self._debug,
        )
        return kwargs

    @classmethod
    def dbapi(cls):
        return pinotdb

    def get_default_broker_port(self):
        if self.scheme.lower() == "https":
            return self.broker_https_port
        return self.broker_http_port

    def create_connect_args(self, url):
        kwargs = {
            "host": url.host,
            "port": url.port or self.get_default_broker_port(),
            "path": url.database,
            "scheme": self.scheme,
            "username": url.username,
            "password": url.password,
            "verify_ssl": self._verify_ssl or True,
        }
        if self.engine_type == "multi_stage":
            kwargs.update({"use_multistage_engine": True})
        if url.query:
            kwargs.update(url.query)

        kwargs = self.update_from_kwargs(kwargs)
        return ([], kwargs)

    def get_metadata_from_controller(self, path):
        url = parse.urljoin(self._controller, path)
        r = requests.get(url, headers={"Accept": "application/json", "Database": self._database}, verify=self._verify_ssl, auth= HTTPBasicAuth(self._username, self._password))
        try:
            result = r.json()
        except ValueError as e:
            raise exceptions.DatabaseError(
                "Got invalid json response from " f"{self._controller}:{path}: {r.text}"
            ) from e
        # Skipping coverage of log lines - because covering them adds no value.
        if self._debug:  # pragma: no cover
            logger.info(
                "metadata get on %s:%s returned %s", self._controller, path, result
            )
        return result

    def get_schema_names(self, connection, **kwargs):
        if self._database:
            return [self._database]
        else:
            return ['default']

    def has_table(self, connection, table_name, schema=None):
        return table_name in self.get_table_names(connection, schema)

    def get_table_names(self, connection, schema=None, **kwargs):
        resp = self.get_metadata_from_controller("/tables")
        if 'tables' in resp:
            return list(map(extract_table_name, resp["tables"]))
        else:
            return []

    def get_view_names(self, connection, schema=None, **kwargs):
        return []

    def get_table_options(self, connection, table_name, schema=None, **kwargs):
        return {}

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        payload = self.get_metadata_from_controller(f"/tables/{table_name}/schema")

        logger.info(
            "Getting columns for %s from %s: %s", table_name, self._controller, payload
        )
        specs = (
            payload.get("dimensionFieldSpecs", [])
            + payload.get("metricFieldSpecs", [])
            + payload.get("dateTimeFieldSpecs", [])
        )

        timeFieldSpec = payload.get("timeFieldSpec")
        if timeFieldSpec:
            specs.append(
                timeFieldSpec.get(
                    "outgoingGranularitySpec", timeFieldSpec["incomingGranularitySpec"]
                )
            )

        columns = [
            {
                "name": spec["name"],
                "type": get_type(spec["dataType"], spec.get("fieldSize")),
                "nullable": True,
                "default": get_default(spec.get("defaultNullValue", "null")),
            }
            for spec in specs
        ]

        return columns

    def get_pk_constraint(self, connection, table_name, schema=None, **kwargs):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_check_constraints(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_table_comment(self, connection, table_name, schema=None, **kwargs):
        return {"text": ""}

    def get_indexes(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_view_definition(self, connection, view_name, schema=None, **kwargs):
        pass

    def do_rollback(self, dbapi_connection):
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        return True

    def _check_unicode_description(self, connection):
        return True

    # Fix for SQL Alchemy error
    def _json_deserializer(self, content: any):
        """
        This function fixes the following error from SQLAlchemy: 

        Traceback (most recent call last):
        File "...sqlalchemy/sql/sqltypes.py", line 2714, in result_processor
        json_deserializer = dialect._json_deserializer or json.loads
        AttributeError: 'PinotDialect' object has no attribute '_json_deserializer'

        The expected behavior is to simply return the passed object (called `content`) because the json.loads() method should already have been called in `db.py`. However, if the content is still one of the supported types for json.loads to deserialize, it will try to do so.
        """
        if isinstance(content, str) or isinstance(content, bytearray) or isinstance(content, bytes):
            return json.loads(content)
        else:
            return content


PinotHTTPDialect = PinotDialect


class PinotHTTPSDialect(PinotDialect):
    scheme = "https"


class PinotMultiStageDialect(PinotDialect):
    engine_type = "multi_stage"


class PinotHTTPSMultiStageDialect(PinotDialect):
    engine_type = "multi_stage"
    scheme = "https"


def get_default(pinot_column_default):
    if pinot_column_default == "null":
        return None
    else:
        return str(pinot_column_default)


# Ref to supported Pinot data types:
# https://docs.pinot.apache.org/basics/components/schema#data-types
def get_type(data_type, field_size):
    type_map = {
        "int": types.BigInteger,
        "long": types.BigInteger,
        "float": types.Float,
        "double": types.Numeric,
        # BOOLEAN, is added after release 0.7.1.
        # In release 0.7.1 and older releases, BOOLEAN is equivalent to STRING.
        "boolean": types.Boolean,
        "timestamp": types.TIMESTAMP,
        "string": types.String,
        "json": types.JSON,
        "bytes": types.LargeBinary,
        # Complex types
        "struct": types.BLOB,
        "map": types.BLOB,
        "array": types.ARRAY,
    }
    return type_map[data_type.lower()]
