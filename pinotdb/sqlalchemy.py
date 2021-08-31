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


CALCITE_KEYWORDS = set([
    'A', 'ABS', 'ABSOLUTE', 'ACTION',
    'ADA', 'ADD', 'ADMIN', 'AFTER',
    'ALL', 'ALLOCATE', 'ALLOW', 'ALTER',
    'ALWAYS', 'AND', 'ANY', 'APPLY',
    'ARE', 'ARRAY', 'ARRAY_MAX_CARDINALITY', 'AS',
    'ASC', 'ASENSITIVE', 'ASSERTION', 'ASSIGNMENT',
    'ASYMMETRIC', 'AT', 'ATOMIC', 'ATTRIBUTE',
    'ATTRIBUTES', 'AUTHORIZATION', 'AVG', 'BEFORE',
    'BEGIN', 'BEGIN_FRAME', 'BEGIN_PARTITION', 'BERNOULLI',
    'BETWEEN', 'BIGINT', 'BINARY', 'BIT',
    'BLOB', 'BOOLEAN', 'BOTH', 'BREADTH',
    'BY', 'C', 'CALL', 'CALLED',
    'CARDINALITY', 'CASCADE', 'CASCADED', 'CASE',
    'CAST', 'CATALOG', 'CATALOG_NAME', 'CEIL',
    'CEILING', 'CENTURY', 'CHAIN', 'CHAR',
    'CHARACTER', 'CHARACTERISTICS', 'CHARACTERS', 'CHARACTER_LENGTH',
    'CHARACTER_SET_CATALOG', 'CHARACTER_SET_NAME', 'CHARACTER_SET_SCHEMA', 'CHAR_LENGTH',
    'CHECK', 'CLASSIFIER', 'CLASS_ORIGIN', 'CLOB',
    'CLOSE', 'COALESCE', 'COBOL', 'COLLATE',
    'COLLATION', 'COLLATION_CATALOG', 'COLLATION_NAME', 'COLLATION_SCHEMA',
    'COLLECT', 'COLUMN', 'COLUMN_NAME', 'COMMAND_FUNCTION',
    'COMMAND_FUNCTION_CODE', 'COMMIT', 'COMMITTED', 'CONDITION',
    'CONDITION_NUMBER', 'CONNECT', 'CONNECTION', 'CONNECTION_NAME',
    'CONSTRAINT', 'CONSTRAINTS', 'CONSTRAINT_CATALOG', 'CONSTRAINT_NAME',
    'CONSTRAINT_SCHEMA', 'CONSTRUCTOR', 'CONTAINS', 'CONTINUE',
    'CONVERT', 'CORR', 'CORRESPONDING', 'COUNT',
    'COVAR_POP', 'COVAR_SAMP', 'CREATE', 'CROSS',
    'CUBE', 'CUME_DIST', 'CURRENT', 'CURRENT_CATALOG',
    'CURRENT_DATE', 'CURRENT_DEFAULT_TRANSFORM_GROUP', 'CURRENT_PATH', 'CURRENT_ROLE',
    'CURRENT_ROW', 'CURRENT_SCHEMA', 'CURRENT_TIME', 'CURRENT_TIMESTAMP',
    'CURRENT_TRANSFORM_GROUP_FOR_TYPE', 'CURRENT_USER', 'CURSOR', 'CURSOR_NAME',
    'CYCLE', 'DATA', 'DATABASE', 'DATE',
    'DATETIME_INTERVAL_CODE', 'DATETIME_INTERVAL_PRECISION', 'DAY', 'DEALLOCATE',
    'DEC', 'DECADE', 'DECIMAL', 'DECLARE',
    'DEFAULT', 'DEFAULTS', 'DEFERRABLE', 'DEFERRED',
    'DEFINE', 'DEFINED', 'DEFINER', 'DEGREE',
    'DELETE', 'DENSE_RANK', 'DEPTH', 'DEREF',
    'DERIVED', 'DESC', 'DESCRIBE', 'DESCRIPTION',
    'DESCRIPTOR', 'DETERMINISTIC', 'DIAGNOSTICS', 'DISALLOW',
    'DISCONNECT', 'DISPATCH', 'DISTINCT', 'DOMAIN',
    'DOUBLE', 'DOW', 'DOY', 'DROP',
    'DYNAMIC', 'DYNAMIC_FUNCTION', 'DYNAMIC_FUNCTION_CODE', 'EACH',
    'ELEMENT', 'ELSE', 'EMPTY', 'END',
    'END-EXEC', 'END_FRAME', 'END_PARTITION', 'EPOCH',
    'EQUALS', 'ESCAPE', 'EVERY', 'EXCEPT',
    'EXCEPTION', 'EXCLUDE', 'EXCLUDING', 'EXEC',
    'EXECUTE', 'EXISTS', 'EXP', 'EXPLAIN',
    'EXTEND', 'EXTERNAL', 'EXTRACT', 'FALSE',
    'FETCH', 'FILTER', 'FINAL', 'FIRST',
    'FIRST_VALUE', 'FLOAT', 'FLOOR', 'FOLLOWING',
    'FOR', 'FOREIGN', 'FORTRAN', 'FOUND',
    'FRAC_SECOND', 'FRAME_ROW', 'FREE', 'FROM',
    'FULL', 'FUNCTION', 'FUSION', 'G',
    'GENERAL', 'GENERATED', 'GEOMETRY', 'GET',
    'GLOBAL', 'GO', 'GOTO', 'GRANT',
    'GRANTED', 'GROUP', 'GROUPING', 'GROUPS',
    'HAVING', 'HIERARCHY', 'HOLD', 'HOUR',
    'IDENTITY', 'IMMEDIATE', 'IMMEDIATELY', 'IMPLEMENTATION',
    'IMPORT', 'IN', 'INCLUDING', 'INCREMENT',
    'INDICATOR', 'INITIAL', 'INITIALLY', 'INNER',
    'INOUT', 'INPUT', 'INSENSITIVE', 'INSERT',
    'INSTANCE', 'INSTANTIABLE', 'INT', 'INTEGER',
    'INTERSECT', 'INTERSECTION', 'INTERVAL', 'INTO',
    'INVOKER', 'IS', 'ISOLATION', 'JAVA',
    'JOIN', 'JSON', 'K', 'KEY',
    'KEY_MEMBER', 'KEY_TYPE', 'LABEL', 'LAG',
    'LANGUAGE', 'LARGE', 'LAST', 'LAST_VALUE',
    'LATERAL', 'LEAD', 'LEADING', 'LEFT',
    'LENGTH', 'LEVEL', 'LIBRARY', 'LIKE',
    'LIKE_REGEX', 'LIMIT', 'LN', 'LOCAL',
    'LOCALTIME', 'LOCALTIMESTAMP', 'LOCATOR', 'LOWER',
    'M', 'MAP', 'MATCH', 'MATCHED',
    'MATCHES', 'MATCH_NUMBER', 'MATCH_RECOGNIZE', 'MAX',
    'MAXVALUE', 'MEASURES', 'MEMBER', 'MERGE',
    'MESSAGE_LENGTH', 'MESSAGE_OCTET_LENGTH', 'MESSAGE_TEXT', 'METHOD',
    'MICROSECOND', 'MILLENNIUM', 'MIN', 'MINUS',
    'MINUTE', 'MINVALUE', 'MOD', 'MODIFIES',
    'MODULE', 'MONTH', 'MORE', 'MULTISET',
    'MUMPS', 'NAME', 'NAMES', 'NATIONAL',
    'NATURAL', 'NCHAR', 'NCLOB', 'NESTING',
    'NEW', 'NEXT', 'NO', 'NONE',
    'NORMALIZE', 'NORMALIZED', 'NOT', 'NTH_VALUE',
    'NTILE', 'NULL', 'NULLABLE', 'NULLIF',
    'NULLS', 'NUMBER', 'NUMERIC', 'OBJECT',
    'OCCURRENCES_REGEX', 'OCTETS', 'OCTET_LENGTH', 'OF',
    'OFFSET', 'OLD', 'OMIT', 'ON',
    'ONE', 'ONLY', 'OPEN', 'OPTION',
    'OPTIONS', 'OR', 'ORDER', 'ORDERING',
    'ORDINALITY', 'OTHERS', 'OUT', 'OUTER',
    'OUTPUT', 'OVER', 'OVERLAPS', 'OVERLAY',
    'OVERRIDING', 'PAD', 'PARAMETER', 'PARAMETER_MODE',
    'PARAMETER_NAME', 'PARAMETER_ORDINAL_POSITION', 'PARAMETER_SPECIFIC_CATALOG',
    'PARAMETER_SPECIFIC_NAME', 'PARAMETER_SPECIFIC_SCHEMA', 'PARTIAL', 'PARTITION',
    'PASCAL', 'PASSTHROUGH', 'PAST', 'PATH', 'PATTERN',
    'PER', 'PERCENT', 'PERCENTILE_CONT', 'PERCENTILE_DISC',
    'PERCENT_RANK', 'PERIOD', 'PERMUTE', 'PLACING',
    'PLAN', 'PLI', 'PORTION', 'POSITION',
    'POSITION_REGEX', 'POWER', 'PRECEDES', 'PRECEDING',
    'PRECISION', 'PREPARE', 'PRESERVE', 'PREV',
    'PRIMARY', 'PRIOR', 'PRIVILEGES', 'PROCEDURE',
    'PUBLIC', 'QUARTER', 'RANGE', 'RANK',
    'READ', 'READS', 'REAL', 'RECURSIVE',
    'REF', 'REFERENCES', 'REFERENCING', 'REGR_AVGX',
    'REGR_AVGY', 'REGR_COUNT', 'REGR_INTERCEPT', 'REGR_R2',
    'REGR_SLOPE', 'REGR_SXX', 'REGR_SXY', 'REGR_SYY',
    'RELATIVE', 'RELEASE', 'REPEATABLE', 'REPLACE',
    'RESET', 'RESTART', 'RESTRICT', 'RESULT',
    'RETURN', 'RETURNED_CARDINALITY', 'RETURNED_LENGTH', 'RETURNED_OCTET_LENGTH',
    'RETURNED_SQLSTATE', 'RETURNS', 'REVOKE', 'RIGHT',
    'ROLE', 'ROLLBACK', 'ROLLUP', 'ROUTINE',
    'ROUTINE_CATALOG', 'ROUTINE_NAME', 'ROUTINE_SCHEMA', 'ROW',
    'ROWS', 'ROW_COUNT', 'ROW_NUMBER', 'RUNNING',
    'SAVEPOINT', 'SCALE', 'SCHEMA', 'SCHEMA_NAME',
    'SCOPE', 'SCOPE_CATALOGS', 'SCOPE_NAME', 'SCOPE_SCHEMA',
    'SCROLL', 'SEARCH', 'SECOND', 'SECTION',
    'SECURITY', 'SEEK', 'SELECT', 'SELF',
    'SENSITIVE', 'SEQUENCE', 'SERIALIZABLE', 'SERVER',
    'SERVER_NAME', 'SESSION', 'SESSION_USER', 'SET',
    'SETS', 'SHOW', 'SIMILAR', 'SIMPLE',
    'SIZE', 'SKIP', 'SMALLINT', 'SOME',
    'SOURCE', 'SPACE', 'SPECIFIC', 'SPECIFICTYPE',
    'SPECIFIC_NAME', 'SQL', 'SQLEXCEPTION', 'SQLSTATE',
    'SQLWARNING', 'SQL_BIGINT', 'SQL_BINARY', 'SQL_BIT',
    'SQL_BLOB', 'SQL_BOOLEAN', 'SQL_CHAR', 'SQL_CLOB',
    'SQL_DATE', 'SQL_DECIMAL', 'SQL_DOUBLE', 'SQL_FLOAT',
    'SQL_INTEGER', 'SQL_INTERVAL_DAY', 'SQL_INTERVAL_DAY_TO_HOUR',
    'SQL_INTERVAL_DAY_TO_MINUTE', 'SQL_INTERVAL_DAY_TO_SECOND', 'SQL_INTERVAL_HOUR',
    'SQL_INTERVAL_HOUR_TO_MINUTE', 'SQL_INTERVAL_HOUR_TO_SECOND', 'SQL_INTERVAL_MINUTE',
    'SQL_INTERVAL_MINUTE_TO_SECOND', 'SQL_INTERVAL_MONTH', 'SQL_INTERVAL_SECOND',
    'SQL_INTERVAL_YEAR', 'SQL_INTERVAL_YEAR_TO_MONTH', 'SQL_LONGVARBINARY',
    'SQL_LONGVARCHAR', 'SQL_LONGVARNCHAR', 'SQL_NCHAR', 'SQL_NCLOB', 'SQL_NUMERIC',
    'SQL_NVARCHAR', 'SQL_REAL', 'SQL_SMALLINT', 'SQL_TIME',
    'SQL_TIMESTAMP', 'SQL_TINYINT', 'SQL_TSI_DAY', 'SQL_TSI_FRAC_SECOND',
    'SQL_TSI_HOUR', 'SQL_TSI_MICROSECOND', 'SQL_TSI_MINUTE', 'SQL_TSI_MONTH',
    'SQL_TSI_QUARTER', 'SQL_TSI_SECOND', 'SQL_TSI_WEEK', 'SQL_TSI_YEAR',
    'SQL_VARBINARY', 'SQL_VARCHAR', 'SQRT', 'START',
    'STATE', 'STATEMENT', 'STATIC', 'STDDEV_POP',
    'STDDEV_SAMP', 'STREAM', 'STRUCTURE', 'STYLE',
    'SUBCLASS_ORIGIN', 'SUBMULTISET', 'SUBSET', 'SUBSTITUTE',
    'SUBSTRING', 'SUBSTRING_REGEX', 'SUCCEEDS', 'SUM',
    'SYMMETRIC', 'SYSTEM', 'SYSTEM_TIME', 'SYSTEM_USER',
    'TABLE', 'TABLESAMPLE', 'TABLE_NAME', 'TEMPORARY',
    'THEN', 'TIES', 'TIME', 'TIMESTAMP',
    'TIMESTAMPADD', 'TIMESTAMPDIFF', 'TIMEZONE_HOUR', 'TIMEZONE_MINUTE',
    'TINYINT', 'TO', 'TOP_LEVEL_COUNT', 'TRAILING',
    'TRANSACTION', 'TRANSACTIONS_ACTIVE', 'TRANSACTIONS_COMMITTED',
    'TRANSACTIONS_ROLLED_BACK', 'TRANSFORM', 'TRANSFORMS', 'TRANSLATE', 'TRANSLATE_REGEX',
    'TRANSLATION', 'TREAT', 'TRIGGER', 'TRIGGER_CATALOG',
    'TRIGGER_NAME', 'TRIGGER_SCHEMA', 'TRIM', 'TRIM_ARRAY',
    'TRUE', 'TRUNCATE', 'TYPE', 'UESCAPE',
    'UNBOUNDED', 'UNCOMMITTED', 'UNDER', 'UNION',
    'UNIQUE', 'UNKNOWN', 'UNNAMED', 'UNNEST',
    'UPDATE', 'UPPER', 'UPSERT', 'USAGE',
    'USER', 'USER_DEFINED_TYPE_CATALOG', 'USER_DEFINED_TYPE_CODE',
    'USER_DEFINED_TYPE_NAME', 'USER_DEFINED_TYPE_SCHEMA', 'USING', 'VALUE', 'VALUES',
    'VALUE_OF', 'VARBINARY', 'VARCHAR', 'VARYING',
    'VAR_POP', 'VAR_SAMP', 'VERSION', 'VERSIONING',
    'VIEW', 'WEEK', 'WHEN', 'WHENEVER',
    'WHERE', 'WIDTH_BUCKET', 'WINDOW', 'WITH',
    'WITHIN', 'WITHOUT', 'WORK', 'WRAPPER',
    'WRITE', 'XML', 'YEAR', 'ZONE',
])

SUPERSET_KEYWORDS = set([
    '__timestamp',
])


class PinotCompiler(compiler.SQLCompiler):
    def visit_select(self, select, **kwargs):
        return super().visit_select(select, **kwargs)

    def visit_column(self, column, result_map=None, **kwargs):
        # Pinot does not support table aliases
        if column.table:
            column.table.named_with_column = False
        result_map = result_map or kwargs.pop("add_to_result_map", None)
        # This is a hack to modify the original column, but how do I clone it ?
        column.is_literal = True
        return super().visit_column(column, result_map, **kwargs)

    def escape_literal_column(self, text):
        # This is a hack to quote column names that conflict with reserved words since 'column.is_literal = True'
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
        if kw:
            render_label_as_label = kw.pop("render_label_as_label", None)
        render_label_as_label = None
        return super().visit_label(
            label,
            add_to_result_map,
            within_label_clause,
            within_columns_clause,
            render_label_as_label,
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

    def visit_DATETIME(self, type_, **kwargs):
        raise exceptions.NotSupportedError("Type DATETIME is not supported")

    def visit_TIME(self, type_, **kwargs):
        raise exceptions.NotSupportedError("Type TIME is not supported")

    def visit_BINARY(self, type_, **kwargs):
        raise exceptions.NotSupportedError("Type BINARY is not supported")

    def visit_VARBINARY(self, type_, **kwargs):
        raise exceptions.NotSupportedError("Type VARBINARY is not supported")

    def visit_BLOB(self, type_, **kwargs):
        raise exceptions.NotSupportedError("Type BLOB is not supported")

    def visit_CLOB(self, type_, **kwargs):
        raise exceptions.NotSupportedError("Type CBLOB is not supported")

    def visit_NCLOB(self, type_, **kwargs):
        raise exceptions.NotSupportedError("Type NCBLOB is not supported")


class PinotIdentifierPareparer(compiler.IdentifierPreparer):
    reserved_words = set([e.lower() for e in (CALCITE_KEYWORDS ^ SUPERSET_KEYWORDS)])

    def __init__(self, dialect, initial_quote='"',
                 final_quote=None, escape_quote='"', omit_schema=True):
        super(PinotIdentifierPareparer, self).__init__(
            dialect, initial_quote, final_quote, escape_quote, omit_schema,
        )


class PinotDialect(default.DefaultDialect):

    name = "pinot"
    scheme = "http"
    driver = "rest"
    preparer = PinotIdentifierPareparer
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

        self._controller = None
        self._debug = False
        self.update_from_kwargs(kwargs)

    def update_from_kwargs(self, givenkw):
        kwargs = givenkw.copy() if givenkw else {}
        ## For backward compatible
        if "server" in kwargs:
            self._controller = kwargs.pop("server")
        if "controller" in kwargs:
            self._controller = kwargs.pop("controller")
        kwargs["debug"] = self._debug = bool(kwargs.get("debug", False))
        logger.info(
            f"Updated pinot dialect args from {kwargs}: {self._controller} and {self._debug}"
        )
        return kwargs

    @classmethod
    def dbapi(cls):
        return pinotdb

    def create_connect_args(self, url):
        kwargs = {
            "host": url.host,
            "port": url.port or 9000,
            "path": url.database,
            "scheme": self.scheme,
            "username": url.username,
            "password": url.password,
        }
        if url.query:
            kwargs.update(url.query)

        kwargs = self.update_from_kwargs(kwargs)
        return ([], kwargs)

    def get_metadata_from_controller(self, path):
        url = parse.urljoin(self._controller, path)
        r = requests.get(url, headers={"Accept": "application/json"})
        try:
            result = r.json()
        except ValueError as e:
            raise exceptions.DatabaseError(
                f"Got invalid json response from {self._controller}:{path}: {r.text}"
            ) from e
        if self._debug:
            logger.info(f"metadata get on {self._controller}:{path} returned {result}")
        return result

    def get_schema_names(self, connection, **kwargs):
        return self.get_metadata_from_controller("/schemas")

    def has_table(self, connection, table_name, schema=None):
        return table_name in self.get_table_names(connection, schema)

    def get_table_names(self, connection, schema=None, **kwargs):
        return self.get_metadata_from_controller("/tables")["tables"]

    def get_view_names(self, connection, schema=None, **kwargs):
        return []

    def get_table_options(self, connection, table_name, schema=None, **kwargs):
        return {}

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        payload = self.get_metadata_from_controller(f"/tables/{table_name}/schema")

        logger.info(
            f"Getting columns for {table_name} from {self._controller}: {payload}"
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


PinotHTTPDialect = PinotDialect


class PinotHTTPSDialect(PinotDialect):

    scheme = "https"


def get_default(pinot_column_default):
    if pinot_column_default == "null":
        return None
    else:
        return str(pinot_column_default)


def get_type(data_type, field_size):
    type_map = {
        "string": types.String,
        "int": types.BigInteger,
        "long": types.BigInteger,
        "float": types.Float,
        "double": types.Numeric,
        "bytes": types.LargeBinary,
        "boolean": types.Boolean,
    }
    return type_map[data_type.lower()]
