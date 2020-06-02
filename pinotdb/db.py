from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from collections import namedtuple, OrderedDict
from enum import Enum
from six import string_types
from six.moves.urllib import parse
from pprint import pformat

import json
import requests

from pinotdb import exceptions
import logging
logger = logging.getLogger(__name__)


class Type(Enum):
    STRING = 1
    NUMBER = 2
    BOOLEAN = 3


def connect(*args, **kwargs):
    """
    Constructor for creating a connection to the database.

        >>> conn = connect('localhost', 8099)
        >>> curs = conn.cursor()

    """
    return Connection(*args, **kwargs)


def check_closed(f):
    """Decorator that checks if connection/cursor is closed."""

    def g(self, *args, **kwargs):
        if self.closed:
            raise exceptions.Error(f'{self.__class__.__name__} already closed')
        return f(self, *args, **kwargs)
    return g


def check_result(f):
    """Decorator that checks if the cursor has results from `execute`."""

    def g(self, *args, **kwargs):
        if self._results is None:
            raise exceptions.Error('Called before `execute`')
        return f(self, *args, **kwargs)
    return g



def get_description_from_types(column_names, types):
    return [
        (
            name,               # name
            tc.code,            # type_code
            None,               # [display_size]
            None,               # [internal_size]
            None,               # [precision]
            None,               # [scale]
            None,               # [null_ok]
        )
        for name, tc in zip(column_names, types)
    ]


TypeCodeAndValue = namedtuple('TypeCodeAndValue', ['code', 'value', 'coerce_to_string'])

def get_types_from_column_data_types(column_data_types):
    types = [None] * len(column_data_types)
    for column_index, column_data_type in enumerate(column_data_types):
        if column_data_type == "INT" or column_data_type == "LONG" or column_data_type == "FLOAT" or column_data_type == "DOUBLE":
            types[column_index] = TypeCodeAndValue(Type.NUMBER, None, False)
        elif column_data_type == "STRING" :
            types[column_index] = TypeCodeAndValue(Type.STRING, None, False)
        else:
            types[column_index] = TypeCodeAndValue(Type.STRING, None, True)
    return types

def get_group_by_column_names(aggregation_results):
    group_by_cols = []
    for metric in aggregation_results:
        metric_name = metric.get('function', 'noname')
        gby_cols_for_metric = metric.get('groupByColumns', []) 
        if group_by_cols and group_by_cols != gby_cols_for_metric:
            raise exceptions.DatabaseError(f"Cols for metric {metric_name}: {gby_cols_for_metric} differ from other columns {group_by_cols}")
        elif not group_by_cols:
            group_by_cols = gby_cols_for_metric[:]
    return group_by_cols


def is_iterable(value):
    try:
        _ = iter(value)
        return True
    except TypeError:
        return False

class Connection(object):

    """Connection to a Pinot database."""

    def __init__(self, *args, **kwargs):
        self._debug = kwargs.get('debug', False)
        self._args = args
        self._kwargs = kwargs
        self.closed = False
        self.cursors = []

    @check_closed
    def close(self):
        """Close the connection now."""
        self.closed = True
        for cursor in self.cursors:
            try:
                cursor.close()
            except exceptions.Error:
                pass  # already closed

    @check_closed
    def commit(self):
        """
        Commit any pending transaction to the database.

        Not supported.
        """
        pass

    @check_closed
    def cursor(self):
        """Return a new Cursor Object using the connection."""
        cursor = Cursor(*self._args, **self._kwargs)
        self.cursors.append(cursor)

        return cursor

    @check_closed
    def execute(self, operation, parameters=None):
        cursor = self.cursor()
        return cursor.execute(operation, parameters)

    def __enter__(self):
        return self.cursor()

    def __exit__(self, *exc):
        self.close()


def convert_result_if_required(types, rows):
    coercion_needed = any(t.coerce_to_string for t in types)
    if not coercion_needed:
        return rows
    for i, t in enumerate(types):
        if t.coerce_to_string:
            for row in rows:
                row[i] = json.dumps(row[i])
    return rows


class Cursor(object):
    """Connection cursor."""

    def __init__(self, host, port=8099, scheme='http', path='/query/sql', extra_request_headers='', debug=False, preserve_types=False, ignore_exception_error_codes='', acceptable_respond_fraction=-1):
        if path == "query":
            path = "query/sql"
        self.url = parse.urlunparse(
            (scheme, f'{host}:{port}', path, None, None, None))

        # This read/write attribute specifies the number of rows to fetch at a
        # time with .fetchmany(). It defaults to 1 meaning to fetch a single
        # row at a time.
        self.arraysize = 1

        self.closed = False

        # these are updated only after a query
        self.description = None
        self.rowcount = -1
        self._results = None
        self._debug = debug
        self._preserve_types = preserve_types
        self.acceptable_respond_fraction = acceptable_respond_fraction
        if ignore_exception_error_codes:
            self._ignore_exception_error_codes = set([int(x) for x in ignore_exception_error_codes.split(',')])
        else:
            self._ignore_exception_error_codes = []
        extra_headers = {}
        if extra_request_headers:
            for header in extra_request_headers.split(','):
                k, v = header.split('=')
                extra_headers[k] = v
        self._extra_request_headers = extra_headers

    @check_closed
    def close(self):
        """Close the cursor."""
        self.closed = True

    def is_valid_exception(self, e):
        if 'errorCode' not in e:
            return True
        else:
            return e['errorCode'] not in self._ignore_exception_error_codes

    def check_sufficient_responded(self, query, queried, responded):
        fraction = self.acceptable_respond_fraction
        if fraction == 0:
            return
        if queried < 0 or responded < 0:
            responded = -1
            needed = -1
        elif fraction <= -1:
            needed = queried
        elif fraction > 0 and fraction < 1:
            needed = int(fraction * queried)
        else:
            needed = fraction
        if responded < 0 or responded < needed:
            raise exceptions.DatabaseError(f"Query\n\n{query} timed out: Out of {queried}, only"
                    f" {responded} responded, while needed was {needed}")

    @check_closed
    def execute(self, operation, parameters=None):
        query = apply_parameters(operation, parameters or {})
        headers = {'Content-Type': 'application/json'}
        headers.update(self._extra_request_headers)
        if self._preserve_types:
            query += " OPTION(preserveType='true')"
        payload = {'sql': query}
        if self._debug:
            logger.info(f'Submitting the pinot query to {self.url}:\n{query}\n{pformat(payload)}, with {headers}')
        r = requests.post(self.url, headers=headers, json=payload)
        if r.encoding is None:
            r.encoding = 'utf-8'

        try:
            payload = r.json()
        except Exception as e:
            raise exceptions.DatabaseError(f"Error when querying {query} from {self.url}, raw response is:\n{r.text}") from e

        if self._debug:
            logger.info(f'Got the payload of type {type(payload)} with the status code {0 if not r else r.status_code}:\n{payload}')

        num_servers_responded = payload.get('numServersResponded', -1)
        num_servers_queried = payload.get('numServersQueried', -1)

        self.check_sufficient_responded(query, num_servers_queried, num_servers_responded)

        # raise any error messages
        if r.status_code != 200:
            msg = f"Query\n\n{query}\n\nreturned an error: {r.status_code}\nFull response is {pformat(payload)}"
            raise exceptions.ProgrammingError(msg)

        query_exceptions = [e for e in payload.get('exceptions', []) if self.is_valid_exception(e)]
        if query_exceptions:
            msg = '\n'.join(pformat(exception) for exception in query_exceptions)
            raise exceptions.DatabaseError(msg)

        rows = []  # array of array, where inner array is array of column values
        column_names = [] # column names, such that len(column_names) == len(rows[0])
        column_data_types = [] # column data types 1:1 mapping to column_names
        if 'resultTable' in payload:
            results = payload['resultTable']
            column_names = results.get('dataSchema').get('columnNames')
            column_data_types = results.get('dataSchema').get('columnDataTypes')
            values = results.get('rows')
            if column_names and values:
                rows = values
            else:
                raise exceptions.DatabaseError(f'Expected columns and results in resultTable, but got {pformat(results)} instead')

        logger.debug(f'Got the rows as a type {type(rows)} of size {len(rows)}')
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(pformat(rows))
        self.description = None
        self._results = []
        if column_data_types:
            types = get_types_from_column_data_types(column_data_types)
            if self._debug:
                logger.info(f'Column_names are {pformat(column_names)}, Column_data_types are {pformat(column_data_types)}, Types are {pformat(types)}')
            self._results = convert_result_if_required(types, rows)
            self.description = get_description_from_types(column_names, types)
        return self

    @check_closed
    def executemany(self, operation, seq_of_parameters=None):
        raise exceptions.NotSupportedError(
            '`executemany` is not supported, use `execute` instead')

    @check_result
    @check_closed
    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single sequence,
        or `None` when no more data is available.
        """
        try:
            return self._results.pop(0)
        except IndexError:
            return None

    @check_result
    @check_closed
    def fetchmany(self, size=None):
        """
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.
        """
        size = size or self.arraysize
        output, self._results = self._results[:size], self._results[size:]
        return output

    @check_result
    @check_closed
    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.
        """
        return list(self)

    @check_closed
    def setinputsizes(self, sizes):
        # not supported
        pass

    @check_closed
    def setoutputsizes(self, sizes):
        # not supported
        pass

    @check_closed
    def __iter__(self):
        return self

    @check_closed
    def __next__(self):
        output = self.fetchone()
        if output is None:
            raise StopIteration

        return output

    next = __next__


def apply_parameters(operation, parameters):
    escaped_parameters = {
        key: escape(value) for key, value in parameters.items()
    }
    return operation % escaped_parameters


def escape(value):
    if value == '*':
        return value
    elif isinstance(value, string_types):
        return "'{}'".format(value.replace("'", "''"))
    elif isinstance(value, (int, float)):
        return value
    elif isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    elif isinstance(value, (list, tuple)):
        return ', '.join(escape(element) for element in value)
