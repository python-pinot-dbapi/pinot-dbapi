# -*- coding: future_fstrings -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from collections import namedtuple
from enum import Enum
from six import string_types
from six.moves.urllib import parse

import requests

from pinotdb import exceptions


class Type(Enum):
    STRING = 1
    NUMBER = 2
    BOOLEAN = 3


def connect(host='localhost', port=8099, path='/query', scheme='http'):
    """
    Constructor for creating a connection to the database.

        >>> conn = connect('localhost', 8099)
        >>> curs = conn.cursor()

    """
    return Connection(host, port, path, scheme)


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


def get_description_from_row(row):
    """
    Return description from a single row.

    We only return the nameand  type (inferred from the data).
    """
    return [
        (
            name,               # name
            get_type(value),    # type_code
            None,               # [display_size]
            None,               # [internal_size]
            None,               # [precision]
            None,               # [scale]
            None,               # [null_ok]
        )
        for name, value in row.items()
    ]


def get_type(value):
    """Infer type from value."""
    if isinstance(value, string_types):
        return Type.STRING
    elif isinstance(value, (int, float)):
        return Type.NUMBER
    elif isinstance(value, bool):
        return Type.BOOLEAN

    raise exceptions.Error(f'Value of unknown type: {value}')


class Connection(object):

    """Connection to a Pinot database."""

    def __init__(
        self,
        host='localhost',
        port=8099,
        path='/query',
        scheme='http',
    ):
        netloc = f'{host}:{port}'
        self.url = parse.urlunparse(
            (scheme, netloc, path, None, None, None))
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
        cursor = Cursor(self.url)
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


class Cursor(object):

    """Connection cursor."""

    def __init__(self, url):
        self.url = url

        # This read/write attribute specifies the number of rows to fetch at a
        # time with .fetchmany(). It defaults to 1 meaning to fetch a single
        # row at a time.
        self.arraysize = 1

        self.closed = False

        # these are updated only after a query
        self.description = None
        self.rowcount = -1
        self._results = None

    @check_closed
    def close(self):
        """Close the cursor."""
        self.closed = True

    @check_closed
    def execute(self, operation, parameters=None):
        query = apply_parameters(operation, parameters or {})
        print(query)

        headers = {'Content-Type': 'application/json'}
        payload = {'pql': query}
        r = requests.post(self.url, headers=headers, json=payload)
        if r.encoding is None:
            r.encoding = 'utf-8'

        try:
            payload = r.json()
        except Exception:
            payload = r.text

        # raise any error messages
        if r.status_code != 200:
            msg = f"Query\n\n{query}\n\nreturned an error"
            raise exceptions.ProgrammingError(msg)

        if 'aggregationResults' in payload:
            results = payload['aggregationResults'][0]
            if 'groupByResult' in results:
                rows = []
                for result in results['groupByResult']:
                    row = {
                        k: v for k, v in
                        zip(results['groupByColumns'], result['group'])
                    }
                    row[results['function']] = result['value']
                    rows.append(row)
            else:
                rows = [{results['function']: results['value']}]
        elif 'selectionResults' in payload:
            results = payload['selectionResults']
            rows = [
                {k: v for k, v in zip(results['columns'], result)}
                for result in results['results']
            ]
        else:
            msg = '\n'.join(exception['message']
                            for exception in payload['exceptions'])
            raise exceptions.ProgrammingError(msg)

        self.description = None
        self._results = []
        if rows:
            self.description = get_description_from_row(rows[0])
            Row = namedtuple('Row', rows[0].keys(), rename=True)
            self._results = [Row(*row.values()) for row in rows]

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
