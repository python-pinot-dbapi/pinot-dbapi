from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from collections import namedtuple, OrderedDict
from enum import Enum
from six import string_types
from six.moves.urllib import parse
from pprint import pprint, pformat

import requests

from pinotdb import exceptions
import logging
logger = logging.getLogger(__name__)


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


        headers = {'Content-Type': 'application/json'}
        payload = {'pql': query}
        logger.debug(f'Submitting the pinot query to {self.url}:\n{query}\n{pformat(payload)}')
        r = requests.post(self.url, headers=headers, json=payload)
        if r.encoding is None:
            r.encoding = 'utf-8'

        try:
            payload = r.json()
        except Exception as e:
            raise exceptions.DatabaseError(f"Error when querying {query} from {self.url}, raw response is:\n{r.text}") from e

        logger.debug(f'Got the payload of type {type(payload)} with the status code {0 if not r else r.status_code}:\n{payload}')

        num_servers_responded = payload.get('numServersResponded', -1)
        num_servers_queried = payload.get('numServersQueried', -1)

        if num_servers_queried > num_servers_responded or num_servers_responded == -1 or num_servers_queried == -1:
            raise exceptions.DatabaseError(f"Query\n\n{query} timed out: Out of {num_servers_queried}, only"
                                           f" {num_servers_responded} responded")

        # raise any error messages
        if r.status_code != 200:
            msg = f"Query\n\n{query}\n\nreturned an error: {r.status_code}\nFull response is {pformat(payload)}"
            raise exceptions.ProgrammingError(msg)

        if payload.get('exceptions', []):
            msg = '\n'.join(pformat(exception)
                            for exception in payload['exceptions'])
            raise exceptions.DatabaseError(msg)

        if 'aggregationResults' in payload:
            aggregation_results = payload['aggregationResults']
            gby_cols = get_group_by_column_names(aggregation_results)
            metric_names = [agg_result['function'] for agg_result in aggregation_results]
            gby_rows = OrderedDict() # Dict of group-by-vals to array of metrics  
            total_group_vals_key = ()
            num_metrics = len(metric_names)
            for i, agg_result in enumerate(aggregation_results):
                if 'groupByResult' in agg_result:
                    if total_group_vals_key in gby_rows:
                        raise exceptions.DatabaseError(f"Invalid response {pformat(aggregation_results)} since we have both total and group by results")
                    for gb_result in agg_result['groupByResult']:
                        group_values = gb_result['group']
                        if len(group_values) < len(gby_cols):
                            raise exceptions.DatabaseError(f"Expected {pformat(agg_result)} to contain {len(gby_cols)}, but got {len(group_values)}")
                        elif len(group_values) > len(gby_cols):
                            # This can happen because of poor escaping in the results
                            extra = len(group_values) - len(gby_cols)
                            new_group_values = group_values[extra:]
                            new_group_values[0] = ''.join(group_values[0:extra]) + new_group_values[0]
                            group_values = new_group_values

                        group_values_key = tuple(group_values)
                        if group_values_key not in gby_rows:
                            gby_rows[group_values_key] = [None] * num_metrics
                        gby_rows[group_values_key][i] = gb_result['value']
                else: # Global aggregation result
                    if total_group_vals_key not in gby_rows:
                        gby_rows[total_group_vals_key] = [None] * num_metrics
                    if len(gby_rows) != 1:
                        raise exceptions.DatabaseError(f"Invalid response {pformat(aggregation_results)} since we have both total and group by results")
                    if len(gby_cols) > 0:
                        raise exceptions.DatabaseError(f"Invalid response since total aggregation results are present even when non zero gby_cols:{gby_cols}, {pformat(aggregation_results)}")
                    gby_rows[total_group_vals_key][i] = agg_result['value']

            rows = []
            num_groups_and_metrics = len(gby_cols) + len(metric_names)
            for group_vals, metric_vals in gby_rows.items():
                if len(group_vals) != len(gby_cols):
                    raise exceptions.DatabaseError(f"Expected {len(gby_cols)} but got {len(group_vals)} for a row")
                if len(metric_vals) != len(metric_names):
                    raise exceptions.DatabaseError(f"Expected {len(metric_names)} but got {len(metric_vals)} for a row")
                row = dict(zip(gby_cols, group_vals))
                row.update(zip(metric_names, metric_vals))
                if len(row) != num_groups_and_metrics:
                    raise exceptions.DatabaseError(f"Expected {num_groups_and_metrics} columns in the row but due to name collisions got less got only {len(row)}, full {pformat(row)}")
                rows.append(row)
        elif 'selectionResults' in payload:
            results = payload['selectionResults']
            rows = [
                {k: v for k, v in zip(results['columns'], result)}
                for result in results['results']
            ]
        else:
            # empty result, no error
            rows = []

        logger.debug(f'Got the rows as a type {type(rows)} of size {len(rows)}')
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(pformat(rows))
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
