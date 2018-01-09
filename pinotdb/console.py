"""Pinot CLI.

Usage:
  pinotdb [--broker=<broker>] [--server=<server>]
  pinotdb (-h | --help)
  pinotdb --version

Options:
  -h --help                         Show this screen.
  -v --version                      Show version.
  -b <broker> --broker=<broker>     Broker URL [default: http://localhost:8099/query]
  -s <server> --server=<server>	    API server URL [default: http://localhost:9000/]

"""

from __future__ import unicode_literals

import os

from docopt import docopt
from prompt_toolkit import prompt, AbortAction
from prompt_toolkit.history import FileHistory
from prompt_toolkit.contrib.completers import WordCompleter
from pygments.lexers import SqlLexer
from pygments.style import Style
from pygments.token import Token
from pygments.styles.default import DefaultStyle
import requests
from six.moves.urllib import parse
from tabulate import tabulate

from pinotdb import connect
from pinotdb.__version__ import __version__


keywords = [
    'SELECT',
    'FROM',
    'WHERE',
    'GROUP BY',
    'ORDER BY',
    'TOP',
    'LIMIT',
    'BETWEEN',
    'IN',
    'NOT',
    'ASC',
    'DESC',
]

aggregate_functions = [
    'COUNT',
    'MIN',
    'MAX',
    'SUM',
    'AVG',
    'MINMAXRANGE',
    'DISTINCTCOUNT',
    'DISTINCTCOUNTHLL',
    'FASTHLL',
    'PERCENTILE50',
    'PERCENTILE90',
    'PERCENTILE95',
    'PERCENTILE99',
    'PERCENTILEEST50',
    'PERCENTILEEST90',
    'PERCENTILEEST95',
    'PERCENTILEEST99',
    'COUNTMV',
    'MINMV',
    'MAXMV',
    'SUMMV',
    'AVGMV',
    'MINMAXRANGEMV',
    'DISTINCTCOUNTMV',
    'DISTINCTCOUNTHLLMV',
    'FASTHLLMV',
    'PERCENTILE50MV',
    'PERCENTILE90MV',
    'PERCENTILE95MV',
    'PERCENTILE99MV',
    'PERCENTILEEST50MV',
    'PERCENTILEEST90MV',
    'PERCENTILEEST95MV',
    'PERCENTILEEST99MV',
]


class DocumentStyle(Style):
    styles = {
        Token.Menu.Completions.Completion.Current: 'bg:#00aaaa #000000',
        Token.Menu.Completions.Completion: 'bg:#008888 #ffffff',
        Token.Menu.Completions.ProgressButton: 'bg:#003333',
        Token.Menu.Completions.ProgressBar: 'bg:#00aaaa',
    }
    styles.update(DefaultStyle.styles)


def get_connection_kwargs(url):
    parts = parse.urlparse(url)
    if ':' in parts.netloc:
        host, port = parts.netloc.split(':', 1)
        port = int(port)
    else:
        host = parts.netloc
        port = 8099

    return {
        'host': host,
        'port': port,
        'path': parts.path,
        'scheme': parts.scheme,
    }


def get_tables(server):
    parts = parse.urlparse(server)
    if ':' in parts.netloc:
        host, port = parts.netloc.split(':', 1)
        port = int(port)
    else:
        host = parts.netloc
        port = 9000

    netloc = f'{host}:{port}'
    url = parse.urlunparse(
        (parts.scheme, netloc, '/tables', None, None, None))

    r = requests.get(url)
    payload = r.json()
    return payload['tables']


def get_autocomplete(server):
    return keywords + aggregate_functions + get_tables(server)


def main():
    arguments = docopt(__doc__, version=__version__)
    history = FileHistory(os.path.expanduser('~/.pinotdb_history'))

    kwargs = get_connection_kwargs(arguments['--broker'])
    connection = connect(**kwargs)
    cursor = connection.cursor()

    words = get_autocomplete(arguments['--server'])
    sql_completer = WordCompleter(words, ignore_case=True)

    while True:
        try:
            query = prompt(
                '> ', lexer=SqlLexer, completer=sql_completer,
                style=DocumentStyle, history=history,
                on_abort=AbortAction.RETRY)
        except EOFError:
            break  # Control-D pressed.

        # run query
        if query.strip():
            try:
                result = cursor.execute(query.rstrip(';'))
            except Exception as e:
                print(e)
                continue

            headers = [t[0] for t in cursor.description]
            print(tabulate(result, headers=headers))

    print('GoodBye!')


if __name__ == '__main__':
    main()
