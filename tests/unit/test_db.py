import datetime
from typing import Any, Dict, Optional
from unittest import TestCase
from unittest.mock import MagicMock, patch

import httpx

try:
    from unittest import IsolatedAsyncioTestCase
except ImportError:
    from mock.backports import IsolatedAsyncioTestCase

try:
    from unittest.mock import AsyncMock
except ImportError:
    from mock import AsyncMock

from pinotdb import db, exceptions


class ConnectionTest(TestCase):
    def test_starts_without_session_by_default(self):
        connection = db.Connection()

        self.assertIsNone(connection.session)
        self.assertFalse(connection.is_session_external)

    def test_verifies_httpx_session_upon_initializing_if_provided(self):
        client = httpx.Client()

        connection = db.Connection(session=client)

        self.assertIs(connection.session, client)
        self.assertTrue(connection.is_session_external)

    def test_verifies_httpx_session(self):
        client = httpx.Client()
        connection = db.Connection()
        connection.session = client

        connection.verify_session()
        # All good, no errors.

    def test_fails_to_verify_session_if_unexpected_type(self):
        connection = db.Connection()
        connection.session = object()

        with self.assertRaises(AssertionError):
            connection.verify_session()

    def test_bypasses_verification_if_no_session_initialized(self):
        connection = db.Connection()

        connection.verify_session()

    def test_gets_cursor_from_connection(self):
        connection = db.Connection(host='localhost')

        cursor = connection.cursor()

        self.assertIsInstance(cursor, db.Cursor)

    def test_gets_cursor_from_connection_with_explicit_session(self):
        connection = db.Connection(
            host='localhost', session=MagicMock(spec=httpx.Client))
        connection.session.is_closed = False

        cursor = connection.cursor()

        self.assertIsInstance(cursor, db.Cursor)

    def test_renews_session_if_closed_when_getting_cursor(self):
        connection = db.Connection(host='localhost')
        connection.cursor()
        session1 = connection.session

        session1.close()
        connection.cursor()
        session2 = connection.session

        self.assertIsNot(session1, session2)

    def test_starts_not_closed(self):
        connection = db.Connection(
            host='localhost', session=MagicMock(spec=httpx.Client))
        cursor = connection.cursor()

        self.assertFalse(connection.closed)
        self.assertFalse(cursor.closed)

    def test_closes_connection(self):
        connection = db.Connection(
            host='localhost', session=MagicMock(spec=httpx.Client))
        cursor = connection.cursor()

        connection.close()

        self.assertTrue(connection.closed)
        self.assertTrue(cursor.closed)

    def test_closes_connection_even_if_cursor_already_closed(self):
        connection = db.Connection(
            host='localhost', session=MagicMock(spec=httpx.Client))
        cursor = connection.cursor()
        cursor.close()

        connection.close()

        self.assertTrue(connection.closed)
        self.assertTrue(cursor.closed)

    def test_closes_underlying_session_as_well(self):
        connection = db.Connection(
            host='localhost', session=MagicMock(spec=httpx.Client))
        # Just to simulate an implicitly created session.
        connection.is_session_external = False

        connection.close()

        self.assertTrue(connection.session.close.called)

    def test_cant_close_connection_twice(self):
        connection = db.Connection(
            host='localhost', session=MagicMock(spec=httpx.Client))

        connection.close()

        with self.assertRaises(exceptions.Error):
            connection.close()

    def test_commits_nothing(self):
        """
        This is just a sanity test, to make sure we follow the expected
        interface.
        """
        connection = db.Connection()

        connection.commit()

    def test_executes_a_statement(self):
        """
        This test tests whether the library is capable of executing statements
        against Pinot by sending requests to it via its API endpoints.

        With this test we're not yet focusing on how the request format or
        anything like that, since it's not the Connection's responsibility to
        do that.
        """
        connection = db.Connection(
            host='localhost', session=MagicMock(spec=httpx.Client))
        connection.session.is_closed = False
        response = connection.session.post.return_value
        response.json.return_value = {
            'numServersResponded': 1,
            'numServersQueried': 1,
        }
        response.status_code = 200

        cursor = connection.execute('some statement')

        self.assertIsInstance(cursor, db.Cursor)

    def test_uses_cursor_in_context_manager_block(self):
        connection = db.Connection(
            host='localhost', session=MagicMock(spec=httpx.Client))
        connection.session.is_closed = False

        with connection as cursor:
            self.assertIsInstance(cursor, db.Cursor)
            self.assertFalse(cursor.closed)

        self.assertTrue(cursor.closed)

    def test_connects_sync_via_function(self):
        connection = db.connect(
            host='localhost', session=MagicMock(spec=httpx.Client))

        self.assertIsInstance(connection, db.Connection)


class AsyncConnectionTest(IsolatedAsyncioTestCase):
    def test_starts_without_session_by_default(self):
        connection = db.AsyncConnection()

        self.assertIsNone(connection.session)
        self.assertFalse(connection.is_session_external)

    def test_verifies_httpx_session_upon_initializing_if_provided(self):
        client = httpx.AsyncClient()

        connection = db.AsyncConnection(session=client)

        self.assertIs(connection.session, client)
        self.assertTrue(connection.is_session_external)

    def test_verifies_httpx_session(self):
        client = httpx.AsyncClient()
        connection = db.AsyncConnection()
        connection.session = client

        connection.verify_session()
        # All good, no errors.

    def test_fails_to_verify_session_if_unexpected_type(self):
        connection = db.AsyncConnection()
        connection.session = object()

        with self.assertRaises(AssertionError):
            connection.verify_session()

    def test_bypasses_verification_if_no_session_initialized(self):
        connection = db.AsyncConnection()

        connection.verify_session()

    async def test_uses_cursor_in_context_manager_block(self):
        connection = db.AsyncConnection(
            host='localhost', session=MagicMock(spec=httpx.AsyncClient))
        connection.session.is_closed = False

        async with connection as cursor:
            self.assertIsInstance(cursor, db.AsyncCursor)
            self.assertFalse(cursor.closed)

        self.assertTrue(cursor.closed)

    async def test_renews_session_if_closed_when_getting_cursor(self):
        connection = db.AsyncConnection(host='localhost')
        connection.cursor()
        session1 = connection.session

        await session1.aclose()
        connection.cursor()
        session2 = connection.session

        self.assertIsNot(session1, session2)

    async def test_closes_connection_even_if_cursor_already_closed(self):
        connection = db.AsyncConnection(
            host='localhost', session=MagicMock(spec=httpx.AsyncClient))
        cursor = connection.cursor()
        await cursor.close()

        await connection.close()

        self.assertTrue(connection.closed)
        self.assertTrue(cursor.closed)

    async def test_closes_underlying_session_as_well(self):
        connection = db.AsyncConnection(
            host='localhost', session=MagicMock(spec=httpx.AsyncClient))
        # Just to simulate an implicitly created session.
        connection.is_session_external = False

        await connection.close()

        self.assertTrue(connection.session.aclose.called)

    async def test_executes_a_statement(self):
        """
        This test tests whether the library is capable of executing statements
        against Pinot by sending requests to it via its API endpoints.

        With this test we're not yet focusing on the request format or
        anything like that, since it's not the Connection's responsibility to
        do that.
        """
        connection = db.AsyncConnection(
            host='localhost', session=AsyncMock(spec=httpx.AsyncClient))
        connection.session.is_closed = False
        response = connection.session.post.return_value
        response.json = MagicMock()
        response.json.return_value = {
            'numServersResponded': 1,
            'numServersQueried': 1,
        }
        response.status_code = 200

        cursor = await connection.execute('some statement')

        self.assertIsInstance(cursor, db.AsyncCursor)

    def test_connects_async_via_function(self):
        connection = db.connect_async()

        self.assertIsInstance(connection, db.AsyncConnection)


class CursorTest(TestCase):
    def create_cursor(
            self, result_table: Optional[Dict[str, Any]] = None,
            status_code: int = 200, debug: bool = False,
            extra_payload: Optional[Dict[str, Any]] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            preserve_types: bool = False,
    ) -> db.Cursor:
        cursor = db.Cursor(
            host='localhost', session=MagicMock(spec=httpx.Client),
            debug=debug, username=username, password=password,
            preserve_types=preserve_types,
        )
        cursor.session.is_closed = False
        response = cursor.session.post.return_value
        payload = {
            'numServersResponded': 1,
            'numServersQueried': 1,
        }
        if result_table is not None:
            payload['resultTable'] = result_table
        if extra_payload:
            payload.update(extra_payload)
        response.json.return_value = payload
        response.status_code = status_code
        return cursor

    def test_instantiates_with_basic_url(self):
        cursor = db.Cursor(host='localhost', session=httpx.Client())

        self.assertEqual(cursor.url, 'http://localhost:8099/query/sql')

    def test_instantiates_with_auth(self):
        cursor = db.Cursor(
            host='localhost', session=httpx.Client(),
            username='john', password='my-pass')

        self.assertIsInstance(cursor.auth, httpx.DigestAuth)

    def test_instantiates_with_extra_headers(self):
        cursor = db.Cursor(
            host='localhost', session=httpx.Client(),
            extra_request_headers='foo=bar,baz=yo')

        self.assertEqual(cursor.session.headers['foo'], 'bar')
        self.assertEqual(cursor.session.headers['baz'], 'yo')

    def test_checks_valid_exception_if_not_containing_error_code(self):
        cursor = db.Cursor(host='localhost', session=httpx.Client())

        self.assertTrue(cursor.is_valid_exception({}))

    def test_checks_valid_exception_if_error_code_not_ignored(self):
        cursor = db.Cursor(host='localhost', session=httpx.Client())

        self.assertTrue(cursor.is_valid_exception({
            'errorCode': 123,
        }))

    def test_checks_invalid_exception_if_error_code_ignored(self):
        cursor = db.Cursor(
            host='localhost', session=httpx.Client(),
            ignore_exception_error_codes='123,234')

        self.assertFalse(cursor.is_valid_exception({
            'errorCode': 123,
        }))

    def test_cant_close_connection_twice(self):
        cursor = db.Cursor(host='localhost', session=httpx.Client())

        cursor.close()

        with self.assertRaises(exceptions.Error):
            cursor.close()

    def test_closes_underlying_session_as_well(self):
        cursor = db.Cursor(host='localhost', session=httpx.Client())

        cursor.close()

        self.assertTrue(cursor.session.is_closed)

    def test_bypasses_session_close_if_already_closed(self):
        cursor = db.Cursor(host='localhost', session=httpx.Client())
        cursor.session.close()

        cursor.close()

    def test_checks_sufficient_responded_0_queried_0_responded(self):
        cursor = db.Cursor(host='localhost', session=httpx.Client())

        cursor.check_sufficient_responded('foo', 0, 0)

    def test_checks_sufficient_responded_min1_queried_min1_responded(self):
        cursor = db.Cursor(host='localhost', session=httpx.Client())

        with self.assertRaises(exceptions.DatabaseError):
            cursor.check_sufficient_responded('foo', -1, -1)

    def test_checks_sufficient_responded_3_queried_3_responded(self):
        cursor = db.Cursor(host='localhost', session=httpx.Client())

        cursor.check_sufficient_responded('foo', 3, 3)

    def test_checks_sufficient_responded_5_queried_3_responded(self):
        cursor = db.Cursor(
            host='localhost', session=httpx.Client(),
            acceptable_respond_fraction=2)

        cursor.check_sufficient_responded('foo', 5, 3)

    def test_checks_sufficient_responded_4_queried_half_responded(self):
        cursor = db.Cursor(
            host='localhost', session=httpx.Client(),
            acceptable_respond_fraction=0.5)

        cursor.check_sufficient_responded('foo', 4, 2)

    def test_checks_sufficient_responded_but_zero_needed(self):
        cursor = db.Cursor(
            host='localhost', session=httpx.Client(),
            acceptable_respond_fraction=0)

        cursor.check_sufficient_responded('foo', 10, 0)

    def test_does_not_allow_fetching_if_not_executed_yet(self):
        cursor = db.Cursor(
            host='localhost', session=httpx.Client())

        with self.assertRaises(exceptions.Error):
            cursor.fetchone()

    def test_executes_query_within_session_with_empty_results(self):
        cursor = self.create_cursor()

        cursor.execute('some statement')

        results = list(iter(cursor))
        self.assertEqual(results, [])
        cursor.session.post.assert_called_once_with(
            'http://localhost:8099/query/sql', json={'sql': 'some statement'})

    def test_executes_query_within_session_with_query_options(self):
        cursor = self.create_cursor()

        cursor.execute('some statement', queryOptions={'foo': 'bar'})

        results = list(iter(cursor))
        self.assertEqual(results, [])
        cursor.session.post.assert_called_once_with(
            'http://localhost:8099/query/sql', json={
                'sql': 'some statement', 'queryOptions': {'foo': 'bar'}})

    def test_executes_query_preserving_types(self):
        cursor = self.create_cursor(preserve_types=True)

        cursor.execute('some statement')

        results = list(iter(cursor))
        self.assertEqual(results, [])
        cursor.session.post.assert_called_once_with(
            'http://localhost:8099/query/sql',
            json={'sql': "some statement OPTION(preserveType='true')"})

    def test_executes_query_with_complex_results(self):
        data = [
            ('age', 'INT', 12),
            ('name', 'STRING', 'John'),
            ('is_old', 'BOOLEAN', False),
            ('born_at', 'TIMESTAMP', '2010-01-01T00:30'),
            ('extras', 'JSON', '{"foo": "bar"}'),
            ('pet_peeve', 'UNKNOWN', 'bicycles'),
        ]
        cursor = self.create_cursor({
            'dataSchema': {
                'columnNames': [d[0] for d in data],
                'columnDataTypes': [d[1] for d in data],
            },
            'rows': [
                [d[2] for d in data],
            ],
        })

        cursor.execute('some statement')

        results = list(iter(cursor))
        self.assertEqual(results, [
            [12, 'John', False, datetime.datetime(2010, 1, 1, 0, 30),
             {'foo': 'bar'}, '"bicycles"'],
        ])

    def test_executes_query_with_simple_results(self):
        cursor = self.create_cursor({
            'dataSchema': {
                'columnNames': ['age'],
                'columnDataTypes': ['INT'],
            },
            'rows': [
                [12],
            ],
        })

        cursor.execute('some statement')

        results = list(iter(cursor))
        self.assertEqual(results, [
            [12],
        ])

    def test_executes_query_with_none(self):
        cursor = self.create_cursor({
            'dataSchema': {
                'columnNames': ['age'],
                'columnDataTypes': ['UNKNOWN'],
            },
            'rows': [
                [None],
            ],
        })

        cursor.execute('some statement')

        results = list(iter(cursor))
        self.assertEqual(results, [
            [None],
        ])

    def test_executes_query_with_auth(self):
        cursor = self.create_cursor({
            'dataSchema': {
                'columnNames': ['age'],
                'columnDataTypes': ['INT'],
            },
            'rows': [
                [1],
            ],
        }, username='john.doe', password='mypass')

        cursor.execute('some statement')

        cursor.session.post.assert_called_once_with(
            'http://localhost:8099/query/sql',
            json={'sql': 'some statement'},
            auth=(b'john.doe', b'mypass'),
        )

    def test_raises_database_error_if_problem_with_json(self):
        cursor = db.Cursor(
            host='localhost', session=MagicMock(spec=httpx.Client))
        cursor.session.is_closed = False
        response = cursor.session.post.return_value
        response.json.side_effect = ValueError()
        response.status_code = 200

        with self.assertRaises(exceptions.DatabaseError):
            cursor.execute('some statement')

    def test_raises_database_error_if_server_exception(self):
        cursor = self.create_cursor({}, extra_payload={
            'exceptions': ['something', 'wrong']
        })

        with self.assertRaises(exceptions.DatabaseError):
            cursor.execute('some statement')

    def test_raises_database_error_if_no_column_names(self):
        cursor = self.create_cursor({
            'dataSchema': {
                'columnNames': [],
                'columnDataTypes': [],
            },
            'rows': [],
        })

        with self.assertRaises(exceptions.DatabaseError):
            cursor.execute('some statement')

    def test_executes_query_with_results_and_debug_enabled(self):
        data = [
            ('age', 'INT', 12),
        ]
        cursor = self.create_cursor({
            'dataSchema': {
                'columnNames': [d[0] for d in data],
                'columnDataTypes': [d[1] for d in data],
            },
            'rows': [
                [d[2] for d in data],
            ],
        }, debug=True)

        with patch.object(db, 'logger') as mock_logger:
            cursor.execute('some statement')

            self.assertGreater(len(mock_logger.info.mock_calls), 0)

    def test_raises_exception_if_error_in_status_code(self):
        cursor = self.create_cursor({}, status_code=400)

        with self.assertRaises(exceptions.ProgrammingError):
            cursor.execute('some statement')

    def test_cannot_execute_many(self):
        cursor = self.create_cursor({})

        with self.assertRaises(exceptions.NotSupportedError):
            cursor.executemany('some statement')

    def test_fetches_many_results(self):
        cursor = self.create_cursor({
            'dataSchema': {
                'columnNames': ['age'],
                'columnDataTypes': ['INT'],
            },
            'rows': [[1], [2], [3]],
        })

        cursor.execute('some statement')

        self.assertEqual(cursor.fetchmany(2), [[1], [2]])

    def test_fetches_all_results(self):
        cursor = self.create_cursor({
            'dataSchema': {
                'columnNames': ['age'],
                'columnDataTypes': ['INT'],
            },
            'rows': [[1], [2], [3]],
        })

        cursor.execute('some statement')

        self.assertEqual(cursor.fetchall(), [[1], [2], [3]])

    def test_fetches_with_schema(self):
        cursor = self.create_cursor({
            'dataSchema': {
                'columnNames': ['age'],
                'columnDataTypes': ['INT'],
            },
            'rows': [[1]],
        })

        cursor.execute('some statement')

        self.assertEqual(cursor.fetchwithschema(), {
            'results': [[1]],
            'schema': [{'name': 'age', 'type': 'INT'}]
        })

    def test_does_nothing_for_setinputsizes(self):
        cursor = self.create_cursor()

        cursor.setinputsizes(123)

        # All good, nothing happened

    def test_does_nothing_for_setoutputsizes(self):
        cursor = self.create_cursor()

        cursor.setoutputsizes(123)

        # All good, nothing happened


class AsyncCursorTest(IsolatedAsyncioTestCase):
    def create_cursor(
            self, result_table: Optional[Dict[str, Any]] = None,
            status_code: int = 200, debug: bool = False,
            extra_payload: Optional[Dict[str, Any]] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            preserve_types: bool = False,
    ) -> db.AsyncCursor:
        cursor = db.AsyncCursor(
            host='localhost', session=AsyncMock(spec=httpx.AsyncClient),
            debug=debug, username=username, password=password,
            preserve_types=preserve_types,
        )
        cursor.session.is_closed = False
        payload = {
            'numServersResponded': 1,
            'numServersQueried': 1,
        }
        if result_table is not None:
            payload['resultTable'] = result_table
        if extra_payload:
            payload.update(extra_payload)
        response = httpx.Response(200, json=payload)
        cursor.session.post.return_value = response
        return cursor

    async def test_executes_query_with_auth(self):
        cursor = self.create_cursor({
            'dataSchema': {
                'columnNames': ['age'],
                'columnDataTypes': ['INT'],
            },
            'rows': [
                [1],
            ],
        }, username='john.doe', password='mypass')

        await cursor.execute('some statement')

        cursor.session.post.assert_called_once_with(
            'http://localhost:8099/query/sql',
            json={'sql': 'some statement'},
            auth=(b'john.doe', b'mypass'),
        )


class EscapeTest(TestCase):
    def test_escapes_asterisk(self):
        self.assertEqual(db.escape('*'), '*')

    def test_escapes_string(self):
        self.assertEqual(db.escape("what 'foo' means"), "'what ''foo'' means'")

    def test_escapes_int(self):
        self.assertEqual(db.escape(1), 1)

    def test_escapes_float(self):
        self.assertEqual(db.escape(1.0), 1.0)

    def test_escapes_bool(self):
        self.assertEqual(db.escape(True), 'TRUE')
        self.assertEqual(db.escape(False), 'FALSE')

    def test_escapes_list(self):
        self.assertEqual(db.escape([1, 'two']), "1, 'two'")

    def test_bypasses_escaping_unknown_types(self):
        self.assertEqual(db.escape({1, 2}), {1, 2})
