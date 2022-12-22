from unittest import TestCase
from unittest.mock import MagicMock

import httpx

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
