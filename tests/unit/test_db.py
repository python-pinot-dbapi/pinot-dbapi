from unittest import TestCase

import httpx

from pinotdb import db


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
