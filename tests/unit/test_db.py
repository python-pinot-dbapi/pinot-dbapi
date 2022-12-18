from unittest import TestCase

from pinotdb import db


class ConnectionTest(TestCase):
    def test_starts_without_session_by_default(self):
        connection = db.Connection()

        self.assertIsNone(connection.session)
        self.assertFalse(connection.is_session_external)
