import unittest

class TestCase(unittest.TestCase):
    """TestCase is a wrapper around unittest.TestCase that evaluates connection
    to existing Pinot cluster and loads test data."""

    _db_url: str = "localhost:9000"

    def __init__(self, method_name='runTest'):
        super().__init__(method_name)

        # Override `_db_url` if configured.
        self._db_url = getattr(self, "db_url", self._db_url)
