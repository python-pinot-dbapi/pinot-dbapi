import asyncio
import os
import unittest

from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine


def _get_pinot_urls():
    host = os.getenv("PINOT_HOST", "localhost")
    broker_port = int(os.getenv("PINOT_BROKER_PORT", "8000"))
    controller_port = int(os.getenv("PINOT_CONTROLLER_PORT", "9000"))
    sync_url = (
        f"pinot://{host}:{broker_port}/query/sql"
        f"?controller=http://{host}:{controller_port}/"
    )
    async_url = (
        f"pinot+async://{host}:{broker_port}/query/sql"
        f"?controller=http://{host}:{controller_port}/"
    )
    return sync_url, async_url


class SQLAlchemySyncAsyncIntegrationTest(unittest.TestCase):
    count_query = text("SELECT count(*) FROM baseballStats LIMIT 1")
    ordered_rows_query = text(
        "SELECT playerID, yearID "
        "FROM baseballStats "
        "ORDER BY playerID, yearID "
        "LIMIT 5"
    )

    def _run_sync_queries(self, url):
        engine = create_engine(url)
        try:
            with engine.connect() as connection:
                count_result = connection.execute(self.count_query)
                row_result = connection.execute(self.ordered_rows_query)
                count_value = count_result.scalar_one()
                rows = [tuple(row) for row in row_result.fetchall()]
        finally:
            engine.dispose()
        return count_value, rows

    async def _run_async_queries(self, url):
        engine = create_async_engine(url)
        try:
            async with engine.connect() as connection:
                count_result = await connection.execute(self.count_query)
                row_result = await connection.execute(self.ordered_rows_query)
                count_value = count_result.scalar_one()
                rows = [tuple(row) for row in row_result.fetchall()]
        finally:
            await engine.dispose()
        return count_value, rows

    def test_sync_and_async_sqlalchemy_results_are_valid(self):
        sync_url, async_url = _get_pinot_urls()

        sync_count, sync_rows = self._run_sync_queries(sync_url)
        async_count, async_rows = asyncio.run(
            self._run_async_queries(async_url)
        )

        self.assertGreater(int(sync_count), 0)
        self.assertGreater(int(async_count), 0)
        self.assertEqual(int(sync_count), int(async_count))

        self.assertEqual(len(sync_rows), 5)
        self.assertEqual(len(async_rows), 5)
        self.assertEqual(sync_rows, async_rows)
        for row in sync_rows:
            self.assertEqual(len(row), 2)
            self.assertIsNotNone(row[0])
            self.assertIsNotNone(row[1])
