# -*- coding: utf-8 -*-

import unittest

from tools.check_ods_incremental import _fetch_duplicate_id_count


class _FakeResult:
    def __init__(self, rows=None, scalar_value=None):
        self._rows = rows or []
        self._scalar_value = scalar_value

    def fetchall(self):
        return self._rows

    def scalar(self):
        return self._scalar_value


class _FakeConnection:
    def __init__(self, results):
        self._results = list(results)
        self.executed_sql = []

    def execute(self, statement, params):
        self.executed_sql.append((str(statement), params))
        return self._results.pop(0)


class _FakeConnectContext:
    def __init__(self, connection):
        self._connection = connection

    def __enter__(self):
        return self._connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class _FakeEngine:
    def __init__(self, results):
        self.connection = _FakeConnection(results)

    def connect(self):
        return _FakeConnectContext(self.connection)


class TestCheckOdsIncremental(unittest.TestCase):
    def test_duplicate_check_short_circuits_when_single_column_unique_index_exists(self):
        engine = _FakeEngine(
            [
                _FakeResult(rows=[{"index_name": "uk_ods_m_retailitem_id", "column_name": "id", "seq_in_index": 1}]),
            ]
        )

        duplicate_count = _fetch_duplicate_id_count(
            engine,
            "ods_m_retailitem",
            "SELECT COUNT(*) FROM duplicate_probe",
            {"start_date": 20260420},
        )

        self.assertEqual(duplicate_count, 0)
        self.assertEqual(len(engine.connection.executed_sql), 1)
        self.assertIn("information_schema.statistics", engine.connection.executed_sql[0][0])

    def test_duplicate_check_falls_back_to_duplicate_query_when_unique_index_missing(self):
        engine = _FakeEngine(
            [
                _FakeResult(rows=[{"index_name": "uk_pair", "column_name": "id", "seq_in_index": 1}, {"index_name": "uk_pair", "column_name": "m_retail_id", "seq_in_index": 2}]),
                _FakeResult(scalar_value=3),
            ]
        )

        duplicate_count = _fetch_duplicate_id_count(
            engine,
            "ods_m_retailitem",
            "SELECT COUNT(*) FROM duplicate_probe WHERE start_date = :start_date",
            {"start_date": 20260420},
        )

        self.assertEqual(duplicate_count, 3)
        self.assertEqual(len(engine.connection.executed_sql), 2)
        self.assertIn("duplicate_probe", engine.connection.executed_sql[1][0])
        self.assertEqual(engine.connection.executed_sql[1][1], {"start_date": 20260420})


if __name__ == '__main__':
    unittest.main()