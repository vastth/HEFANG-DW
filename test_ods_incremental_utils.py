# -*- coding: utf-8 -*-

import unittest

from etl_ods_incremental_utils import delete_existing_ids


class _FakeConnection:
    def __init__(self):
        self.calls = []

    def execute(self, statement, params):
        self.calls.append((str(statement), params))


class _FakeBegin:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class _FakeEngine:
    def __init__(self):
        self.connection = _FakeConnection()

    def begin(self):
        return _FakeBegin(self.connection)


class TestOdsIncrementalUtils(unittest.TestCase):
    def test_delete_existing_ids_batches_and_deduplicates(self):
        engine = _FakeEngine()

        delete_existing_ids(engine, 'ods_m_retail', [1, 2, 2, None, 3, '4'], batch_size=2)

        self.assertEqual(
            engine.connection.calls,
            [
                (
                    'DELETE FROM ods_m_retail WHERE id IN (:id_0, :id_1)',
                    {'id_0': 1, 'id_1': 2},
                ),
                (
                    'DELETE FROM ods_m_retail WHERE id IN (:id_0, :id_1)',
                    {'id_0': 3, 'id_1': 4},
                ),
            ],
        )

    def test_delete_existing_ids_skips_empty_payload(self):
        engine = _FakeEngine()

        delete_existing_ids(engine, 'ods_m_retailitem', [None, '', 'bad-id'])

        self.assertEqual(engine.connection.calls, [])

    def test_delete_existing_ids_supports_existing_connection(self):
        connection = _FakeConnection()

        delete_existing_ids(connection, 'ods_m_retail', [9, '10', 9], batch_size=10)

        self.assertEqual(
            connection.calls,
            [
                (
                    'DELETE FROM ods_m_retail WHERE id IN (:id_0, :id_1)',
                    {'id_0': 9, 'id_1': 10},
                ),
            ],
        )


if __name__ == '__main__':
    unittest.main()