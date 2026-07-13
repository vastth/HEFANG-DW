import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import etl_ods_m_retailitem as retailitem_etl


class _DummyConnection:
    def execute(self, *_args, **_kwargs):
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _DummyEngine:
    def connect(self):
        return _DummyConnection()

    def begin(self):
        return _DummyConnection()

    def dispose(self):
        return None


class RetailItemIncrementalFallbackTests(unittest.TestCase):
    def test_build_double_null_fallback_query_uses_head_modifieddate_window(self):
        query = retailitem_etl._build_double_null_fallback_query()

        self.assertIn('JOIN M_RETAIL r ON r.ID = ri.M_RETAIL_ID', query)
        self.assertIn('ri.MODIFIEDDATE IS NULL', query)
        self.assertIn('ri.SETTIME IS NULL', query)
        self.assertIn('r.MODIFIEDDATE AS retail_modifieddate', query)
        self.assertIn('r.MODIFIEDDATE >= :start_time', query)
        self.assertIn('r.MODIFIEDDATE < :end_time', query)
        self.assertIn('ORDER BY r.MODIFIEDDATE, ri.ID', query)

    def test_incremental_mode_reads_double_null_fallback_channel(self):
        last_sync = datetime(2026, 5, 12, 17, 11, 13)
        as_of = last_sync + timedelta(days=1)
        read_queries = []

        def fake_read_sql(query, *_args, **_kwargs):
            read_queries.append(query)
            return []

        with patch.object(retailitem_etl, 'create_oracle_engine', return_value=_DummyEngine()), \
                patch.object(retailitem_etl, 'create_mysql_engine', return_value=_DummyEngine()), \
                patch.object(
                    retailitem_etl,
                    '_get_sync_state',
                    side_effect=[
                        {
                            'last_sync': last_sync,
                            'current_window_start': None,
                            'current_window_end': None,
                            'status': 'success',
                        },
                        None,
                    ],
                ), \
                patch.object(retailitem_etl, '_get_settime_range', return_value=(None, None)), \
                patch.object(retailitem_etl, '_update_window_state'), \
                patch.object(retailitem_etl, '_update_sync_state'), \
                patch.object(retailitem_etl.pd, 'read_sql', side_effect=fake_read_sql):
            rows = retailitem_etl.extract_and_load(
                mode='incremental',
                backfill_days=0,
                window_days=1,
                as_of=as_of,
            )

        self.assertEqual(rows, 0)
        self.assertTrue(
            any('r.MODIFIEDDATE AS retail_modifieddate' in query for query in read_queries),
            '增量模式未触发双空明细的头单 modifieddate 兜底查询',
        )


if __name__ == '__main__':
    unittest.main()