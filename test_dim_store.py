# -*- coding: utf-8 -*-

import unittest

import pandas as pd
from sqlalchemy import create_engine, text

from etl_dim_store import DIM_STORE_LOAD_COLUMNS, EXTRACT_SQL, _validate_target_columns, transform


class TestDimStoreExtractSql(unittest.TestCase):
    def test_extract_sql_keeps_inactive_store_rows(self):
        normalized_sql = ' '.join(EXTRACT_SQL.split()).upper()

        self.assertIn('S.ISACTIVE AS IS_ACTIVE', normalized_sql)
        self.assertNotIn("WHERE S.ISACTIVE = 'Y'", normalized_sql)

    def test_extract_sql_safely_converts_invalid_open_date_to_null(self):
        normalized_sql = ' '.join(EXTRACT_SQL.split()).upper()

        self.assertIn('S.OPENDATE AS SOURCE_OPENDATE_RAW', normalized_sql)
        self.assertIn('DEFAULT NULL ON CONVERSION ERROR', normalized_sql)
        self.assertIn("'YYYYMMDD'", normalized_sql)
        self.assertIn('AS SOURCE_OPENDATE_IS_INVALID', normalized_sql)
        self.assertIn('AS OPEN_DATE', normalized_sql)

    def test_transform_drops_source_open_date_audit_columns(self):
        source_df = pd.DataFrame([
            {
                'store_id': 1,
                'store_code': 'RT001',
                'store_name': '测试门店',
                'area_id': 1,
                'area_name': '测试区域',
                'is_warehouse': 0,
                'is_store': 1,
                'is_cloud_store': 'N',
                'is_center': 'N',
                'store_type': '门店',
                'is_active': 'Y',
                'source_opendate_raw': 20260230,
                'source_opendate_is_null': 0,
                'source_opendate_is_invalid': 1,
                'open_date': None,
            }
        ])

        transformed = transform(source_df)

        self.assertEqual(tuple(transformed.columns), DIM_STORE_LOAD_COLUMNS)
        self.assertNotIn('source_opendate_raw', transformed.columns)
        self.assertTrue(pd.isna(transformed.loc[0, 'open_date']))

    def test_target_schema_requires_open_date_before_truncate(self):
        with self.assertRaisesRegex(RuntimeError, 'open_date'):
            _validate_target_columns(set(DIM_STORE_LOAD_COLUMNS) - {'open_date'})

    def test_rolled_back_named_column_insert_accepts_retained_nullable_open_date(self):
        engine = create_engine('sqlite:///:memory:')
        with engine.begin() as conn:
            conn.execute(text(
                """
                CREATE TABLE dim_store (
                    store_id INTEGER NOT NULL,
                    store_code TEXT NOT NULL,
                    open_date DATE NULL
                )
                """
            ))

        legacy_frame = pd.DataFrame([{'store_id': 1, 'store_code': 'RT001'}])
        legacy_frame.to_sql('dim_store', con=engine, if_exists='append', index=False)

        with engine.connect() as conn:
            row = conn.execute(text('SELECT store_id, store_code, open_date FROM dim_store')).one()
        self.assertEqual((row[0], row[1]), (1, 'RT001'))
        self.assertIsNone(row[2])
        engine.dispose()


if __name__ == '__main__':
    unittest.main()