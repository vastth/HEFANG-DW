# -*- coding: utf-8 -*-

import unittest
from datetime import date

from tools.import_store_operation_owner_from_nas import (
    ExpectedEntity,
    SnapshotRow,
    SourceRow,
    _build_validation_warning_messages,
    _build_snapshot_rows,
    _classify_history_changes,
)


class TestStoreOperationOwnerImport(unittest.TestCase):
    def test_build_snapshot_rows_tolerates_absorbed_store_row_when_subject_present(self):
        source_rows = [
            SourceRow(
                row_number=10,
                entity_type='STORE',
                entity_code='RT007',
                entity_name='深圳万象天地店',
                owner_name='Amor',
                remark=None,
                effective_start_date=None,
                effective_end_date=None,
            ),
            SourceRow(
                row_number=11,
                entity_type='SUBJECT',
                entity_code='SUBJ_SZ_WXTD',
                entity_name='深圳万象天地经营体',
                owner_name='Amor',
                remark=None,
                effective_start_date=None,
                effective_end_date=None,
            ),
        ]
        expected_entities = {
            ('SUBJECT', 'SUBJ_SZ_WXTD'): ExpectedEntity(
                entity_type='SUBJECT',
                entity_id=20,
                entity_code='SUBJ_SZ_WXTD',
                entity_name='深圳万象天地经营体',
            )
        }

        snapshot_rows, validation = _build_snapshot_rows(
            source_rows=source_rows,
            expected_entities=expected_entities,
            snapshot_date=date(2026, 4, 21),
            month_store_subject_map={'RT007': 'SUBJ_SZ_WXTD'},
            month_subject_store_map={'SUBJ_SZ_WXTD': {'RT007'}},
        )

        self.assertEqual(len(snapshot_rows), 1)
        self.assertEqual(snapshot_rows[0].entity_code, 'SUBJ_SZ_WXTD')
        self.assertEqual(validation['matched_entity_count'], 1)
        self.assertEqual(validation['unexpected_entities'], [])
        self.assertEqual(validation['missing_entities'], [])
        self.assertEqual(
            validation['tolerated_transition_entities'],
            [
                {
                    'entity_type': 'STORE',
                    'entity_code': 'RT007',
                    'row_number': 10,
                    'entity_name': '深圳万象天地店',
                    'reason': 'store_absorbed_by_subject',
                    'related_subject_code': 'SUBJ_SZ_WXTD',
                }
            ],
        )

    def test_build_snapshot_rows_tolerates_subject_row_before_effective_date_when_store_still_present(self):
        source_rows = [
            SourceRow(
                row_number=20,
                entity_type='STORE',
                entity_code='RT045',
                entity_name='广州天环广场店',
                owner_name='Toby',
                remark=None,
                effective_start_date=None,
                effective_end_date=None,
            ),
            SourceRow(
                row_number=21,
                entity_type='SUBJECT',
                entity_code='SUBJ_GZTH',
                entity_name='广州天环广场经营体',
                owner_name='Toby',
                remark=None,
                effective_start_date=None,
                effective_end_date=None,
            ),
        ]
        expected_entities = {
            ('STORE', 'RT045'): ExpectedEntity(
                entity_type='STORE',
                entity_id=288,
                entity_code='RT045',
                entity_name='广州天环广场店',
            )
        }

        snapshot_rows, validation = _build_snapshot_rows(
            source_rows=source_rows,
            expected_entities=expected_entities,
            snapshot_date=date(2026, 6, 18),
            month_store_subject_map={'RT045': 'SUBJ_GZTH', 'RT140': 'SUBJ_GZTH'},
            month_subject_store_map={'SUBJ_GZTH': {'RT045', 'RT140'}},
        )

        self.assertEqual(len(snapshot_rows), 1)
        self.assertEqual(snapshot_rows[0].entity_code, 'RT045')
        self.assertEqual(validation['missing_entities'], [])
        self.assertEqual(validation['unexpected_entities'], [])
        self.assertEqual(
            validation['tolerated_transition_entities'][0]['reason'],
            'subject_prepared_before_effective_date',
        )
        self.assertEqual(
            validation['tolerated_transition_entities'][0]['related_store_codes'],
            ['RT045'],
        )

    def test_build_validation_warning_messages_reports_tolerated_transition_entities(self):
        warning_messages = _build_validation_warning_messages(
            {
                'tolerated_transition_entities': [
                    {
                        'entity_type': 'STORE',
                        'entity_code': 'RT045',
                        'row_number': 20,
                        'entity_name': '广州天环广场店',
                        'reason': 'store_absorbed_by_subject',
                        'related_subject_code': 'SUBJ_GZTH',
                    }
                ]
            }
        )

        self.assertEqual(len(warning_messages), 1)
        self.assertIn('共同考核过渡期并存实体', warning_messages[0])
        self.assertIn('RT045 -> SUBJ_GZTH', warning_messages[0])

    def test_classify_history_changes_splits_changed_new_and_exited(self):
        snapshot_rows = [
            SnapshotRow(
                snapshot_date=date(2026, 4, 21),
                entity_type='SUBJECT',
                entity_id=20,
                entity_code='SUBJ_SZ_WXTD',
                entity_name='深圳万象天地经营体',
                owner_name='Bella',
                remark=None,
                effective_start_date=date(2026, 4, 21),
                effective_end_date=date(9999, 12, 31),
            ),
            SnapshotRow(
                snapshot_date=date(2026, 4, 21),
                entity_type='STORE',
                entity_id=318,
                entity_code='RT050',
                entity_name='北京国贸店',
                owner_name='Amor',
                remark=None,
                effective_start_date=date(2026, 4, 21),
                effective_end_date=date(9999, 12, 31),
            ),
        ]
        current_map = {
            ('SUBJECT', 'SUBJ_SZ_WXTD'): {
                'entity_type': 'SUBJECT',
                'entity_id': 20,
                'entity_code': 'SUBJ_SZ_WXTD',
                'entity_name': '深圳万象天地经营体',
                'owner_name': 'Amor',
                'effective_start_date': date(2026, 4, 1),
                'effective_end_date': date(9999, 12, 31),
                'is_current': 'Y',
            },
            ('STORE', 'RT068'): {
                'entity_type': 'STORE',
                'entity_id': 438,
                'entity_code': 'RT068',
                'entity_name': '南京德基广场店',
                'owner_name': 'Amor',
                'effective_start_date': date(2026, 4, 1),
                'effective_end_date': date(9999, 12, 31),
                'is_current': 'Y',
            },
        }

        classified = _classify_history_changes(snapshot_rows, current_map)

        self.assertEqual(len(classified['changed_rows']), 1)
        self.assertEqual(classified['changed_rows'][0]['entity_code'], 'SUBJ_SZ_WXTD')
        self.assertEqual(len(classified['new_rows']), 1)
        self.assertEqual(classified['new_rows'][0]['entity_code'], 'RT050')
        self.assertEqual(len(classified['exited_rows']), 1)
        self.assertEqual(classified['exited_rows'][0]['entity_code'], 'RT068')
        self.assertEqual(len(classified['unchanged_rows']), 0)

    def test_build_snapshot_rows_uses_explicit_effective_dates(self):
        source_rows = [
            SourceRow(
                row_number=12,
                entity_type='STORE',
                entity_code='RT117',
                entity_name='昆明万象城店',
                owner_name='Luna',
                remark='补录',
                effective_start_date=date(2026, 5, 9),
                effective_end_date=None,
            ),
        ]
        expected_entities = {
            ('STORE', 'RT117'): ExpectedEntity(
                entity_type='STORE',
                entity_id=748,
                entity_code='RT117',
                entity_name='昆明万象城店',
            )
        }

        snapshot_rows, validation = _build_snapshot_rows(
            source_rows=source_rows,
            expected_entities=expected_entities,
            snapshot_date=date(2026, 5, 12),
            month_store_subject_map={},
            month_subject_store_map={},
        )

        self.assertEqual(validation['invalid_effective_date_rows'], [])
        self.assertEqual(snapshot_rows[0].effective_start_date, date(2026, 5, 9))
        self.assertEqual(snapshot_rows[0].effective_end_date, date(9999, 12, 31))

    def test_build_snapshot_rows_rejects_interval_not_covering_snapshot_date(self):
        source_rows = [
            SourceRow(
                row_number=8,
                entity_type='STORE',
                entity_code='RT117',
                entity_name='昆明万象城店',
                owner_name='Luna',
                remark=None,
                effective_start_date=date(2026, 5, 13),
                effective_end_date=None,
            ),
        ]
        expected_entities = {
            ('STORE', 'RT117'): ExpectedEntity(
                entity_type='STORE',
                entity_id=748,
                entity_code='RT117',
                entity_name='昆明万象城店',
            )
        }

        snapshot_rows, validation = _build_snapshot_rows(
            source_rows=source_rows,
            expected_entities=expected_entities,
            snapshot_date=date(2026, 5, 12),
            month_store_subject_map={},
            month_subject_store_map={},
        )

        self.assertEqual(snapshot_rows, [])
        self.assertEqual(
            validation['invalid_effective_date_rows'],
            [
                {
                    'entity_type': 'STORE',
                    'entity_code': 'RT117',
                    'row_number': 8,
                    'snapshot_date': '2026-05-12',
                    'effective_start_date': '2026-05-13',
                    'effective_end_date': '9999-12-31',
                }
            ],
        )

    def test_build_snapshot_rows_ignores_expired_historical_row_not_in_current_scope(self):
        source_rows = [
            SourceRow(
                row_number=66,
                entity_type='STORE',
                entity_code='RT105',
                entity_name='昆明顺城购物中心店',
                owner_name='Gloria',
                remark=None,
                effective_start_date=None,
                effective_end_date=date(2026, 5, 8),
                has_explicit_effective_end_date=True,
            ),
            SourceRow(
                row_number=67,
                entity_type='STORE',
                entity_code='RT117',
                entity_name='昆明万象城店',
                owner_name='Luna',
                remark=None,
                effective_start_date=date(2026, 5, 9),
                effective_end_date=None,
                has_explicit_effective_start_date=True,
            ),
        ]
        expected_entities = {
            ('STORE', 'RT117'): ExpectedEntity(
                entity_type='STORE',
                entity_id=748,
                entity_code='RT117',
                entity_name='昆明万象城店',
            )
        }

        snapshot_rows, validation = _build_snapshot_rows(
            source_rows=source_rows,
            expected_entities=expected_entities,
            snapshot_date=date(2026, 5, 14),
            month_store_subject_map={},
            month_subject_store_map={},
        )

        self.assertEqual(len(snapshot_rows), 1)
        self.assertEqual(snapshot_rows[0].entity_code, 'RT117')
        self.assertEqual(validation['unexpected_entities'], [])
        self.assertEqual(validation['invalid_effective_date_rows'], [])
        self.assertEqual(validation['missing_entities'], [])

    def test_classify_history_changes_marks_backdated_interval_as_changed(self):
        snapshot_rows = [
            SnapshotRow(
                snapshot_date=date(2026, 5, 12),
                entity_type='STORE',
                entity_id=748,
                entity_code='RT117',
                entity_name='昆明万象城店',
                owner_name='Luna',
                remark=None,
                effective_start_date=date(2026, 5, 9),
                effective_end_date=date(9999, 12, 31),
                has_explicit_effective_start_date=True,
            ),
        ]
        current_map = {
            ('STORE', 'RT117'): {
                'entity_type': 'STORE',
                'entity_id': 748,
                'entity_code': 'RT117',
                'entity_name': '昆明万象城店',
                'owner_name': 'Luna',
                'effective_start_date': date(2026, 5, 12),
                'effective_end_date': date(9999, 12, 31),
                'is_current': 'Y',
            },
        }

        classified = _classify_history_changes(snapshot_rows, current_map)

        self.assertEqual(len(classified['changed_rows']), 1)
        self.assertEqual(classified['changed_rows'][0]['snapshot'].effective_start_date, date(2026, 5, 9))
        self.assertEqual(len(classified['unchanged_rows']), 0)

    def test_classify_history_changes_keeps_default_interval_rows_unchanged(self):
        snapshot_rows = [
            SnapshotRow(
                snapshot_date=date(2026, 5, 12),
                entity_type='STORE',
                entity_id=13,
                entity_code='RT001',
                entity_name='广州K11专卖店',
                owner_name='Kason',
                remark=None,
                effective_start_date=date(2026, 5, 12),
                effective_end_date=date(9999, 12, 31),
            ),
        ]
        current_map = {
            ('STORE', 'RT001'): {
                'entity_type': 'STORE',
                'entity_id': 13,
                'entity_code': 'RT001',
                'entity_name': '广州K11专卖店',
                'owner_name': 'Kason',
                'effective_start_date': date(2026, 4, 22),
                'effective_end_date': date(9999, 12, 31),
                'is_current': 'Y',
            },
        }

        classified = _classify_history_changes(snapshot_rows, current_map)

        self.assertEqual(len(classified['unchanged_rows']), 1)
        self.assertEqual(len(classified['changed_rows']), 0)


if __name__ == '__main__':
    unittest.main()