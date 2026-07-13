# -*- coding: utf-8 -*-

import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from scheduled_total_control import (
    CHAIN_DEFINITIONS,
    ChildRunResult,
    WECHAT_TEXT_MAX_BYTES,
    _build_python_command,
    _compose_total_control_alert,
    _compose_total_control_local_summary,
    build_parser,
    run_total_control,
)


def _build_child_result(chain_key, status, summary_lines, detail_lines=None, issue_lines=None, exit_code=0):
    return ChildRunResult(
        chain=CHAIN_DEFINITIONS[chain_key],
        exit_code=exit_code,
        summary={
            'chain_key': CHAIN_DEFINITIONS[chain_key].key,
            'chain_label': CHAIN_DEFINITIONS[chain_key].label,
            'status': status,
            'headline': f'{CHAIN_DEFINITIONS[chain_key].label}摘要',
            'started_at': '2026-04-27 10:00:00',
            'ended_at': '2026-04-27 10:05:00',
            'duration_seconds': 300,
            'summary_lines': summary_lines,
            'detail_lines': detail_lines or [],
            'issue_lines': issue_lines or [],
            'python_cleanup_exit_downgraded': False,
        },
    )


class TestScheduledTotalControl(unittest.TestCase):
    def test_compose_total_control_alert_keeps_high_level_sections_only(self):
        alert = _compose_total_control_alert(
            [
                _build_child_result(
                    'main',
                    'SUCCESS',
                    ['结果：成功8 / 警告1 / 失败0'],
                    detail_lines=['1. ✅ 商品维度: SUCCESS [31s]'],
                    issue_lines=['- 达播数据就绪检查: label=PENDING'],
                ),
                _build_child_result(
                    'store_daily_topic',
                    'SUCCESS',
                    ['动作：NAS 目标导入命中幂等跳过，但自然日兜底已补跑 ADS'],
                    detail_lines=['ADS批量重跑：2/2（门店层+主体层+销售看板+SKU汇总+组织汇总）'],
                ),
                _build_child_result(
                    'dws_v2_shadow',
                    'SUCCESS',
                    ['动作：shadow 调度完成'],
                    detail_lines=['销售对账：status=SUCCESS, mismatch_count=0'],
                ),
            ],
            overall_status='SUCCESS',
            started_at=datetime(2026, 4, 27, 10, 0, 0),
            ended_at=datetime(2026, 4, 27, 10, 6, 0),
            conn_test=False,
            main_only=False,
            topic_only=False,
            shadow_only=False,
        )

        self.assertIn('总控调度完成', alert)
        self.assertIn('主链调度: SUCCESS', alert)
        self.assertIn('门店销售专题: SUCCESS', alert)
        self.assertIn('DWS v2 Shadow: SUCCESS', alert)
        self.assertIn('📋 链路摘要', alert)
        self.assertIn('详细明细见日志', alert)
        self.assertIn('结果：成功8 / 警告1 / 失败0', alert)
        self.assertNotIn('🔎 明细', alert)
        self.assertNotIn('ADS批量重跑：2/2', alert)

    def test_compose_total_control_alert_stays_within_wechat_text_limit(self):
        long_summary_lines = [
            f'摘要{i}：门店专题批量重跑说明与对账结果 ' + '门店日报专题链路说明' * 10
            for i in range(1, 10)
        ]
        long_issue_lines = [
            f'异常{i}：负责人快照区间明细 ' + 'RT105 显式生效区间异常说明' * 8
            for i in range(1, 5)
        ]

        alert = _compose_total_control_alert(
            [
                _build_child_result(
                    'dws_v2_pre_refresh',
                    'SUCCESS',
                    long_summary_lines,
                    detail_lines=['不应出现在企微中的库存明细 ' + '明细' * 50],
                ),
                _build_child_result(
                    'main',
                    'FAILED',
                    long_summary_lines,
                    detail_lines=['不应出现在企微中的主链明细 ' + '明细' * 50],
                    issue_lines=long_issue_lines,
                    exit_code=2,
                ),
                _build_child_result(
                    'store_daily_topic',
                    'SUCCESS',
                    long_summary_lines,
                    detail_lines=['不应出现在企微中的专题明细 ' + '明细' * 50],
                ),
                _build_child_result(
                    'dws_v2_shadow',
                    'SKIPPED',
                    ['未执行：V2 模式已在主链前完成 DWS v2 读源预刷新，本轮不重复执行后置 shadow'],
                ),
            ],
            overall_status='FAILED',
            started_at=datetime(2026, 5, 18, 9, 49, 22),
            ended_at=datetime(2026, 5, 18, 10, 7, 33),
            conn_test=False,
            main_only=False,
            topic_only=False,
            shadow_only=False,
        )

        self.assertLessEqual(len(alert.encode('utf-8')), WECHAT_TEXT_MAX_BYTES)
        self.assertIn('详细明细见日志', alert)
        self.assertNotIn('不应出现在企微中的主链明细', alert)

    def test_compose_total_control_local_summary_strips_emoji(self):
        local_summary = _compose_total_control_local_summary(
            [
                _build_child_result(
                    'main',
                    'WARNING',
                    ['结果：成功8 / 警告1 / 失败0'],
                    detail_lines=['1. ✅ 商品维度: SUCCESS [31s]'],
                    issue_lines=['- ⚠️ 达播数据就绪检查: label=PENDING'],
                ),
            ],
            overall_status='SUCCESS',
            started_at=datetime(2026, 4, 27, 10, 0, 0),
            ended_at=datetime(2026, 4, 27, 10, 6, 0),
            conn_test=False,
            main_only=False,
            topic_only=False,
            shadow_only=False,
        )

        self.assertIn('链路摘要:', local_summary)
        self.assertIn('主链调度: WARNING', local_summary)
        self.assertIn('1. 商品维度: SUCCESS [31s]', local_summary)
        self.assertNotIn('✅', local_summary)
        self.assertNotIn('⚠️', local_summary)
        self.assertNotIn('❌', local_summary)

    @patch('scheduled_total_control.send_wechat_alert')
    @patch('scheduled_total_control._run_child')
    def test_run_total_control_sends_single_unified_alert_when_both_chains_succeed(
        self,
        mock_run_child,
        mock_send_wechat_alert,
    ):
        mock_run_child.side_effect = [
            _build_child_result('main', 'SUCCESS', ['结果：成功8 / 警告1 / 失败0']),
            _build_child_result('store_daily_topic', 'SUCCESS', ['动作：专题调度完成']),
            _build_child_result('dws_v2_shadow', 'SUCCESS', ['动作：shadow 调度完成']),
        ]

        exit_code = run_total_control()

        self.assertEqual(exit_code, 0)
        self.assertEqual(mock_run_child.call_count, 3)
        mock_send_wechat_alert.assert_called_once()
        alert_content = mock_send_wechat_alert.call_args.args[1]
        self.assertIn('主链调度: SUCCESS', alert_content)
        self.assertIn('门店销售专题: SUCCESS', alert_content)
        self.assertIn('动作：专题调度完成', alert_content)
        self.assertIn('DWS v2 Shadow: SUCCESS', alert_content)

    @patch('scheduled_total_control.send_wechat_alert')
    @patch('scheduled_total_control._run_child')
    def test_run_total_control_marks_topic_skipped_when_main_fails(
        self,
        mock_run_child,
        mock_send_wechat_alert,
    ):
        mock_run_child.return_value = _build_child_result(
            'main',
            'FAILED',
            ['结果：成功5 / 警告0 / 失败1'],
            issue_lines=['- 销售数据层: timeout'],
            exit_code=2,
        )

        exit_code = run_total_control()

        self.assertEqual(exit_code, 2)
        mock_send_wechat_alert.assert_called_once()
        alert_content = mock_send_wechat_alert.call_args.args[1]
        self.assertIn('主链调度: FAILED', alert_content)
        self.assertIn('门店销售专题: SKIPPED', alert_content)
        self.assertIn('DWS v2 Shadow: SKIPPED', alert_content)
        self.assertIn('未执行：主链调度失败，总控已停止后续专题链', alert_content)

    @patch('scheduled_total_control.send_wechat_alert')
    @patch('scheduled_total_control._run_child')
    def test_run_total_control_still_runs_shadow_when_topic_fails(
        self,
        mock_run_child,
        mock_send_wechat_alert,
    ):
        mock_run_child.side_effect = [
            _build_child_result('main', 'SUCCESS', ['结果：成功8 / 警告0 / 失败0']),
            _build_child_result('store_daily_topic', 'FAILED', ['动作：专题调度失败'], exit_code=2),
            _build_child_result('dws_v2_shadow', 'SUCCESS', ['动作：shadow 调度完成']),
        ]

        exit_code = run_total_control()

        self.assertEqual(exit_code, 2)
        self.assertEqual(mock_run_child.call_count, 3)
        mock_send_wechat_alert.assert_called_once()
        alert_content = mock_send_wechat_alert.call_args.args[1]
        self.assertIn('门店销售专题: FAILED', alert_content)
        self.assertIn('DWS v2 Shadow: SUCCESS', alert_content)

    @patch('scheduled_total_control.send_wechat_alert')
    @patch('scheduled_total_control._run_child')
    def test_run_total_control_shadow_failure_only_warns(
        self,
        mock_run_child,
        mock_send_wechat_alert,
    ):
        mock_run_child.side_effect = [
            _build_child_result('main', 'SUCCESS', ['结果：成功8 / 警告0 / 失败0']),
            _build_child_result('store_daily_topic', 'SUCCESS', ['动作：专题调度完成']),
            _build_child_result('dws_v2_shadow', 'FAILED', ['动作：shadow 调度失败'], exit_code=1),
        ]

        exit_code = run_total_control()

        self.assertEqual(exit_code, 0)
        self.assertEqual(mock_run_child.call_count, 3)
        mock_send_wechat_alert.assert_called_once()
        alert_content = mock_send_wechat_alert.call_args.args[1]
        self.assertIn('存在告警', alert_content)
        self.assertIn('DWS v2 Shadow: FAILED', alert_content)

    @patch('scheduled_total_control.send_wechat_alert')
    @patch('scheduled_total_control._run_child')
    def test_run_total_control_v2_pre_refreshes_before_main_and_skips_post_shadow(
        self,
        mock_run_child,
        mock_send_wechat_alert,
    ):
        mock_run_child.side_effect = [
            _build_child_result('dws_v2_pre_refresh', 'SUCCESS', ['动作：已执行 M3 raw / DWD 刷新并写入 DWS v2 shadow 表']),
            _build_child_result('main', 'SUCCESS', ['结果：成功8 / 警告1 / 失败0']),
            _build_child_result('store_daily_topic', 'SUCCESS', ['动作：专题调度完成']),
        ]

        exit_code = run_total_control(cutover_mode='v2')

        self.assertEqual(exit_code, 0)
        self.assertEqual(mock_run_child.call_count, 3)
        called_chain_keys = [call.args[0].key for call in mock_run_child.call_args_list]
        self.assertEqual(called_chain_keys, ['dws_v2_pre_refresh', 'main_etl', 'store_daily_topic'])
        alert_content = mock_send_wechat_alert.call_args.args[1]
        self.assertIn('DWS v2 读源预刷新: SUCCESS', alert_content)
        self.assertIn('主链调度: SUCCESS', alert_content)
        self.assertIn('门店销售专题: SUCCESS', alert_content)
        self.assertIn('DWS v2 Shadow: SKIPPED', alert_content)
        self.assertIn('主链前完成 DWS v2 读源预刷新', alert_content)

    @patch('scheduled_total_control.send_wechat_alert')
    @patch('scheduled_total_control._run_child')
    def test_run_total_control_v2_blocks_when_pre_refresh_fails(
        self,
        mock_run_child,
        mock_send_wechat_alert,
    ):
        mock_run_child.return_value = _build_child_result(
            'dws_v2_pre_refresh',
            'FAILED',
            ['动作：shadow 调度失败'],
            exit_code=1,
        )

        exit_code = run_total_control(cutover_mode='v2')

        self.assertEqual(exit_code, 1)
        mock_run_child.assert_called_once()
        alert_content = mock_send_wechat_alert.call_args.args[1]
        self.assertIn('DWS v2 读源预刷新: FAILED', alert_content)
        self.assertIn('主链调度: SKIPPED', alert_content)
        self.assertIn('V2 读源预刷新失败', alert_content)

    @patch('scheduled_total_control.send_wechat_alert')
    @patch('scheduled_total_control._run_child')
    def test_run_total_control_v2_allows_python_cleanup_exit_120_when_pre_refresh_summary_succeeds(
        self,
        mock_run_child,
        mock_send_wechat_alert,
    ):
        pre_refresh_result = _build_child_result(
            'dws_v2_pre_refresh',
            'WARNING',
            ['动作：已执行 M3 raw / DWD 刷新并写入 DWS v2 shadow 表'],
            issue_lines=['- 子链业务摘要已成功，但 Python 在进程清理/标准流刷新阶段返回退出码 120；总控按告警继续执行'],
            exit_code=120,
        )
        pre_refresh_result.summary['python_cleanup_exit_downgraded'] = True
        mock_run_child.side_effect = [
            pre_refresh_result,
            _build_child_result('main', 'SUCCESS', ['结果：成功8 / 警告0 / 失败0']),
            _build_child_result('store_daily_topic', 'SUCCESS', ['动作：专题调度完成']),
        ]

        exit_code = run_total_control(cutover_mode='v2')

        self.assertEqual(exit_code, 0)
        self.assertEqual(mock_run_child.call_count, 3)
        called_chain_keys = [call.args[0].key for call in mock_run_child.call_args_list]
        self.assertEqual(called_chain_keys, ['dws_v2_pre_refresh', 'main_etl', 'store_daily_topic'])
        alert_content = mock_send_wechat_alert.call_args.args[1]
        self.assertIn('存在告警', alert_content)
        self.assertIn('DWS v2 读源预刷新: WARNING', alert_content)
        self.assertIn('标准流刷新阶段返回退出码 120', alert_content)
        self.assertIn('主链调度: SUCCESS', alert_content)
        self.assertIn('门店销售专题: SUCCESS', alert_content)

    def test_windows_total_control_wrapper_forwards_arguments(self):
        wrapper = Path(__file__).with_name('run_scheduled_total_control.bat').read_text(encoding='utf-8')

        self.assertIn('scheduled_total_control.py %*', wrapper)

    def test_windows_v2_total_control_wrapper_pins_v2_and_forwards_arguments(self):
        wrapper = Path(__file__).with_name('run_scheduled_total_control_v2.bat').read_text(encoding='utf-8')

        self.assertIn('scheduled_total_control.py --cutover-mode v2 %*', wrapper)

    def test_build_python_command_forwards_topic_report_date_mode_only_to_topic_chain(self):
        topic_command = _build_python_command(
            CHAIN_DEFINITIONS['store_daily_topic'],
            conn_test=False,
            cutover_mode='v2',
            rollback_to_legacy=False,
            topic_report_date_mode='current-day',
        )
        main_command = _build_python_command(
            CHAIN_DEFINITIONS['main'],
            conn_test=False,
            cutover_mode='v2',
            rollback_to_legacy=False,
            topic_report_date_mode='current-day',
        )

        self.assertIn('--auto-report-date-mode', topic_command)
        self.assertIn('current-day', topic_command)
        self.assertNotIn('--auto-report-date-mode', main_command)

    def test_build_parser_accepts_topic_report_date_mode(self):
        args = build_parser().parse_args(['--topic-report-date-mode', 'current-day'])

        self.assertEqual(args.topic_report_date_mode, 'current-day')


if __name__ == '__main__':
    unittest.main()