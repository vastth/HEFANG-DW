# -*- coding: utf-8 -*-
"""
ODS同步入口（支持增量/全量）
"""

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime

import etl_ods_fa_storage
import etl_ods_m_retail
import etl_ods_m_retailitem

def _setup_logger():
    logger = logging.getLogger(__name__)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    log_dir = os.path.join(os.path.dirname(__file__), "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"ods_qc_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info(f"质量校验日志写入: {log_file}")
    return logger


def _run_quality_checks(logger, qc_days, qc_all, qc_start_date, qc_end_date, as_of=None):
    python_exec = sys.executable
    base_dir = os.path.dirname(__file__)
    tools_dir = os.path.join(base_dir, "tools")

    incremental_tool = os.path.join(tools_dir, "check_ods_incremental.py")
    retailitem_tool = os.path.join(tools_dir, "check_ods_retailitem_quality.py")

    incremental_cmd = [python_exec, incremental_tool, "--days", str(qc_days)]
    if qc_end_date:
        incremental_cmd.extend(["--end-date", str(qc_end_date)])
    if as_of:
        incremental_cmd.extend(["--as-of", as_of])

    retailitem_cmd = [python_exec, retailitem_tool]
    if qc_all:
        retailitem_cmd.append("--all")
    else:
        retailitem_cmd.extend(["--days", str(qc_days)])
        if qc_start_date and qc_end_date:
            retailitem_cmd.extend(["--start-date", str(qc_start_date), "--end-date", str(qc_end_date)])
    if as_of:
        retailitem_cmd.extend(["--as-of", as_of])

    logger.info("开始执行质量校验: ods_m_retail / ods_m_retailitem")

    for label, cmd in (
        ("check_ods_incremental", incremental_cmd),
        ("check_ods_retailitem_quality", retailitem_cmd),
    ):
        logger.info(f"运行工具: {label}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.stdout:
            logger.info("\n" + result.stdout.strip())
        if result.stderr:
            logger.error("\n" + result.stderr.strip())
        if result.returncode != 0:
            raise RuntimeError(f"{label} 执行失败，返回码 {result.returncode}")


def run(mode="incremental", backfill_days=7, window_days=None, run_qc=True,
        qc_days=7, qc_all=False, qc_start_date=None, qc_end_date=None):
    start_time = datetime.now()
    logger = _setup_logger()
    logger.info("=" * 50)
    logger.info(f"开始执行 ODS 同步（mode={mode}）")
    logger.info("=" * 50)

    try:
        etl_ods_fa_storage.run()
        etl_ods_m_retail.run(mode=mode, backfill_days=backfill_days, window_days=window_days)
        etl_ods_m_retailitem.run(mode=mode, backfill_days=backfill_days, window_days=window_days)

        end_time = datetime.now()
        duration = (end_time - start_time).seconds
        logger.info("=" * 50)
        logger.info(f"✓ ODS执行成功！耗时 {duration} 秒")
        logger.info("=" * 50)

        if run_qc:
            as_of = end_time.strftime("%Y-%m-%d %H:%M:%S")
            _run_quality_checks(logger, qc_days, qc_all, qc_start_date, qc_end_date, as_of=as_of)
        return True

    except Exception as e:
        logger.error(f"✗ ODS执行失败: {str(e)}")
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="ODS sync runner")
    parser.add_argument("--full", action="store_true", help="full reload for retail tables")
    parser.add_argument("--backfill-days", type=int, default=7, help="backfill days for incremental")
    parser.add_argument("--window-days", type=int, default=None, help="window size in days for Oracle reads")
    parser.add_argument("--skip-qc", action="store_true", help="skip quality checks")
    parser.add_argument("--qc-days", type=int, default=7, help="days back for quality checks")
    parser.add_argument("--qc-all", action="store_true", help="quality check on full dataset")
    parser.add_argument("--qc-start-date", type=int, default=None, help="qc start date YYYYMMDD")
    parser.add_argument("--qc-end-date", type=int, default=None, help="qc end date YYYYMMDD")
    args = parser.parse_args()

    run(
        mode="full" if args.full else "incremental",
        backfill_days=args.backfill_days,
        window_days=args.window_days,
        run_qc=not args.skip_qc,
        qc_days=args.qc_days,
        qc_all=args.qc_all,
        qc_start_date=args.qc_start_date,
        qc_end_date=args.qc_end_date,
    )
