# -*- coding: utf-8 -*-
"""从 NAS 云雀订单管理 Excel 生成并导入达播订单标签。"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict
import json
from datetime import datetime
from pathlib import Path
import sys
from typing import Any

from pymysql.cursors import DictCursor

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db_connections import connect_mysql
from tools.extract_dabo_order_candidates_from_nas import (
    DEFAULT_FILE_PATTERN,
    DEFAULT_NAS_DIR,
    DEFAULT_SHEET_NAME,
    build_order_id_token_key,
    extract_candidates,
)


TARGET_TABLE_NAME = "ads_dabo_order_label"
DDL_FILE_PATH = REPO_ROOT / "SQL" / "create_ads_dabo_order_label.sql"

NORMALIZATION_STATUS_EXACT_HIT = "exact_hit"
NORMALIZATION_STATUS_AUTO_ALIAS = "auto_alias"
NORMALIZATION_STATUS_NO_CANDIDATE = "unmatched_no_candidate"
NORMALIZATION_STATUS_AMBIGUOUS = "unmatched_ambiguous_candidate"

NORMALIZATION_RULE_EXACT = "exact_system_order_id"
NORMALIZATION_RULE_SUPERSET = "same_file_unique_token_superset"
NORMALIZATION_RULE_NO_ALIAS = "exact_miss_no_alias"

SCHEMA_COLUMNS = {
    "canonical_system_order_id": "ALTER TABLE ads_dabo_order_label ADD COLUMN canonical_system_order_id VARCHAR(512) NULL AFTER system_order_id",
    "normalization_status": "ALTER TABLE ads_dabo_order_label ADD COLUMN normalization_status VARCHAR(32) NOT NULL DEFAULT 'unreviewed' AFTER canonical_system_order_id",
    "normalization_rule": "ALTER TABLE ads_dabo_order_label ADD COLUMN normalization_rule VARCHAR(64) NULL AFTER normalization_status",
    "normalization_evidence": "ALTER TABLE ads_dabo_order_label ADD COLUMN normalization_evidence TEXT NULL AFTER normalization_rule",
}

SCHEMA_INDEXES = {
    "idx_ads_dabo_order_label_canonical_system_order": "ALTER TABLE ads_dabo_order_label ADD KEY idx_ads_dabo_order_label_canonical_system_order (canonical_system_order_id(255))",
}


def _connect():
    return connect_mysql(
        cursorclass=DictCursor,
        autocommit=False,
    )


def _resolve_input_file(explicit_file: str | None, nas_dir: Path, pattern: str) -> Path:
    if explicit_file:
        file_path = Path(explicit_file)
        if not file_path.is_absolute():
            file_path = REPO_ROOT / file_path
        if not file_path.exists() or not file_path.is_file():
            raise FileNotFoundError(f"未找到 Excel 文件: {file_path}")
        return file_path

    if not nas_dir.exists() or not nas_dir.is_dir():
        raise FileNotFoundError(f"未找到 NAS 目录: {nas_dir}")

    candidates = [
        path for path in nas_dir.glob(pattern)
        if path.is_file() and not path.name.startswith("~$")
    ]
    if not candidates:
        raise FileNotFoundError(f"目录 {nas_dir} 下未找到匹配文件: {pattern}")

    return max(candidates, key=lambda path: (path.stat().st_mtime, path.name))


def _resolve_output_path(path_value: str) -> Path:
    path = Path(path_value)
    if not path.is_absolute():
        path = REPO_ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _get_existing_columns(cursor) -> set[str]:
    cursor.execute(f"SHOW COLUMNS FROM {TARGET_TABLE_NAME}")
    return {row["Field"] for row in cursor.fetchall()}


def _get_existing_indexes(cursor) -> set[str]:
    cursor.execute(f"SHOW INDEX FROM {TARGET_TABLE_NAME}")
    return {row["Key_name"] for row in cursor.fetchall()}


def _ensure_target_table_schema(cursor) -> None:
    existing_columns = _get_existing_columns(cursor)
    for column_name, statement in SCHEMA_COLUMNS.items():
        if column_name not in existing_columns:
            cursor.execute(statement)

    existing_indexes = _get_existing_indexes(cursor)
    for index_name, statement in SCHEMA_INDEXES.items():
        if index_name not in existing_indexes:
            cursor.execute(statement)


def _ensure_target_table(cursor) -> None:
    if not DDL_FILE_PATH.exists():
        raise FileNotFoundError(f"未找到达播订单标签表建表脚本: {DDL_FILE_PATH}")

    ddl_sql = DDL_FILE_PATH.read_text(encoding="utf-8")
    statements = [statement.strip() for statement in ddl_sql.split(";") if statement.strip()]
    for statement in statements:
        cursor.execute(statement)
    _ensure_target_table_schema(cursor)


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[index:index + size] for index in range(0, len(items), size)]


def _fetch_exact_hit_order_ids(order_ids: list[str]) -> set[str]:
    distinct_order_ids = [order_id for order_id in dict.fromkeys(order_ids) if order_id]
    if not distinct_order_ids:
        return set()

    hits: set[str] = set()
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            for batch in _chunked(distinct_order_ids, 200):
                placeholders = ", ".join(["%s"] * len(batch))
                cursor.execute(
                    f"""
                    SELECT DISTINCT oms_sourcecode
                    FROM ods_m_retail
                    WHERE isactive = 'Y'
                      AND status = 2
                      AND oms_sourcecode IN ({placeholders})
                    """,
                    batch,
                )
                hits.update(
                    row["oms_sourcecode"]
                    for row in cursor.fetchall()
                    if row.get("oms_sourcecode")
                )
    finally:
        conn.close()

    return hits


def _ship_date_key(platform_ship_time: str | None) -> str | None:
    if not platform_ship_time:
        return None
    text_value = str(platform_ship_time).strip()
    if not text_value:
        return None
    try:
        return datetime.fromisoformat(text_value).strftime("%Y-%m-%d")
    except ValueError:
        return text_value[:10]


def _is_same_normalization_context(source_payload: dict[str, Any], candidate_payload: dict[str, Any]) -> bool:
    if source_payload["source_file"] != candidate_payload["source_file"]:
        return False
    if source_payload["dabo_channel_code"] != candidate_payload["dabo_channel_code"]:
        return False
    if source_payload["influencer_name"] != candidate_payload["influencer_name"]:
        return False
    if _ship_date_key(source_payload.get("platform_ship_time")) != _ship_date_key(candidate_payload.get("platform_ship_time")):
        return False

    source_influencer_id = source_payload.get("influencer_id") or ""
    candidate_influencer_id = candidate_payload.get("influencer_id") or ""
    if source_influencer_id and candidate_influencer_id and source_influencer_id != candidate_influencer_id:
        return False

    return True


def _is_token_superset(candidate_token_key: tuple[str, ...], source_token_key: tuple[str, ...]) -> bool:
    return set(source_token_key).issubset(set(candidate_token_key))


def _build_normalization_evidence(
    source_payload: dict[str, Any],
    token_key: tuple[str, ...],
    candidate_payloads: list[dict[str, Any]],
    selected_candidate: dict[str, Any] | None,
) -> str:
    evidence = {
        "normalized_tokens": list(token_key),
        "candidate_count": len(candidate_payloads),
        "source_platform_ship_date": _ship_date_key(source_payload.get("platform_ship_time")),
        "candidate_system_order_ids": [payload["system_order_id"] for payload in candidate_payloads[:5]],
    }
    if selected_candidate is not None:
        evidence["selected_candidate_system_order_id"] = selected_candidate["system_order_id"]
        evidence["selected_candidate_first_source_row_number"] = selected_candidate["first_source_row_number"]
    return json.dumps(evidence, ensure_ascii=False, sort_keys=True)


def _normalize_order_labels(order_labels: list[Any], preview_limit: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    normalized_payloads = [asdict(row) for row in order_labels]
    exact_hit_order_ids = _fetch_exact_hit_order_ids([payload["system_order_id"] for payload in normalized_payloads])

    for payload in normalized_payloads:
        payload["_token_key"] = build_order_id_token_key(payload["system_order_id"])

    exact_hit_payloads = [
        payload for payload in normalized_payloads
        if payload["system_order_id"] in exact_hit_order_ids
    ]

    status_counter: Counter[str] = Counter()
    normalization_preview: list[dict[str, Any]] = []
    unresolved_preview: list[dict[str, Any]] = []

    for payload in normalized_payloads:
        token_key = payload["_token_key"]
        candidate_payloads: list[dict[str, Any]] = []
        selected_candidate: dict[str, Any] | None = None

        payload["canonical_system_order_id"] = None
        payload["normalization_status"] = NORMALIZATION_STATUS_NO_CANDIDATE
        payload["normalization_rule"] = NORMALIZATION_RULE_NO_ALIAS
        payload["normalization_evidence"] = None

        if payload["system_order_id"] in exact_hit_order_ids:
            payload["canonical_system_order_id"] = payload["system_order_id"]
            payload["normalization_status"] = NORMALIZATION_STATUS_EXACT_HIT
            payload["normalization_rule"] = NORMALIZATION_RULE_EXACT
        elif "," in payload["system_order_id"] and len(token_key) >= 2:
            candidate_payloads = [
                candidate for candidate in exact_hit_payloads
                if candidate["system_order_id"] != payload["system_order_id"]
                and _is_same_normalization_context(payload, candidate)
                and _is_token_superset(candidate["_token_key"], token_key)
            ]

            if len(candidate_payloads) == 1:
                selected_candidate = candidate_payloads[0]
                payload["canonical_system_order_id"] = selected_candidate["system_order_id"]
                payload["normalization_status"] = NORMALIZATION_STATUS_AUTO_ALIAS
                payload["normalization_rule"] = NORMALIZATION_RULE_SUPERSET
            elif len(candidate_payloads) > 1:
                payload["normalization_status"] = NORMALIZATION_STATUS_AMBIGUOUS

        if payload["normalization_status"] != NORMALIZATION_STATUS_EXACT_HIT:
            payload["normalization_evidence"] = _build_normalization_evidence(
                source_payload=payload,
                token_key=token_key,
                candidate_payloads=candidate_payloads,
                selected_candidate=selected_candidate,
            )

        status_counter.update([payload["normalization_status"]])

        if payload["normalization_status"] == NORMALIZATION_STATUS_AUTO_ALIAS:
            normalization_preview.append(
                {
                    "first_source_row_number": payload["first_source_row_number"],
                    "system_order_id": payload["system_order_id"],
                    "canonical_system_order_id": payload["canonical_system_order_id"],
                    "normalization_rule": payload["normalization_rule"],
                }
            )
        elif payload["normalization_status"] != NORMALIZATION_STATUS_EXACT_HIT:
            unresolved_preview.append(
                {
                    "first_source_row_number": payload["first_source_row_number"],
                    "system_order_id": payload["system_order_id"],
                    "normalization_status": payload["normalization_status"],
                    "normalization_rule": payload["normalization_rule"],
                }
            )

    normalization_summary = {
        "normalization_status_distribution": dict(status_counter.most_common()),
        "normalization_auto_alias_count": status_counter.get(NORMALIZATION_STATUS_AUTO_ALIAS, 0),
        "normalization_unresolved_count": status_counter.get(NORMALIZATION_STATUS_NO_CANDIDATE, 0) + status_counter.get(NORMALIZATION_STATUS_AMBIGUOUS, 0),
    }
    if preview_limit > 0:
        normalization_summary["preview_normalization_actions"] = normalization_preview[:preview_limit]
        normalization_summary["preview_unresolved_normalization"] = unresolved_preview[:preview_limit]

    for payload in normalized_payloads:
        payload.pop("_token_key", None)

    return normalized_payloads, normalization_summary


def _apply_labels(source_file: str, order_labels: list[dict]) -> tuple[int, int]:
    deleted_rows = 0
    inserted_rows = 0
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            _ensure_target_table(cursor)
            deleted_rows = cursor.execute(
                f"DELETE FROM {TARGET_TABLE_NAME} WHERE source_file = %s",
                (source_file,),
            )

            if order_labels:
                inserted_rows = cursor.executemany(
                    f"""
                    INSERT INTO {TARGET_TABLE_NAME} (
                        source_file,
                        source_sheet,
                        source_file_mtime,
                        first_source_row_number,
                        source_row_count,
                        system_order_id,
                        canonical_system_order_id,
                        platform_order_id,
                        is_dabo_order,
                        dabo_source,
                        dabo_channel_code,
                        dabo_channel_name,
                        influencer_id,
                        influencer_name,
                        order_status,
                        platform_ship_time,
                        normalization_status,
                        normalization_rule,
                        normalization_evidence
                    ) VALUES (
                        %(source_file)s,
                        %(source_sheet)s,
                        %(source_file_mtime)s,
                        %(first_source_row_number)s,
                        %(source_row_count)s,
                        %(system_order_id)s,
                        %(canonical_system_order_id)s,
                        %(platform_order_id)s,
                        %(is_dabo_order)s,
                        %(dabo_source)s,
                        %(dabo_channel_code)s,
                        %(dabo_channel_name)s,
                        %(influencer_id)s,
                        %(influencer_name)s,
                        %(order_status)s,
                        %(platform_ship_time)s,
                        %(normalization_status)s,
                        %(normalization_rule)s,
                        %(normalization_evidence)s
                    )
                    """,
                    order_labels,
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return deleted_rows, inserted_rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="从 NAS 云雀订单管理 Excel 生成并导入达播订单标签")
    parser.add_argument("--file", help="指定 Excel 文件路径；不传时默认扫描 NAS 最新订单管理*.xlsx")
    parser.add_argument("--nas-dir", default=str(DEFAULT_NAS_DIR), help="NAS 目录，默认读取云雀达播订单筛选目录")
    parser.add_argument("--pattern", default=DEFAULT_FILE_PATTERN, help="Excel 文件匹配模式")
    parser.add_argument("--sheet", default=DEFAULT_SHEET_NAME, help="工作表名称")
    parser.add_argument("--preview-limit", type=int, default=5, help="输出预览行数，默认 5")
    parser.add_argument("--report-json", help="将 dry-run / apply 摘要写入指定 JSON 路径")
    parser.add_argument("--apply", action="store_true", help="显式启用写库模式；默认仅 dry-run")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    file_path = _resolve_input_file(
        explicit_file=args.file,
        nas_dir=Path(args.nas_dir),
        pattern=args.pattern,
    )
    summary, _, order_labels = extract_candidates(
        file_path=file_path,
        sheet_name=args.sheet,
        preview_limit=max(args.preview_limit, 0),
    )
    normalized_order_labels, normalization_summary = _normalize_order_labels(
        order_labels=order_labels,
        preview_limit=max(args.preview_limit, 0),
    )
    summary.update(normalization_summary)
    if args.preview_limit > 0:
        summary["preview_order_labels"] = normalized_order_labels[:max(args.preview_limit, 0)]

    summary["mode"] = "apply" if args.apply else "dry-run"
    summary["target_table"] = TARGET_TABLE_NAME
    summary["records_deleted_before_insert"] = 0
    summary["records_inserted"] = 0

    if args.apply:
        deleted_rows, inserted_rows = _apply_labels(file_path.name, normalized_order_labels)
        summary["records_deleted_before_insert"] = deleted_rows
        summary["records_inserted"] = inserted_rows

    if args.report_json:
        output_path = _resolve_output_path(args.report_json)
        _write_json(output_path, summary)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())