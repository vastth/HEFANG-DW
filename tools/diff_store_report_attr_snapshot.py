# -*- coding: utf-8 -*-
"""只读比对 NAS 门店日报完整快照与当前有效门店属性，输出四类差异清单。"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.import_cfg_store_target_daily_from_nas import (
    DEFAULT_SHEET_NAME,
    STORE_ATTR_TABLE_NAME,
    STORE_TABLE_NAME,
    _build_store_attr_rows,
    _compute_file_md5,
    _connect,
    _derive_report_channel_type_group,
    _load_latest_store_attr,
    _load_store_mapping,
    _normalize_store_key,
    _normalize_text,
    _parse_target_month_arg,
    _parse_workbook,
    _resolve_input_file,
)


COMPARE_FIELDS = ("store_code", "store_name", "report_channel_type")


def _resolve_output_path(path_value: str) -> Path:
    path = Path(path_value)
    if not path.is_absolute():
        path = REPO_ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _ensure_required_tables(conn: Any) -> None:
    required_tables = [STORE_TABLE_NAME, STORE_ATTR_TABLE_NAME]
    placeholders = ", ".join(["%s"] * len(required_tables))
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT table_name AS table_name_alias
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
              AND table_name IN ({placeholders})
            """,
            required_tables,
        )
        existing = {dict(row)["table_name_alias"] for row in cursor.fetchall()}

    missing_tables = [table_name for table_name in required_tables if table_name not in existing]
    if missing_tables:
        raise RuntimeError(f"缺少依赖表: {', '.join(missing_tables)}")


def _serialize_current_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "store_id": int(row["store_id"]),
        "store_code": row["store_code"],
        "store_name": row["store_name"],
        "report_channel_type": row["report_channel_type"],
        "report_channel_type_group": row.get("report_channel_type_group")
        or _derive_report_channel_type_group(row.get("report_channel_type"), allow_unknown=True),
        "store_grade": row.get("store_grade"),
        "is_duty_free": row.get("is_duty_free"),
        "is_include_in_daily_report": row.get("is_include_in_daily_report"),
        "remark": row.get("remark"),
        "effective_start_date": row["effective_start_date"].isoformat(),
        "effective_end_date": row["effective_end_date"].isoformat(),
    }


def _serialize_candidate_row(row: Any) -> dict[str, Any]:
    return {
        "store_id": row.store_id,
        "store_code": row.store_code,
        "store_name": row.store_name,
        "report_channel_type": row.report_channel_type,
        "report_channel_type_group": row.report_channel_type_group,
        "store_grade": row.store_grade,
        "is_duty_free": row.is_duty_free,
        "is_include_in_daily_report": row.is_include_in_daily_report,
        "remark": row.remark,
        "effective_start_date": row.effective_start_date.isoformat(),
        "effective_end_date": row.effective_end_date.isoformat(),
    }


def _load_current_effective_store_attr(
    conn: Any,
    compare_date: date,
) -> tuple[list[dict[str, Any]], dict[int, dict[str, Any]], list[dict[str, Any]]]:
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT
                store_id AS store_id_alias,
                store_code AS store_code_alias,
                store_name AS store_name_alias,
                report_channel_type AS report_channel_type_alias,
                store_grade AS store_grade_alias,
                is_duty_free AS is_duty_free_alias,
                is_include_in_daily_report AS is_include_in_daily_report_alias,
                remark AS remark_alias,
                effective_start_date AS effective_start_date_alias,
                effective_end_date AS effective_end_date_alias
            FROM {STORE_ATTR_TABLE_NAME}
            WHERE is_include_in_daily_report = 'Y'
              AND %s BETWEEN effective_start_date AND effective_end_date
            ORDER BY store_id, effective_start_date
            """,
            (compare_date,),
        )
        rows = [dict(row) for row in cursor.fetchall()]

    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        current_row = {
            "store_id": int(row["store_id_alias"]),
            "store_code": row["store_code_alias"],
            "store_name": row["store_name_alias"],
            "report_channel_type": row["report_channel_type_alias"],
            "report_channel_type_group": _derive_report_channel_type_group(
                row["report_channel_type_alias"], allow_unknown=True
            ),
            "store_grade": row["store_grade_alias"],
            "is_duty_free": row["is_duty_free_alias"],
            "is_include_in_daily_report": row["is_include_in_daily_report_alias"],
            "remark": row["remark_alias"],
            "effective_start_date": row["effective_start_date_alias"],
            "effective_end_date": row["effective_end_date_alias"],
        }
        grouped.setdefault(current_row["store_id"], []).append(current_row)

    current_rows: list[dict[str, Any]] = []
    overlap_rows: list[dict[str, Any]] = []
    current_map: dict[int, dict[str, Any]] = {}
    for store_id, matched_rows in grouped.items():
        if len(matched_rows) > 1:
            overlap_rows.extend(matched_rows)
            continue
        current_rows.append(matched_rows[0])
        current_map[store_id] = matched_rows[0]

    return current_rows, current_map, overlap_rows


def _collect_mapping_issues(
    parsed_rows: list[Any],
    store_map: dict[str, dict[str, Any]],
    ambiguous_store_names: list[dict[str, Any]],
) -> tuple[list[str], list[dict[str, Any]]]:
    ambiguous_keys = {item["store_name"]: item["matched_store_ids"] for item in ambiguous_store_names}
    missing_store_names: list[str] = []
    ambiguous_matches: list[dict[str, Any]] = []

    for parsed_row in parsed_rows:
        normalized_name = _normalize_store_key(parsed_row.store_name)
        if normalized_name in ambiguous_keys:
            ambiguous_matches.append(
                {
                    "store_name": parsed_row.store_name,
                    "matched_store_ids": ambiguous_keys[normalized_name],
                }
            )
            continue
        if normalized_name not in store_map:
            missing_store_names.append(parsed_row.store_name)

    dedup_missing = sorted(set(missing_store_names))
    dedup_ambiguous = sorted(
        {item["store_name"]: item for item in ambiguous_matches}.values(),
        key=lambda item: item["store_name"],
    )
    return dedup_missing, dedup_ambiguous


def _build_candidate_map(store_attr_rows: list[Any]) -> tuple[dict[int, Any], list[dict[str, Any]]]:
    candidate_map: dict[int, Any] = {}
    duplicated_store_ids: list[dict[str, Any]] = []
    for row in store_attr_rows:
        if row.store_id in candidate_map:
            duplicated_store_ids.append(
                {
                    "store_id": row.store_id,
                    "first_store_name": candidate_map[row.store_id].store_name,
                    "duplicate_store_name": row.store_name,
                }
            )
            continue
        candidate_map[row.store_id] = row
    return candidate_map, duplicated_store_ids


def _detect_changed_fields(current_row: dict[str, Any], candidate_row: Any) -> list[str]:
    changed_fields: list[str] = []
    for field_name in COMPARE_FIELDS:
        current_value = _normalize_text(current_row.get(field_name))
        candidate_value = _normalize_text(getattr(candidate_row, field_name))
        if current_value != candidate_value:
            changed_fields.append(field_name)
    return changed_fields


def _classify_rows(current_map: dict[int, dict[str, Any]], candidate_map: dict[int, Any]) -> dict[str, list[dict[str, Any]]]:
    unchanged_rows: list[dict[str, Any]] = []
    changed_rows: list[dict[str, Any]] = []
    new_rows: list[dict[str, Any]] = []
    exited_rows: list[dict[str, Any]] = []

    all_store_ids = sorted(set(current_map) | set(candidate_map))
    for store_id in all_store_ids:
        current_row = current_map.get(store_id)
        candidate_row = candidate_map.get(store_id)
        if current_row and candidate_row:
            changed_fields = _detect_changed_fields(current_row, candidate_row)
            diff_payload = {
                "store_id": store_id,
                "changed_fields": changed_fields,
                "current": _serialize_current_row(current_row),
                "candidate": _serialize_candidate_row(candidate_row),
            }
            if changed_fields:
                changed_rows.append(diff_payload)
            else:
                unchanged_rows.append(diff_payload)
            continue

        if candidate_row:
            new_rows.append(
                {
                    "store_id": store_id,
                    "candidate": _serialize_candidate_row(candidate_row),
                }
            )
            continue

        if current_row:
            exited_rows.append(
                {
                    "store_id": store_id,
                    "current": _serialize_current_row(current_row),
                }
            )

    return {
        "unchanged_rows": unchanged_rows,
        "changed_rows": changed_rows,
        "new_rows": new_rows,
        "exited_rows": exited_rows,
    }


def _build_payload(
    file_path: Path,
    file_md5: str,
    sheet_name: str,
    workbook_summary: dict[str, Any],
    compare_date: date,
    current_rows: list[dict[str, Any]],
    overlap_rows: list[dict[str, Any]],
    store_attr_rows: list[Any],
    missing_store_names: list[str],
    ambiguous_store_names: list[dict[str, Any]],
    duplicated_store_ids: list[dict[str, Any]],
    classified_rows: dict[str, list[dict[str, Any]]],
) -> tuple[dict[str, Any], str | None]:
    current_type_counts = Counter(row["report_channel_type"] for row in current_rows)
    candidate_type_counts = Counter(row.report_channel_type for row in store_attr_rows)
    current_group_counts = Counter(row["report_channel_type_group"] for row in current_rows)
    candidate_group_counts = Counter(row.report_channel_type_group for row in store_attr_rows)

    payload: dict[str, Any] = {
        "mode": "diff-only",
        "file_name": file_path.name,
        "file_path": str(file_path),
        "file_md5": file_md5,
        "sheet_name": sheet_name,
        "target_month": workbook_summary["target_month"],
        "target_version": workbook_summary["target_version"],
        "compare_date": compare_date.isoformat(),
        "source_row_count": workbook_summary["source_row_count"],
        "available_target_months": workbook_summary["available_target_months"],
        "current_effective_row_count": len(current_rows),
        "current_overlap_row_count": len(overlap_rows),
        "candidate_row_count": len(store_attr_rows),
        "missing_store_names": missing_store_names,
        "ambiguous_store_names": ambiguous_store_names,
        "candidate_duplicate_store_ids": duplicated_store_ids,
        "current_type_counts": dict(current_type_counts),
        "candidate_type_counts": dict(candidate_type_counts),
        "current_group_counts": dict(current_group_counts),
        "candidate_group_counts": dict(candidate_group_counts),
        "diff_counts": {
            "unchanged": len(classified_rows["unchanged_rows"]),
            "changed": len(classified_rows["changed_rows"]),
            "new": len(classified_rows["new_rows"]),
            "exited": len(classified_rows["exited_rows"]),
        },
        "proposed_action_counts": {
            "no_action": len(classified_rows["unchanged_rows"]),
            "close_and_open": len(classified_rows["changed_rows"]),
            "open_only": len(classified_rows["new_rows"]),
            "close_only": len(classified_rows["exited_rows"]),
        },
        "current_overlap_rows": [_serialize_current_row(row) for row in overlap_rows],
        "unchanged_rows": classified_rows["unchanged_rows"],
        "changed_rows": classified_rows["changed_rows"],
        "new_rows": classified_rows["new_rows"],
        "exited_rows": classified_rows["exited_rows"],
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    error_message: str | None = None
    if overlap_rows:
        error_message = f"{STORE_ATTR_TABLE_NAME} 在 compare_date 下已存在重叠有效记录，需先清理当前基线"
    elif missing_store_names:
        error_message = f"候选快照中存在未映射门店: {missing_store_names}"
    elif ambiguous_store_names:
        error_message = f"候选快照中存在门店名称歧义: {[item['store_name'] for item in ambiguous_store_names]}"
    elif duplicated_store_ids:
        error_message = f"候选快照中存在重复 store_id: {[item['store_id'] for item in duplicated_store_ids]}"

    return payload, error_message


def _build_printable_summary(payload: dict[str, Any], error_message: str | None, preview_limit: int) -> dict[str, Any]:
    printable = {
        key: value
        for key, value in payload.items()
        if key
        not in {
            "current_overlap_rows",
            "unchanged_rows",
            "changed_rows",
            "new_rows",
            "exited_rows",
        }
    }
    printable["validation_status"] = "FAILED" if error_message else "PASSED"
    if error_message:
        printable["error_message"] = error_message
    if preview_limit > 0:
        printable["current_overlap_preview"] = payload["current_overlap_rows"][:preview_limit]
        printable["unchanged_preview"] = payload["unchanged_rows"][:preview_limit]
        printable["changed_preview"] = payload["changed_rows"][:preview_limit]
        printable["new_preview"] = payload["new_rows"][:preview_limit]
        printable["exited_preview"] = payload["exited_rows"][:preview_limit]
    return printable


def _write_output_json(path_value: str, payload: dict[str, Any]) -> None:
    output_path = _resolve_output_path(path_value)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def build_store_attr_snapshot_diff(
    target_month: str,
    file_path_arg: str | None = None,
    sheet_name: str = DEFAULT_SHEET_NAME,
    compare_date: date | None = None,
    preview_limit: int = 10,
) -> tuple[dict[str, Any], dict[str, Any], str | None]:
    file_path = _resolve_input_file(file_path_arg, target_month)
    file_md5 = _compute_file_md5(file_path)
    parsed_rows, workbook_summary = _parse_workbook(
        file_path,
        sheet_name,
        require_store_type=True,
        target_month_filter=target_month,
    )
    target_month_start = datetime.strptime(workbook_summary["target_month"], "%Y-%m").date().replace(day=1)
    resolved_compare_date = compare_date or target_month_start

    conn = _connect()
    try:
        _ensure_required_tables(conn)
        store_map, ambiguous_store_names = _load_store_mapping(conn)
        missing_store_names, ambiguous_matches = _collect_mapping_issues(parsed_rows, store_map, ambiguous_store_names)
        latest_store_attr_map = _load_latest_store_attr(conn)
        current_rows, current_map, overlap_rows = _load_current_effective_store_attr(conn, resolved_compare_date)
    finally:
        conn.close()

    store_attr_rows = _build_store_attr_rows(parsed_rows, store_map, latest_store_attr_map, target_month_start)
    candidate_map, duplicated_store_ids = _build_candidate_map(store_attr_rows)
    classified_rows = _classify_rows(current_map, candidate_map)
    payload, error_message = _build_payload(
        file_path=file_path,
        file_md5=file_md5,
        sheet_name=sheet_name,
        workbook_summary=workbook_summary,
        compare_date=resolved_compare_date,
        current_rows=current_rows,
        overlap_rows=overlap_rows,
        store_attr_rows=store_attr_rows,
        missing_store_names=missing_store_names,
        ambiguous_store_names=ambiguous_matches,
        duplicated_store_ids=duplicated_store_ids,
        classified_rows=classified_rows,
    )
    printable = _build_printable_summary(payload, error_message, max(preview_limit, 0))
    return payload, printable, error_message


def main() -> int:
    parser = argparse.ArgumentParser(description="只读比对门店日报完整快照与当前有效门店属性")
    parser.add_argument(
        "--file-path",
        default=None,
        help="可选：显式指定目标 Excel 路径；不传时按 NAS 目录与 --target-month 自动解析文件",
    )
    parser.add_argument("--sheet-name", default=DEFAULT_SHEET_NAME, help="导入模板工作表名称")
    parser.add_argument(
        "--target-month",
        required=True,
        type=_parse_target_month_arg,
        help="显式指定需比对的目标月份，格式 YYYY-MM",
    )
    parser.add_argument(
        "--compare-date",
        type=date.fromisoformat,
        help="可选：用于读取当前有效 dim_store_report_attr 的比对日期，默认取目标月首日",
    )
    parser.add_argument("--preview-limit", type=int, default=10, help="控制台输出预览条数")
    parser.add_argument("--output-json", help="可选：将完整差异清单写入 JSON 文件")
    args = parser.parse_args()

    payload, printable, error_message = build_store_attr_snapshot_diff(
        target_month=args.target_month,
        file_path_arg=args.file_path,
        sheet_name=args.sheet_name,
        compare_date=args.compare_date,
        preview_limit=args.preview_limit,
    )

    if args.output_json:
        _write_output_json(args.output_json, payload)
    print(json.dumps(printable, ensure_ascii=False, indent=2))
    return 1 if error_message else 0


if __name__ == "__main__":
    raise SystemExit(main())