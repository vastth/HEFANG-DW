# -*- coding: utf-8 -*-
"""登记门店属性 NAS 快照与现网差异摘要。"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.diff_store_report_attr_snapshot import build_store_attr_snapshot_diff
from tools.import_cfg_store_target_daily_from_nas import DEFAULT_SHEET_NAME, _parse_target_month_arg


DEFAULT_REGISTRY_PATH = REPO_ROOT / "reports" / "store_attr_snapshot_registry.json"


def _resolve_path(path_value: str | None, default_path: Path) -> Path:
    if not path_value:
        path = default_path
    else:
        path = Path(path_value)
        if not path.is_absolute():
            path = REPO_ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _resolve_diff_output_path(path_value: str | None, target_month: str) -> Path:
    default_name = f"store_attr_snapshot_diff_{target_month.replace('-', '')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    return _resolve_path(path_value, REPO_ROOT / "reports" / default_name)


def _relative_to_repo(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return str(path)


def _load_registry(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "updated_at": None,
            "entries": [],
        }

    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"快照登记文件格式非法: {path}")
    entries = data.get("entries")
    if not isinstance(entries, list):
        raise ValueError(f"快照登记文件缺少 entries 数组: {path}")
    return data


def _write_registry(path: Path, registry: dict[str, Any]) -> None:
    path.write_text(json.dumps(registry, ensure_ascii=False, indent=2), encoding="utf-8")


def _count_detail_type_only_changes(payload: dict[str, Any]) -> int:
    count = 0
    for row in payload.get("changed_rows", []):
        if row.get("changed_fields") != ["report_channel_type"]:
            continue
        current_group = row.get("current", {}).get("report_channel_type_group")
        candidate_group = row.get("candidate", {}).get("report_channel_type_group")
        if current_group == candidate_group:
            count += 1
    return count


def _derive_status(payload: dict[str, Any], error_message: str | None) -> str:
    if error_message:
        return "validation_failed"

    diff_counts = payload["diff_counts"]
    if diff_counts["changed"] == 0 and diff_counts["new"] == 0 and diff_counts["exited"] == 0:
        return "aligned"
    return "pending_apply"


def _build_registry_entry(
    payload: dict[str, Any],
    error_message: str | None,
    diff_output_path: Path,
) -> dict[str, Any]:
    registered_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    detail_type_only_change_count = _count_detail_type_only_changes(payload)
    changed_rows = payload["diff_counts"]["changed"]
    group_counts_aligned = payload["current_group_counts"] == payload["candidate_group_counts"]

    return {
        "registered_at": registered_at,
        "snapshot_key": f"{payload['target_month']}::{payload['compare_date']}::{payload['file_md5']}",
        "status": _derive_status(payload, error_message),
        "validation_status": "FAILED" if error_message else "PASSED",
        "error_message": error_message,
        "target_month": payload["target_month"],
        "target_version": payload["target_version"],
        "compare_date": payload["compare_date"],
        "file_name": payload["file_name"],
        "file_path": payload["file_path"],
        "file_md5": payload["file_md5"],
        "source_row_count": payload["source_row_count"],
        "current_effective_row_count": payload["current_effective_row_count"],
        "candidate_row_count": payload["candidate_row_count"],
        "diff_counts": payload["diff_counts"],
        "proposed_action_counts": payload["proposed_action_counts"],
        "current_type_counts": payload["current_type_counts"],
        "candidate_type_counts": payload["candidate_type_counts"],
        "current_group_counts": payload["current_group_counts"],
        "candidate_group_counts": payload["candidate_group_counts"],
        "group_counts_aligned": group_counts_aligned,
        "detail_type_only_change_count": detail_type_only_change_count,
        "non_detail_only_change_count": changed_rows - detail_type_only_change_count,
        "diff_output_path": _relative_to_repo(diff_output_path),
    }


def _upsert_registry_entry(registry: dict[str, Any], entry: dict[str, Any]) -> tuple[dict[str, Any], str]:
    entries = registry.setdefault("entries", [])
    matching_index = None
    for index, existing_entry in enumerate(entries):
        if not isinstance(existing_entry, dict):
            continue
        if existing_entry.get("snapshot_key") != entry["snapshot_key"]:
            continue
        if existing_entry.get("status") != entry["status"]:
            continue
        if existing_entry.get("diff_counts") != entry["diff_counts"]:
            continue
        if existing_entry.get("file_md5") != entry["file_md5"]:
            continue
        matching_index = index
        break

    action = "inserted"
    if matching_index is None:
        entries.insert(0, entry)
    else:
        action = "updated"
        entries[matching_index] = entry
    registry["updated_at"] = entry["registered_at"]
    return registry, action


def main() -> int:
    parser = argparse.ArgumentParser(description="登记门店属性快照与现网差异摘要")
    parser.add_argument(
        "--target-month",
        required=True,
        type=_parse_target_month_arg,
        help="显式指定需登记的目标月份，格式 YYYY-MM",
    )
    parser.add_argument("--file-path", help="可选：显式指定目标 Excel 路径")
    parser.add_argument("--sheet-name", default=DEFAULT_SHEET_NAME, help="导入模板工作表名称")
    parser.add_argument(
        "--compare-date",
        type=date.fromisoformat,
        help="可选：用于读取当前有效 dim_store_report_attr 的比对日期，默认取目标月首日",
    )
    parser.add_argument("--preview-limit", type=int, default=10, help="控制台输出预览条数")
    parser.add_argument("--diff-output", help="可选：将完整差异清单写入指定 JSON 文件")
    parser.add_argument("--registry-json", help="可选：快照登记台账 JSON 路径")
    args = parser.parse_args()

    payload, printable, error_message = build_store_attr_snapshot_diff(
        target_month=args.target_month,
        file_path_arg=args.file_path,
        sheet_name=args.sheet_name,
        compare_date=args.compare_date,
        preview_limit=args.preview_limit,
    )

    diff_output_path = _resolve_diff_output_path(args.diff_output, args.target_month)
    diff_output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    registry_path = _resolve_path(args.registry_json, DEFAULT_REGISTRY_PATH)
    registry = _load_registry(registry_path)
    entry = _build_registry_entry(payload, error_message, diff_output_path)
    registry, action = _upsert_registry_entry(registry, entry)
    _write_registry(registry_path, registry)

    result = dict(printable)
    result["registry_action"] = action
    result["registry_entry"] = entry
    result["registry_path"] = _relative_to_repo(registry_path)
    result["diff_output_path"] = _relative_to_repo(diff_output_path)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if error_message else 0


if __name__ == "__main__":
    raise SystemExit(main())