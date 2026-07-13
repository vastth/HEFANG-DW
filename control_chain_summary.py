# -*- coding: utf-8 -*-
"""总控调度链路摘要协议。"""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any


TOTAL_CONTROL_SUMMARY_PATH_ENV = 'HEFANG_TOTAL_CONTROL_SUMMARY_PATH'
TOTAL_CONTROL_SUPPRESS_CHILD_WECHAT_ENV = 'HEFANG_TOTAL_CONTROL_SUPPRESS_CHILD_WECHAT'


def should_suppress_child_wechat_alert() -> bool:
    return os.getenv(TOTAL_CONTROL_SUPPRESS_CHILD_WECHAT_ENV, '0') == '1'


def get_total_control_summary_path() -> Path | None:
    raw_path = os.getenv(TOTAL_CONTROL_SUMMARY_PATH_ENV, '').strip()
    if not raw_path:
        return None
    return Path(raw_path)


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(value, date):
        return value.isoformat()
    return value


def write_total_control_chain_summary(payload: dict[str, Any]) -> None:
    output_path = get_total_control_summary_path()
    if output_path is None:
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(_to_jsonable(payload), ensure_ascii=False, indent=2),
        encoding='utf-8',
    )