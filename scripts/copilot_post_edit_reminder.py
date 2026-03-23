from __future__ import annotations

import json
import re
import sys
from datetime import datetime
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
LOG_DIR = REPO_ROOT / "logs"
LOG_PATH = LOG_DIR / "copilot_post_edit_reminder.log"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

RULES: list[tuple[str, re.Pattern[str], str]] = [
    (
        "copilot-customization",
        re.compile(r"(?i)(\.github[\\/].*(\.md|\.json)|copilot-instructions\.md)"),
        "Customization reminder: check runtime acceptance, meeting notes, handoff, and lessons.",
    ),
    (
        "etl",
        re.compile(r"(?i)(etl_.*\.py|run_etl\.py|run_ods\.py|scheduled_etl\.py|test_etl_automation\.py)"),
        "ETL reminder: check minimum validation, doc-sync, handoff, and lessons.",
    ),
    (
        "sql",
        re.compile(r"(?i)(^|[\\/])SQL[\\/].*\.sql"),
        "SQL reminder: check data dictionary, contracts, doc-sync, and handoff.",
    ),
    (
        "meeting-minutes",
        re.compile(r"(?i)docs[\\/]misc[\\/].*会议纪要.*\.md"),
        "Meeting notes reminder: check current status, new conclusions, version record, and handoff.",
    ),
    (
        "data-dictionary",
        re.compile(r"(?i)docs[\\/](MYSQL数据字典|HFSY数据字典)\.md"),
        "Data dictionary reminder: check schema evidence, mappings, contracts, and doc-sync.",
    ),
    (
        "governance-docs",
        re.compile(r"(?i)docs[\\/](AGENT_HANDOFF|AGENT_LESSONS|TODO_ISSUES)\.md"),
        "Governance reminder: check todo state, handoff conclusions, lessons, and meeting notes.",
    ),
    (
        "runbook-docs",
        re.compile(r"(?i)docs[\\/](ARCHITECTURE|RUNBOOK|DATA_CONTRACTS|ETL业务逻辑说明|数据结构与映射手册|业务逻辑与指标规范|数据仓库与ETL手册|SQL开发手册)\.md"),
        "Runbook reminder: check version record, run instructions, doc-sync, and AGENT_HANDOFF.",
    ),
    (
        "readme",
        re.compile(r"(?i)README\.md"),
        "README reminder: check entry docs, commands, version record, related docs, and handoff.",
    ),
    (
        "doc",
        re.compile(r"(?i)docs[\\/].*\.md"),
        "Docs reminder: check version record, AGENT_HANDOFF, meeting notes, and doc rescan.",
    ),
]


def ensure_log_dir() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def write_hook_log(result: str, matched_rule: str, preview: str) -> None:
    entry = {
        "timestamp": datetime.now().strftime(TIMESTAMP_FORMAT),
        "result": result,
        "matchedRule": matched_rule,
        "preview": preview,
    }
    with LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")


def main() -> int:
    ensure_log_dir()
    raw_input = sys.stdin.read()
    preview = raw_input.replace("\r", " ").replace("\n", " ")[:300]

    if not raw_input.strip():
        write_hook_log("empty-input", "", "")
        sys.stdout.write('{"continue":true}\n')
        return 0

    for matched_rule, pattern, message in RULES:
        if pattern.search(raw_input):
            write_hook_log("warning", matched_rule, preview)
            sys.stderr.write(message + "\n")
            return 1

    write_hook_log("no-match", "", preview)
    sys.stdout.write('{"continue":true}\n')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())