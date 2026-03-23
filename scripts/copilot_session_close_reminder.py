from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
LOG_DIR = REPO_ROOT / "logs"
POST_EDIT_LOG_PATH = LOG_DIR / "copilot_post_edit_reminder.log"
STOP_LOG_PATH = LOG_DIR / "copilot_session_close_reminder.log"
STATE_PATH = LOG_DIR / "copilot_session_close_reminder_state.json"
RECENT_WINDOW_MINUTES = 180
DEDUPE_WINDOW_MINUTES = 15
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def ensure_log_dir() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def write_hook_log(result: str, matched_rules: str, preview: str) -> None:
    entry = {
        "timestamp": datetime.now().strftime(TIMESTAMP_FORMAT),
        "result": result,
        "matchedRules": matched_rules,
        "preview": preview,
    }
    with STOP_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")


def parse_timestamp(value: str) -> datetime | None:
    try:
        return datetime.strptime(value, TIMESTAMP_FORMAT)
    except ValueError:
        return None


def get_recent_matched_rules() -> list[str]:
    if not POST_EDIT_LOG_PATH.exists():
        return []

    cutoff = datetime.now() - timedelta(minutes=RECENT_WINDOW_MINUTES)
    rules: list[str] = []

    for line in POST_EDIT_LOG_PATH.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue

        if entry.get("result") != "warning":
            continue

        matched_rule = str(entry.get("matchedRule", "")).strip()
        if not matched_rule:
            continue

        timestamp = parse_timestamp(str(entry.get("timestamp", "")))
        if timestamp is None or timestamp < cutoff:
            continue

        rules.append(matched_rule)

    return rules


def should_suppress_reminder(signature: str) -> bool:
    if not STATE_PATH.exists():
        return False

    try:
        state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False

    last_signature = str(state.get("signature", ""))
    last_timestamp = parse_timestamp(str(state.get("timestamp", "")))
    if last_signature != signature or last_timestamp is None:
        return False

    return last_timestamp > datetime.now() - timedelta(minutes=DEDUPE_WINDOW_MINUTES)


def save_reminder_state(signature: str) -> None:
    state = {
        "timestamp": datetime.now().strftime(TIMESTAMP_FORMAT),
        "signature": signature,
    }
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")


def build_action_hints(rules: list[str]) -> list[str]:
    hints: list[str] = []

    if "copilot-customization" in rules:
        hints.append("runtime acceptance and meeting notes")

    if "etl" in rules or "sql" in rules:
        hints.append("minimum validation and doc-sync")

    doc_rules = {
        "meeting-minutes",
        "data-dictionary",
        "governance-docs",
        "runbook-docs",
        "readme",
        "doc",
    }
    if any(rule in doc_rules for rule in rules):
        hints.append("version record and doc consistency")

    hints.extend(["AGENT_HANDOFF", "AGENT_LESSONS", "open todos"])
    return sorted(set(hints))


def main() -> int:
    ensure_log_dir()
    raw_input = sys.stdin.read()
    preview = raw_input.replace("\r", " ").replace("\n", " ")[:300]

    recent_rules = sorted(set(get_recent_matched_rules()))
    if not recent_rules:
        write_hook_log("no-recent-edit-signal", "", preview)
        sys.stdout.write('{"continue":true}\n')
        return 0

    signature = ",".join(recent_rules)
    if should_suppress_reminder(signature):
        write_hook_log("deduped", signature, preview)
        sys.stdout.write('{"continue":true}\n')
        return 0

    hints = build_action_hints(recent_rules)
    message = (
        "Stop hook reminder: recent edit types: "
        f"{', '.join(recent_rules)}. "
        "Check "
        f"{'; '.join(hints)} before ending the session."
    )

    save_reminder_state(signature)
    write_hook_log("warning", signature, preview)
    sys.stderr.write(message + "\n")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())