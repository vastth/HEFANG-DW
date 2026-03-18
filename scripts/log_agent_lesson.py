#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
log_agent_lesson.py — Agent 经验台帐写入工具

用途：将可复用的排障结论、字段语义修正、用户业务纠错沉淀到
      docs/AGENT_LESSONS.md，供后续 Copilot / Claude Code / OpenCode 复用。
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
LESSON_FILE = REPO_ROOT / "docs" / "AGENT_LESSONS.md"
ENTRY_MARKER = "## 经验记录\n"
VERSION_MARKER = "## 版本记录\n"


def _build_entry(source, category, trigger, mistake, correction, evidence, prevention):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    evidence_md = "\n".join(f"- {item}" for item in evidence) if evidence else "- （无）"
    return (
        f"### [{now}] · {source} · {category}\n\n"
        f"**触发场景**：{trigger}\n\n"
        f"**错误假设**：{mistake}\n\n"
        f"**修正结论**：{correction}\n\n"
        f"**证据**：\n{evidence_md}\n\n"
        f"**预防动作**：{prevention}\n\n"
        f"---\n\n"
    )


def _read_sections():
    if not LESSON_FILE.exists():
        raise FileNotFoundError(f"未找到经验台帐文件：{LESSON_FILE}")

    text = LESSON_FILE.read_text(encoding="utf-8")
    entry_pos = text.find(ENTRY_MARKER)
    version_pos = text.find(VERSION_MARKER)

    if entry_pos == -1 or version_pos == -1:
        raise RuntimeError("AGENT_LESSONS.md 结构不符合预期，缺少固定标题")

    header = text[: entry_pos + len(ENTRY_MARKER)]
    body = text[entry_pos + len(ENTRY_MARKER):version_pos]
    footer = text[version_pos:]
    return header, body, footer


def main():
    parser = argparse.ArgumentParser(description="向 docs/AGENT_LESSONS.md 追加一条经验记录")
    parser.add_argument("--source", required=True, choices=["task", "user-feedback"], help="经验来源")
    parser.add_argument("--category", required=True, help="经验类别，例如 field-mapping / business-rule / mcp")
    parser.add_argument("--trigger", required=True, help="触发场景")
    parser.add_argument("--mistake", required=True, help="错误假设")
    parser.add_argument("--correction", required=True, help="修正结论")
    parser.add_argument("--evidence", nargs="*", default=[], help="证据列表，可多个")
    parser.add_argument("--prevention", required=True, help="后续预防动作")
    parser.add_argument("--dry-run", action="store_true", help="仅打印，不落盘")
    args = parser.parse_args()

    entry = _build_entry(
        args.source,
        args.category,
        args.trigger,
        args.mistake,
        args.correction,
        args.evidence,
        args.prevention,
    )

    if args.dry_run:
        print(entry)
        return

    header, body, footer = _read_sections()
    LESSON_FILE.write_text(header + "\n" + entry + body.lstrip("\n") + footer, encoding="utf-8")
    print(f"[OK] 已写入经验台帐：{LESSON_FILE}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[ERROR] 写入经验台帐失败：{exc}", file=sys.stderr)
        sys.exit(1)