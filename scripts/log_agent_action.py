#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
log_agent_action.py — Agent 协作交接日志写入工具

用途：Claude Code 或 Copilot 完成一组变更后，调用本脚本在
      docs/AGENT_HANDOFF.md 顶部追加一条结构化交接记录。

用法：
    python scripts/log_agent_action.py \\
      --agent "Claude Code" \\
      --action "修复 ETL 口径" \\
      --summary "修正 dws_sales 退货金额计算逻辑" \\
      --files "etl_dws_sales.py:修改:return_amt 计算逻辑修正" \\
              "docs/DATA_CONTRACTS.md:修改:同步口径说明" \\
      --notes "etl_dws_sales.py:L88 的 return_amt 公式已从 SUM 改为 ABS(SUM)" \\
              "Copilot 审计时注意 test_etl_automation.py 中对应断言是否需更新" \\
      --todos "验证回填后近30天数据无异常" \\
              "test_etl_automation.py 新增退货金额断言"

归档策略：AGENT_HANDOFF.md 超过 MAX_ENTRIES 条记录时，自动将最早的
          若干条归档到 docs/AGENT_HANDOFF_archive.md。
"""

import argparse
import os
import re
import sys
from datetime import datetime
from pathlib import Path

# ─── 配置 ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR    = Path(__file__).resolve().parent
REPO_ROOT     = SCRIPT_DIR.parent
HANDOFF_FILE  = REPO_ROOT / "docs" / "AGENT_HANDOFF.md"
ARCHIVE_FILE  = REPO_ROOT / "docs" / "AGENT_HANDOFF_archive.md"
MAX_ENTRIES   = 10          # 保留最近多少条（超过则归档）
SECTION_SEP   = "---\n\n### ["  # 用于定位记录边界


def _build_entry(agent: str, action: str, summary: str,
                 files: list[str], notes: list[str], todos: list[str]) -> str:
    """构建一条交接记录的 Markdown 文本。"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # 文件变更表格
    file_rows = []
    for f in files:
        parts = f.split(":", 2)
        path_str  = parts[0].strip() if len(parts) > 0 else f
        change    = parts[1].strip() if len(parts) > 1 else "修改"
        desc      = parts[2].strip() if len(parts) > 2 else ""
        file_rows.append(f"| `{path_str}` | {change} | {desc} |")

    file_table = (
        "| 文件 | 变更类型 | 说明 |\n"
        "|------|---------|------|\n"
        + "\n".join(file_rows)
        if file_rows else "（无文件变更）"
    )

    # 接棒须知列表
    notes_md = "\n".join(f"- {n}" for n in notes) if notes else "- 无特殊注意事项"

    # 未完成项
    todos_md = "\n".join(f"- [ ] {t}" for t in todos) if todos else "- [ ] （无）"

    return (
        f"---\n\n"
        f"### [{now}] · {agent} · {action}\n\n"
        f"**摘要**：{summary}\n\n"
        f"**变更文件**：\n\n"
        f"{file_table}\n\n"
        f"**Copilot 接棒须知**：\n"
        f"{notes_md}\n\n"
        f"**未完成项**：\n"
        f"{todos_md}\n"
    )


def _split_entries(body: str) -> list[str]:
    """将交接日志正文拆分为独立记录列表（最新在前）。"""
    # 以 "---\n\n### [" 为分隔符
    parts = re.split(r'(?=---\n\n### \[)', body)
    return [p for p in parts if p.strip()]


def _read_handoff() -> tuple[str, str, str]:
    """
    返回 (header, journal_body, template_section)
    header: 文件头部（第一个 '## 交接日志' 之前）
    journal_body: '## 交接日志' 与 '## 模板' 之间的内容
    template_section: '## 模板' 及其之后的内容
    """
    text = HANDOFF_FILE.read_text(encoding="utf-8")

    journal_marker  = "## 交接日志\n"
    template_marker = "## 模板"

    j_pos = text.find(journal_marker)
    t_pos = text.find("\n" + template_marker)

    if j_pos == -1:
        # 文件格式不符合预期，整体作为 body
        return "", text, ""

    header = text[:j_pos + len(journal_marker)]

    if t_pos == -1:
        body     = text[j_pos + len(journal_marker):]
        template = ""
    else:
        body     = text[j_pos + len(journal_marker): t_pos]
        template = text[t_pos:]

    return header, body, template


def _write_handoff(header: str, entries: list[str], template: str) -> None:
    """将记录列表写回文件。"""
    body = "\n".join(entries)
    content = header + "\n" + body + "\n" + template
    HANDOFF_FILE.write_text(content, encoding="utf-8")


def _archive_old_entries(entries: list[str], keep: int) -> list[str]:
    """将超出 keep 条的旧记录归档，返回保留的记录列表。"""
    if len(entries) <= keep:
        return entries

    to_archive = entries[keep:]
    to_keep    = entries[:keep]

    archive_header = (
        "# AGENT_HANDOFF_archive.md — Agent 交接日志归档\n\n"
        "> 本文件由 `scripts/log_agent_action.py` 自动维护，请勿手动编辑结构。\n\n"
        "## 归档记录\n\n"
    )

    if ARCHIVE_FILE.exists():
        existing = ARCHIVE_FILE.read_text(encoding="utf-8")
        # 去掉 header，只保留记录体
        marker = "## 归档记录\n\n"
        pos = existing.find(marker)
        existing_body = existing[pos + len(marker):] if pos != -1 else existing
        ARCHIVE_FILE.write_text(
            archive_header + "\n".join(to_archive) + "\n" + existing_body,
            encoding="utf-8"
        )
    else:
        ARCHIVE_FILE.write_text(
            archive_header + "\n".join(to_archive),
            encoding="utf-8"
        )

    print(f"  [归档] 将 {len(to_archive)} 条旧记录归档到 {ARCHIVE_FILE.name}")
    return to_keep


def main() -> None:
    parser = argparse.ArgumentParser(
        description="向 docs/AGENT_HANDOFF.md 追加一条 Agent 交接记录",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        "--agent", required=True,
        help="执行方（例如：Claude Code / GitHub Copilot）"
    )
    parser.add_argument(
        "--action", required=True,
        help="操作类型（例如：新增文件 / 修复 ETL / 文档同步）"
    )
    parser.add_argument(
        "--summary", required=True,
        help="一句话摘要，描述本次做了什么"
    )
    parser.add_argument(
        "--files", nargs="*", default=[],
        metavar="路径:变更类型:说明",
        help="变更文件列表，格式 '路径:新增/修改/删除:说明'，可多个"
    )
    parser.add_argument(
        "--notes", nargs="*", default=[],
        metavar="注意事项",
        help="Copilot 接棒须知，可多个"
    )
    parser.add_argument(
        "--todos", nargs="*", default=[],
        metavar="未完成项",
        help="未完成待办项，可多个"
    )
    parser.add_argument(
        "--max-entries", type=int, default=MAX_ENTRIES,
        help=f"保留最近多少条记录（默认 {MAX_ENTRIES}）"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="仅输出将要写入的内容，不实际修改文件"
    )

    args = parser.parse_args()

    # 检查目标文件存在
    if not HANDOFF_FILE.exists():
        print(f"[ERROR] {HANDOFF_FILE} 不存在，请先创建 docs/AGENT_HANDOFF.md", file=sys.stderr)
        sys.exit(1)

    # 构建新记录
    new_entry = _build_entry(
        agent   = args.agent,
        action  = args.action,
        summary = args.summary,
        files   = args.files,
        notes   = args.notes,
        todos   = args.todos,
    )

    if args.dry_run:
        print("── DRY RUN ── 以下内容将被追加到 AGENT_HANDOFF.md 顶部 ──")
        print(new_entry)
        return

    # 读取现有内容
    header, body, template = _read_handoff()

    # 拆分现有记录
    existing_entries = _split_entries(body)

    # 新记录放最前
    all_entries = [new_entry] + existing_entries

    # 归档超出部分
    kept_entries = _archive_old_entries(all_entries, args.max_entries)

    # 写回
    _write_handoff(header, kept_entries, template)

    print(f"[OK] 已追加交接记录到 docs/AGENT_HANDOFF.md")
    print(f"     Agent: {args.agent} | 操作: {args.action}")
    print(f"     当前记录数: {len(kept_entries)} / {args.max_entries}")
    print(f"     变更文件数: {len(args.files)}")


if __name__ == "__main__":
    main()
