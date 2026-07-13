from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
HANDOFF_PATH = REPO_ROOT / "docs" / "AGENT_HANDOFF.md"
TODO_PATH = REPO_ROOT / "docs" / "TODO_ISSUES.md"
LESSON_INDEX_PATH = REPO_ROOT / "docs" / "AGENT_LESSONS_INDEX.md"
DOC_SYNC_REPORT_PATH = REPO_ROOT / "reports" / "docs_code_alignment.json"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
DOC_SYNC_PARSE_SIZE_LIMIT = 5 * 1024 * 1024


def read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="ignore")


def clamp(text: str, max_chars: int) -> str:
    cleaned = text.strip()
    if len(cleaned) <= max_chars:
        return cleaned
    return cleaned[:max_chars].rstrip() + "\n...（已截断；如需完整证据，请定向读取原文）"


def extract_latest_handoff(text: str) -> str:
    marker = "## 交接日志"
    if marker in text:
        text = text.split(marker, 1)[1]

    lines = text.splitlines()
    start = None
    for index, line in enumerate(lines):
        if line.startswith("### ["):
            start = index
            break
    if start is None:
        return "未找到最新交接记录。"

    end = len(lines)
    for index in range(start + 1, len(lines)):
        if lines[index].startswith("### [") or lines[index].startswith("## 模板"):
            end = index
            break

    return "\n".join(lines[start:end]).strip("\n-")


def extract_issue_rows(text: str, heading: str) -> list[str]:
    lines = text.splitlines()
    start = None
    for index, line in enumerate(lines):
        if line.strip() == f"## {heading}":
            start = index + 1
            break
    if start is None:
        return []

    rows: list[str] = []
    for line in lines[start:]:
        if line.startswith("## "):
            break
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        if "---" in stripped or "编号" in stripped:
            continue
        if "暂无" in stripped:
            continue
        rows.append(stripped)
    return rows


def git_status_short() -> tuple[str, list[str]]:
    try:
        completed = subprocess.run(
            ["git", "-c", "core.quotepath=false", "status", "--short"],
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
        )
    except subprocess.TimeoutExpired:
        return "timeout", []
    except (OSError, subprocess.SubprocessError):
        return "error", []

    if completed.returncode != 0:
        return "error", []
    return "ok", [line for line in completed.stdout.splitlines() if line.strip()]


def doc_sync_summary() -> str:
    if not DOC_SYNC_REPORT_PATH.exists():
        return "未找到 reports/docs_code_alignment.json。"

    stat = DOC_SYNC_REPORT_PATH.stat()
    modified = datetime.fromtimestamp(stat.st_mtime).strftime(TIMESTAMP_FORMAT)
    size_kb = stat.st_size / 1024
    summary = f"reports/docs_code_alignment.json 存在；更新时间 {modified}；大小 {size_kb:.1f} KB。"
    if HANDOFF_PATH.exists() and stat.st_mtime < HANDOFF_PATH.stat().st_mtime:
        summary += " 早于最新交接记录；若本轮涉及 doc-sync 范围需复扫。"

    if stat.st_size > DOC_SYNC_PARSE_SIZE_LIMIT:
        return summary + " 文件较大，未全量解析；如需详情请定向读取或复跑审计。"

    try:
        data = json.loads(DOC_SYNC_REPORT_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return summary + " JSON 解析失败；如需详情请定向读取。"

    if isinstance(data, dict):
        interesting_keys = [key for key in ("summary", "high_risk", "medium_risk", "missing", "outdated") if key in data]
        if interesting_keys:
            return summary + " 顶层摘要键：" + ", ".join(interesting_keys) + "。"
        return summary + " 顶层键：" + ", ".join(list(data.keys())[:8]) + "。"
    if isinstance(data, list):
        return summary + f" 顶层为列表，条目数 {len(data)}。"
    return summary


def lesson_index_preview(max_lines: int = 18) -> str:
    text = read_text(LESSON_INDEX_PATH)
    if not text:
        return "未找到 docs/AGENT_LESSONS_INDEX.md；如需经验命中，请先运行 python scripts/build_agent_lessons_index.py。"

    lines = [line for line in text.splitlines() if line.strip()]
    return "\n".join(lines[:max_lines])


def summarize_git_status(status: str, lines: list[str], max_lines: int) -> str:
    if status == "timeout":
        return "git status --short 超时，未读取完整工作树状态；如需确认请人工复查。"
    if status == "error":
        return "git status --short 执行失败，未读取工作树状态；如需确认请人工复查。"
    if not lines:
        return "git status --short 成功执行，当前未读取到变更。"

    counts: dict[str, int] = {}
    for line in lines:
        status = line[:2].strip() or "??"
        counts[status] = counts.get(status, 0) + 1

    summary = "；".join(f"{status}={count}" for status, count in sorted(counts.items()))
    selected = lines[:max_lines]
    output = [f"共 {len(lines)} 个工作树条目（{summary}）。"]
    output.extend(f"- {line}" for line in selected)
    if len(lines) > max_lines:
        output.append(f"- ...（其余 {len(lines) - max_lines} 个条目未展开；如需完整列表请运行 git status --short）")
    return "\n".join(output)


def build_context_pack(max_handoff_chars: int = 2400, max_git_lines: int = 16) -> str:
    now = datetime.now().strftime(TIMESTAMP_FORMAT)
    handoff = clamp(extract_latest_handoff(read_text(HANDOFF_PATH)), max_handoff_chars)
    todo_text = read_text(TODO_PATH)
    p0_rows = extract_issue_rows(todo_text, "P0")
    p1_rows = extract_issue_rows(todo_text, "P1")
    git_status_state, git_lines = git_status_short()

    output: list[str] = [
        "# Agent Context Pack",
        "",
        f"> 生成时间：{now}",
        "> 用途：作为本轮 Agent 开局的短上下文入口；不替代必要的定向证据读取。",
        "",
        "## 1. 最新交接摘要",
        "",
        handoff or "未读取到交接摘要。",
        "",
        "## 2. 当前待办风险",
        "",
        "### P0",
        "",
        "\n".join(p0_rows) if p0_rows else "暂无未关闭 P0。",
        "",
        "### P1",
        "",
        "\n".join(p1_rows[:8]) if p1_rows else "暂无 P1。",
        "",
        "## 3. 工作树变更概览",
        "",
        summarize_git_status(git_status_state, git_lines, max_git_lines),
        "",
        "## 4. 文档同步审计快照",
        "",
        doc_sync_summary(),
        "",
        "## 5. 经验台账索引预览",
        "",
        lesson_index_preview(),
        "",
        "## 6. 本轮上下文读取策略",
        "",
        "- 不整篇读取 docs/AGENT_LESSONS.md；先用索引或关键词命中。",
        "- 不整篇读取长会议纪要或历史归档；优先读取当前状态、版本记录和命中片段。",
        "- Oracle/MySQL 查询结果只作为数据证据；如需完整结果，落盘到 reports/ 或 reports/context_cache/ 后在对话中总结。",
        "- 如本上下文包不足以支持修改，继续定向读取真实文件、脚本输出或数据库快照。",
        "",
    ]
    return "\n".join(output)


def main() -> int:
    parser = argparse.ArgumentParser(description="生成 hefang_dw Agent 开局上下文压缩包。")
    parser.add_argument("--output", help="可选：将上下文包写入指定路径；不提供则输出到 stdout。")
    parser.add_argument("--max-handoff-chars", type=int, default=2400, help="最新交接记录最大字符数。")
    args = parser.parse_args()

    content = build_context_pack(max_handoff_chars=args.max_handoff_chars)
    if args.output:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = REPO_ROOT / output_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content, encoding="utf-8")
        print(f"已写入 {output_path}")
    else:
        print(content)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
