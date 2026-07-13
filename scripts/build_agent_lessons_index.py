from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
LESSONS_PATH = REPO_ROOT / "docs" / "AGENT_LESSONS.md"
DEFAULT_OUTPUT_PATH = REPO_ROOT / "docs" / "AGENT_LESSONS_INDEX.md"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

HEADING_RE = re.compile(r"^### \[(?P<date>[^\]]+)\] · (?P<source>[^·]+) · (?P<category>.+)$")
FIELD_RE_TEMPLATE = r"\*\*{field}\*\*：(?P<value>.*?)(?=\n\n\*\*|\n---|\Z)"

KEYWORD_RULES: dict[str, tuple[str, ...]] = {
    "timeout": ("超时", "timeout", "read_timeout", "write_timeout", "long_running", "60 秒"),
    "dbhub": ("DBHub", "collation", "ONLY_FULL_GROUP_BY", "用户变量"),
    "oracle": ("Oracle", "BOSNDS3", "ALL_TAB_COLUMNS", "CTE"),
    "mysql": ("MySQL", "pymysql", "information_schema", "唯一键"),
    "powershell": ("PowerShell", "PSReadLine", "pwsh"),
    "sales": ("销售", "ads_sales", "dws_sales", "门店日报"),
    "inventory": ("库存", "inventory", "ads_inventory"),
    "business-rule": ("业务口径", "公式", "过滤", "阈值", "共同考核"),
    "performance": ("性能", "耗时", "重复重算", "cache", "缓存"),
    "doc-sync": ("文档", "doc-sync", "check_doc_sync", "字典"),
    "agent-context": ("上下文", "prompt", "hook", "Copilot", "agent"),
}


@dataclass
class LessonEntry:
    date: str
    source: str
    category: str
    line: int
    trigger: str
    correction: str
    evidence: str
    keywords: list[str]


def read_text(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"找不到经验台账：{path}")
    return path.read_text(encoding="utf-8", errors="ignore")


def extract_field(block: str, field: str) -> str:
    pattern = re.compile(FIELD_RE_TEMPLATE.format(field=re.escape(field)), re.S)
    match = pattern.search(block)
    if not match:
        return ""
    return " ".join(match.group("value").strip().split())


def extract_evidence(block: str) -> str:
    match = re.search(r"\*\*证据\*\*：(?P<value>.*?)(?=\n\n\*\*|\n---|\Z)", block, re.S)
    if not match:
        return ""
    lines = []
    for line in match.group("value").splitlines():
        stripped = line.strip().lstrip("- ").strip()
        if stripped:
            lines.append(stripped)
    return "; ".join(lines[:4])


def infer_keywords(category: str, block: str) -> list[str]:
    haystack = f"{category}\n{block}".lower()
    keywords: list[str] = []
    for keyword, needles in KEYWORD_RULES.items():
        if any(needle.lower() in haystack for needle in needles):
            keywords.append(keyword)
    if category.strip() and category.strip() not in keywords:
        keywords.insert(0, category.strip())
    return keywords[:6]


def truncate(value: str, max_chars: int = 86) -> str:
    cleaned = value.replace("|", "／").strip()
    if len(cleaned) <= max_chars:
        return cleaned
    return cleaned[: max_chars - 1].rstrip() + "…"


def parse_entries(text: str) -> list[LessonEntry]:
    lines = text.splitlines()
    headings: list[tuple[int, re.Match[str]]] = []
    for index, line in enumerate(lines, start=1):
        match = HEADING_RE.match(line)
        if match:
            headings.append((index, match))

    entries: list[LessonEntry] = []
    for pos, (line_no, match) in enumerate(headings):
        start = line_no - 1
        end = headings[pos + 1][0] - 1 if pos + 1 < len(headings) else len(lines)
        block = "\n".join(lines[start:end])
        trigger = extract_field(block, "触发场景")
        correction = extract_field(block, "修正结论")
        evidence = extract_evidence(block)
        category = match.group("category").strip()
        entries.append(
            LessonEntry(
                date=match.group("date").strip(),
                source=match.group("source").strip(),
                category=category,
                line=line_no,
                trigger=trigger,
                correction=correction,
                evidence=evidence,
                keywords=infer_keywords(category, block),
            )
        )
    return entries


def build_markdown(entries: list[LessonEntry], max_entries: int | None = None, detailed: bool = False) -> str:
    selected = entries if max_entries is None else entries[:max_entries]
    now = datetime.now().strftime(TIMESTAMP_FORMAT)
    header = "| 日期 | 分类 | 关键词 | 触发场景 | 修正结论 | 原文行号 | 证据摘录 |" if detailed else "| 日期 | 分类 | 关键词 | 触发场景 | 原文行号 |"
    separator = "|---|---|---|---|---|---:|---|" if detailed else "|---|---|---|---|---:|"
    output = [
        "# AGENT_LESSONS_INDEX.md — Agent 经验台帐索引",
        "",
        "> 自动生成文件。用于先按关键词定位经验，再定向读取 `docs/AGENT_LESSONS.md` 具体条目，避免整篇经验台账进入常规上下文。",
        f"> 生成时间：{now}；索引条目：{len(selected)} / {len(entries)}。",
        "",
        "## 使用方式",
        "",
        "1. 先按关键词、分类或触发场景搜索本索引。",
        "2. 命中后再按 `原文行号` 定向读取 `docs/AGENT_LESSONS.md` 的对应条目。",
        "3. 不要把完整 `docs/AGENT_LESSONS.md` 作为常规上下文读取。",
        "",
        "## 索引",
        "",
        header,
        separator,
    ]
    for entry in selected:
        cells = [
            truncate(entry.date, 18),
            truncate(entry.category, 24),
            truncate(", ".join(entry.keywords), 40),
            truncate(entry.trigger, 58),
            str(entry.line),
        ]
        if detailed:
            cells.insert(4, truncate(entry.correction, 70))
            cells.append(truncate(entry.evidence, 56))
        output.append(
            "| "
            + " | ".join(cells)
            + " |"
        )
    output.append("")
    return "\n".join(output)


def main() -> int:
    parser = argparse.ArgumentParser(description="为 docs/AGENT_LESSONS.md 生成轻量索引。")
    parser.add_argument("--input", default=str(LESSONS_PATH), help="经验台账路径。")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="索引输出路径。")
    parser.add_argument("--max-entries", type=int, default=0, help="最多输出多少条；0 表示全部。")
    parser.add_argument("--detailed", action="store_true", help="输出修正结论和证据摘录列；默认生成更短索引。")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    if not input_path.is_absolute():
        input_path = REPO_ROOT / input_path
    if not output_path.is_absolute():
        output_path = REPO_ROOT / output_path

    entries = parse_entries(read_text(input_path))
    max_entries = None if args.max_entries <= 0 else args.max_entries
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(build_markdown(entries, max_entries=max_entries, detailed=args.detailed), encoding="utf-8")
    print(f"已生成 {output_path}，条目数 {len(entries)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
