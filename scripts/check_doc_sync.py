import argparse
import json
import re
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT_DIR / "reports" / "docs_code_alignment.json"
DOC_AUDIT_SCOPE = (
    Path("README.md"),
    Path("docs") / "ARCHITECTURE.md",
    Path("docs") / "DATA_CONTRACTS.md",
    Path("docs") / "RUNBOOK.md",
    Path("docs") / "MYSQL数据字典.md",
    Path("docs") / "数据结构与映射手册.md",
    Path("docs") / "业务逻辑与指标规范.md",
    Path("docs") / "数据仓库与ETL手册.md",
    Path("docs") / "ETL业务逻辑说明.md",
    Path("docs") / "SQL开发手册.md",
)
MARKDOWN_CODEBLOCK_IGNORED_FILES = {
    Path("README.md"),
    Path("docs") / "RUNBOOK.md",
}

TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]{2,}")
TABLE_RE = re.compile(r"^(ods|dim|dwd|dws|ads)_[a-z0-9_]+$")
FIELD_RE = re.compile(r".*_(id|qty|amount|date|time|code|name)$")
TRAILING_UNDERSCORE_RE = re.compile(r".*_$")
NUMBERED_PLACEHOLDER_RE = re.compile(r".*_\d+$")
LINE_ANCHOR_RE = re.compile(r"l\d+$")
YOUR_PLACEHOLDER_RE = re.compile(r"your_[a-z0-9_]+$")
RAW_URL_RE = re.compile(r"https?://\S+")
MARKDOWN_LINK_RE = re.compile(r"!?\[([^\]]*)\]\(([^)]+)\)")
PLANNED_MARKERS = (
    "规划/未实现",
    "未在代码实现",
    "规划方案",
    "TODO(human)",
    "规划态",
    "规划态未实现",
    "未实现",
    "草案",
    "设计稿",
)
NOT_FILLED_MARKER = "字段存在但当前ETL不填充"
MYSQL_SNAPSHOT_PATH = ROOT_DIR / "reports" / "snapshot_mysql_hefangdw_schema.json"
SELF_AUDIT_SCRIPT_PATH = ROOT_DIR / "scripts" / "check_doc_sync.py"

AUDIT_META_TERMS = {
    "advisories",
    "docs_advisories",
    "code_advisories",
    "non_blocking",
    "non_blocking_advisories_total",
    "field_exists_but_not_filled",
    "original_risk",
    "reason",
    "not_filled_marker",
    "mysql_snapshot_path",
}

STOPWORDS = {
    "select",
    "from",
    "where",
    "table",
    "create",
    "insert",
    "update",
    "delete",
    "python",
    "sql",
    "true",
    "false",
    "none",
    "and",
    "or",
    "not",
    "data",
    "date",
    "time",
    "value",
    "count",
    "index",
    "primary",
    "key",
    "varchar",
    "int",
    "decimal",
    "char",
    "datetime",
    "float",
    "double",
    "text",
    "set",
    "into",
    "join",
    "left",
    "right",
    "inner",
    "outer",
    "group",
    "by",
    "order",
    "limit",
    "as",
    "is",
    "on",
    "if",
    "else",
    "for",
    "while",
    "return",
    "class",
    "def",
    "import",
    "with",
    "this",
    "that",
    "use",
    "example",
    "todo",
    "note",
    "docs",
    "doc",
    "readme",
    "config",
    "mysql",
    "oracle",
    "get_snapshot_date",
    "sql_name",
    "template_name",
    "total_amount",
    "idx_channel_code",
    "idx_store_code",
    "idx_wing_code",
}


def iter_files(root_dir: Path, include_exts, exclude_dirs):
    for path in root_dir.rglob("*"):
        if path.is_dir():
            continue
        if path.suffix.lower() not in include_exts:
            continue
        if any(part in exclude_dirs for part in path.parts):
            continue
        yield path


def _looks_like_file_token(term: str, file_stems: set) -> bool:
    if term in file_stems:
        return True
    if term.endswith("py") and term[:-2] in file_stems:
        return True
    if term.endswith("sql") and term[:-3] in file_stems:
        return True
    return False


def extract_terms(text: str, file_stems: set, excluded_terms=None):
    excluded_terms = excluded_terms or set()
    terms = set()
    for match in TOKEN_RE.findall(text):
        term = match.lower()
        if term in excluded_terms:
            continue
        if term in AUDIT_META_TERMS:
            continue
        if term in STOPWORDS:
            continue
        if _looks_like_file_token(term, file_stems):
            continue
        if TRAILING_UNDERSCORE_RE.match(term):
            continue
        if NUMBERED_PLACEHOLDER_RE.match(term):
            continue
        if LINE_ANCHOR_RE.fullmatch(term):
            continue
        if YOUR_PLACEHOLDER_RE.fullmatch(term):
            continue
        terms.add(term)
    return terms


def collect_internal_function_terms(script_path: Path):
    terms = set()
    if not script_path.exists():
        return terms
    try:
        content = script_path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return terms

    for func in re.findall(r"^def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", content, flags=re.MULTILINE):
        terms.add(func.lower())
    return terms


def _collect_planned_terms_from_markdown(lines):
    planned_terms = set()
    heading_indices = [
        idx for idx, line in enumerate(lines) if line.lstrip().startswith("## ")
    ]
    if not heading_indices:
        return planned_terms

    heading_indices.append(len(lines))
    for idx in range(len(heading_indices) - 1):
        start = heading_indices[idx]
        end = heading_indices[idx + 1]
        section_lines = lines[start:end]
        if not any(marker in line for line in section_lines for marker in PLANNED_MARKERS):
            continue
        for line in section_lines:
            for term in TOKEN_RE.findall(line):
                term = term.lower()
                if term in STOPWORDS:
                    continue
                planned_terms.add(term)
    return planned_terms


def preprocess_markdown_content(path: Path, content: str) -> list[str]:
    relative_path = path.relative_to(ROOT_DIR)
    ignore_codeblocks = relative_path in MARKDOWN_CODEBLOCK_IGNORED_FILES
    lines = []
    in_codeblock = False

    for raw_line in content.splitlines():
        stripped = raw_line.strip()
        if stripped.startswith("```"):
            if ignore_codeblocks:
                in_codeblock = not in_codeblock
                continue
        if ignore_codeblocks and in_codeblock:
            continue

        line = MARKDOWN_LINK_RE.sub(lambda match: match.group(1), raw_line)
        line = RAW_URL_RE.sub(" ", line)
        lines.append(line)

    return lines


def classify_risk(term: str) -> str:
    if TABLE_RE.match(term):
        return "high"
    if FIELD_RE.match(term):
        return "medium"
    return "low"


def load_mysql_schema_fields(snapshot_path: Path):
    fields = set()
    if not snapshot_path.exists():
        return fields
    try:
        data = json.loads(snapshot_path.read_text(encoding="utf-8", errors="ignore"))
    except (OSError, json.JSONDecodeError):
        return fields

    for table in data.get("tables", []):
        for col in table.get("columns", []):
            name = str(col.get("column_name", "")).strip().lower()
            if name:
                fields.add(name)
    return fields


def collect_not_filled_terms_from_data_contracts(path: Path):
    terms = set()
    if not path.exists():
        return terms
    try:
        content = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return terms

    for line in content.splitlines():
        if NOT_FILLED_MARKER not in line:
            continue
        for term in TOKEN_RE.findall(line):
            candidate = term.lower()
            if candidate in STOPWORDS:
                continue
            if FIELD_RE.match(candidate):
                terms.add(candidate)
    return terms


def collect_terms(files, file_stems: set, per_file_excluded_terms=None):
    per_file_excluded_terms = per_file_excluded_terms or {}
    terms = set()
    sources = {}
    for path in files:
        try:
            content = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue

        excluded_terms = per_file_excluded_terms.get(path.resolve(), set())
        if path.suffix.lower() == ".md" and "docs" in path.parts:
            lines = preprocess_markdown_content(path, content)
            planned_terms = _collect_planned_terms_from_markdown(lines)
            for line in lines:
                for term in extract_terms(line, file_stems, excluded_terms=excluded_terms):
                    if term in planned_terms:
                        continue
                    terms.add(term)
                    sources.setdefault(term, set()).add(str(path.relative_to(ROOT_DIR)))
        elif path.name.lower() == "readme.md":
            lines = preprocess_markdown_content(path, content)
            for line in lines:
                for term in extract_terms(line, file_stems, excluded_terms=excluded_terms):
                    terms.add(term)
                    sources.setdefault(term, set()).add(str(path.relative_to(ROOT_DIR)))
        else:
            for term in extract_terms(content, file_stems, excluded_terms=excluded_terms):
                terms.add(term)
                sources.setdefault(term, set()).add(str(path.relative_to(ROOT_DIR)))
    return terms, sources


def pack_terms(terms, sources, downgrade_terms=None, schema_fields=None):
    downgrade_terms = downgrade_terms or set()
    schema_fields = schema_fields or set()
    packed = []
    advisories = []
    for term in sorted(terms):
        risk = classify_risk(term)
        item = {
            "term": term,
            "risk": risk,
            "sources": sorted(sources.get(term, [])),
        }

        if (
            risk == "medium"
            and term in downgrade_terms
            and term in schema_fields
        ):
            item["original_risk"] = "medium"
            item["risk"] = "low"
            item["reason"] = "field_exists_but_not_filled"
            advisories.append(
                {
                    "term": term,
                    "reason": "field_exists_but_not_filled",
                    "non_blocking": True,
                    "sources": item["sources"],
                }
            )

        packed.append(item)
    return packed, advisories


def risk_summary(items):
    summary = {"high": 0, "medium": 0, "low": 0}
    for item in items:
        summary[item["risk"]] += 1
    return summary


def main():
    parser = argparse.ArgumentParser(description="Audit docs/code term alignment.")
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Output JSON path (default: reports/docs_code_alignment.json)",
    )
    args = parser.parse_args()

    docs_files = [
        ROOT_DIR / relative_path
        for relative_path in DOC_AUDIT_SCOPE
        if (ROOT_DIR / relative_path).exists()
    ]

    code_exts = {".py", ".sql", ".bat", ".yml", ".yaml", ".ini", ".cfg", ".conf"}
    exclude_dirs = {
        ".git",
        "__pycache__",
        "logs",
        "data",
        "notebooks",
        "docs",
        ".github",
        "reports",
        "example_repos",
    }
    code_files = iter_files(ROOT_DIR, code_exts, exclude_dirs)

    file_stems = {
        path.stem.lower()
        for path in iter_files(ROOT_DIR, code_exts, exclude_dirs)
    }

    self_internal_function_terms = collect_internal_function_terms(SELF_AUDIT_SCRIPT_PATH)
    per_file_excluded_terms = {
        SELF_AUDIT_SCRIPT_PATH.resolve(): self_internal_function_terms,
    }

    docs_terms, docs_sources = collect_terms(docs_files, file_stems)
    code_terms, code_sources = collect_terms(
        code_files,
        file_stems,
        per_file_excluded_terms=per_file_excluded_terms,
    )

    docs_only = docs_terms - code_terms
    code_only = code_terms - docs_terms
    intersection = docs_terms & code_terms

    data_contracts_path = ROOT_DIR / "docs" / "DATA_CONTRACTS.md"
    not_filled_terms = collect_not_filled_terms_from_data_contracts(data_contracts_path)
    mysql_schema_fields = load_mysql_schema_fields(MYSQL_SNAPSHOT_PATH)

    docs_only_items, docs_advisories = pack_terms(
        docs_only,
        docs_sources,
        downgrade_terms=not_filled_terms,
        schema_fields=mysql_schema_fields,
    )
    code_only_items, code_advisories = pack_terms(code_only, code_sources)

    output = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "docs_only": docs_only_items,
        "code_only": code_only_items,
        "intersection": sorted(intersection),
        "summary": {
            "docs_only": {
                "total": len(docs_only_items),
                "risk": risk_summary(docs_only_items),
            },
            "code_only": {
                "total": len(code_only_items),
                "risk": risk_summary(code_only_items),
            },
            "intersection_total": len(intersection),
            "non_blocking_advisories_total": len(docs_advisories) + len(code_advisories),
        },
        "advisories": {
            "docs_only": docs_advisories,
            "code_only": code_advisories,
        },
        "config": {
            "docs_scope": [str(path).replace("\\", "/") for path in DOC_AUDIT_SCOPE],
            "code_scope": sorted(code_exts),
            "exclude_dirs": sorted(exclude_dirs),
            "ignored_doc_files": [],
            "not_filled_marker": NOT_FILLED_MARKER,
            "mysql_snapshot_path": str(MYSQL_SNAPSHOT_PATH.relative_to(ROOT_DIR)),
            "audit_meta_terms_filtered": sorted(AUDIT_META_TERMS),
            "self_internal_functions_filtered_count": len(self_internal_function_terms),
        },
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, ensure_ascii=True, indent=2), encoding="utf-8")

    print("Docs-only:", len(docs_only_items))
    print("Code-only:", len(code_only_items))
    print("Intersection:", len(intersection))
    print("Output:", output_path)


if __name__ == "__main__":
    main()
