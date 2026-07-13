from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
CONTEXT_CACHE_DIR = REPO_ROOT / "reports" / "context_cache"
JSON_PARSE_SIZE_LIMIT = 20 * 1024 * 1024


def resolve_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if not path.is_absolute():
        path = REPO_ROOT / path
    return path.resolve()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def truncate(value: object, max_chars: int = 120) -> str:
    text = "" if value is None else str(value)
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1] + "…"


def summarize_csv(path: Path, max_preview_rows: int) -> dict:
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
        reader = csv.reader(handle)
        rows = list()
        header = next(reader, [])
        row_count = 0
        for row in reader:
            row_count += 1
            if len(rows) < max_preview_rows:
                rows.append(row)

    return {
        "format": "csv",
        "row_count": row_count,
        "column_count": len(header),
        "columns": header,
        "preview_rows": [dict(zip(header, row)) if header else {"row": row} for row in rows],
    }


def summarize_json(path: Path, max_preview_rows: int) -> dict:
    if path.stat().st_size > JSON_PARSE_SIZE_LIMIT:
        return {
            "format": "json",
            "row_count": None,
            "column_count": None,
            "columns": [],
            "preview_rows": [],
            "note": "文件超过 JSON_PARSE_SIZE_LIMIT，未全量解析。",
        }

    data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    if isinstance(data, list):
        preview = data[:max_preview_rows]
        columns = sorted({key for item in preview if isinstance(item, dict) for key in item.keys()})
        return {
            "format": "json-list",
            "row_count": len(data),
            "column_count": len(columns),
            "columns": columns,
            "preview_rows": preview,
        }
    if isinstance(data, dict):
        keys = list(data.keys())
        return {
            "format": "json-object",
            "row_count": 1,
            "column_count": len(keys),
            "columns": keys,
            "preview_rows": [{key: data[key] for key in keys[:20]}],
        }
    return {
        "format": "json-scalar",
        "row_count": 1,
        "column_count": 1,
        "columns": ["value"],
        "preview_rows": [{"value": data}],
    }


def summarize_jsonl(path: Path, max_preview_rows: int) -> dict:
    preview = []
    row_count = 0
    columns: set[str] = set()
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if not line.strip():
                continue
            row_count += 1
            if len(preview) < max_preview_rows:
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    item = {"raw": line.strip()}
                if isinstance(item, dict):
                    columns.update(item.keys())
                preview.append(item)
    return {
        "format": "jsonl",
        "row_count": row_count,
        "column_count": len(columns),
        "columns": sorted(columns),
        "preview_rows": preview,
    }


def summarize_text(path: Path, max_preview_rows: int) -> dict:
    preview = []
    row_count = 0
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            row_count += 1
            if len(preview) < max_preview_rows:
                preview.append({"line": line.rstrip("\n")})
    return {
        "format": "text",
        "row_count": row_count,
        "column_count": 1,
        "columns": ["line"],
        "preview_rows": preview,
    }


def build_summary(path: Path, max_preview_rows: int = 5) -> dict:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        detail = summarize_csv(path, max_preview_rows)
    elif suffix == ".json":
        detail = summarize_json(path, max_preview_rows)
    elif suffix in {".jsonl", ".ndjson"}:
        detail = summarize_jsonl(path, max_preview_rows)
    else:
        detail = summarize_text(path, max_preview_rows)

    stat = path.stat()
    source_file = str(path.relative_to(REPO_ROOT)).replace("\\", "/") if path.is_relative_to(REPO_ROOT) else str(path)
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_file": source_file,
        "file_size_bytes": stat.st_size,
        "sha256": sha256_file(path),
        **detail,
    }


def render_markdown(summary: dict) -> str:
    lines = [
        "# Context Cache Summary",
        "",
        f"> 生成时间：{summary['generated_at']}",
        f"> 完整结果文件：{summary['source_file']}",
        "",
        "## 元数据",
        "",
        f"- 文件大小：{summary['file_size_bytes']} bytes",
        f"- SHA256：`{summary['sha256']}`",
        f"- 格式：{summary['format']}",
        f"- 行数：{summary.get('row_count')}",
        f"- 列数：{summary.get('column_count')}",
    ]
    if summary.get("note"):
        lines.append(f"- 备注：{summary['note']}")

    columns = summary.get("columns") or []
    if columns:
        lines.extend(["", "## 字段", "", ", ".join(f"`{truncate(column, 80)}`" for column in columns[:60])])

    preview_rows = summary.get("preview_rows") or []
    if preview_rows:
        lines.extend(["", "## 预览", "", "```json"])
        lines.append(json.dumps(preview_rows, ensure_ascii=False, indent=2, default=str))
        lines.append("```")

    lines.extend([
        "",
        "## 使用边界",
        "",
        "- 本摘要只用于压缩对话上下文，不替代完整结果文件。",
        "- 若需要完整证据，请定向读取 `完整结果文件` 或复查原始查询输出。",
        "",
    ])
    return "\n".join(lines)


def default_output_base(path: Path) -> Path:
    CONTEXT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if path.is_relative_to(CONTEXT_CACHE_DIR):
        return CONTEXT_CACHE_DIR / path.stem
    return CONTEXT_CACHE_DIR / path.name


def main() -> int:
    parser = argparse.ArgumentParser(description="为已落盘的大查询/大结果文件生成轻量上下文摘要。")
    parser.add_argument("input", help="已落盘结果文件，支持 CSV/JSON/JSONL/TXT。")
    parser.add_argument("--output-base", help="输出路径前缀；默认 reports/context_cache/<输入文件名>。")
    parser.add_argument("--max-preview-rows", type=int, default=5, help="摘要预览行数。")
    args = parser.parse_args()

    input_path = resolve_path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(input_path)

    output_base = resolve_path(args.output_base) if args.output_base else default_output_base(input_path)
    output_base.parent.mkdir(parents=True, exist_ok=True)

    summary = build_summary(input_path, max_preview_rows=args.max_preview_rows)
    json_path = output_base.with_suffix(".summary.json")
    md_path = output_base.with_suffix(".summary.md")
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    md_path.write_text(render_markdown(summary), encoding="utf-8")

    print(f"已写入 {json_path}")
    print(f"已写入 {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())