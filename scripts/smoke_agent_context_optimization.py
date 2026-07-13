from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = REPO_ROOT / "reports"
CONTEXT_CACHE_DIR = REPORTS_DIR / "context_cache"
SMOKE_JSON = REPORTS_DIR / "agent_context_optimization_smoke.json"
SMOKE_MD = REPORTS_DIR / "agent_context_optimization_smoke.md"


def read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def file_chars(relative_path: str) -> int:
    return len(read_text(REPO_ROOT / relative_path))


def git_show_chars(relative_path: str, ref: str = "main") -> int | None:
    try:
        completed = subprocess.run(
            ["git", "show", f"{ref}:{relative_path}"],
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    return len(completed.stdout)


def score_from_bool(value: bool, full: int = 100, empty: int = 0) -> int:
    return full if value else empty


def clamp_score(value: float) -> int:
    return max(0, min(100, round(value)))


def run_command(args: list[str]) -> dict:
    started = datetime.now()
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    completed = subprocess.run(
        args,
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
        timeout=120,
    )
    ended = datetime.now()
    return {
        "command": " ".join(args),
        "returncode": completed.returncode,
        "duration_seconds": round((ended - started).total_seconds(), 3),
        "stdout_tail": completed.stdout[-1000:],
        "stderr_tail": completed.stderr[-1000:],
    }


def smoke_context_cache_summary() -> dict:
    CONTEXT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    sample_path = CONTEXT_CACHE_DIR / "_smoke_query_result.csv"
    with sample_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["date_id", "metric", "amount"])
        writer.writeheader()
        writer.writerow({"date_id": "20260429", "metric": "sales", "amount": "123.45"})
        writer.writerow({"date_id": "20260429", "metric": "refund", "amount": "-10.00"})

    result = run_command([
        sys.executable,
        "scripts/summarize_context_cache.py",
        sample_path.relative_to(REPO_ROOT).as_posix(),
        "--max-preview-rows",
        "2",
    ])
    summary_json = CONTEXT_CACHE_DIR / "_smoke_query_result.summary.json"
    summary_md = CONTEXT_CACHE_DIR / "_smoke_query_result.summary.md"
    return {
        "sample_file": sample_path.relative_to(REPO_ROOT).as_posix(),
        "summary_json_exists": summary_json.exists(),
        "summary_md_exists": summary_md.exists(),
        "command": result,
    }


def build_smoke_report() -> dict:
    copilot_text = read_text(REPO_ROOT / ".github" / "copilot-instructions.md")
    agents_text = read_text(REPO_ROOT / "AGENTS.md")
    context_pack_text = read_text(REPO_ROOT / "reports" / "agent_context_summary.md")
    lessons_text = read_text(REPO_ROOT / "docs" / "AGENT_LESSONS.md")
    lesson_index_text = read_text(REPO_ROOT / "docs" / "AGENT_LESSONS_INDEX.md")
    meeting_text = read_text(REPO_ROOT / "docs" / "数云数据同步-子项目资料" / "superpowers内化会议纪要.md")
    clone_pack_text = read_text(REPO_ROOT / "docs" / "copilot_agent_clone_pack.md")

    baseline_copilot_chars = git_show_chars(".github/copilot-instructions.md")
    current_copilot_chars = len(copilot_text)
    current_agents_chars = len(agents_text)
    hcs = sorted(set(part.split(" ", 1)[0] for part in copilot_text.split("| HC-")[1:]))

    context_pack_run = run_command([sys.executable, "scripts/agent_context_pack.py", "--output", "reports/agent_context_summary.md"])
    lesson_index_run = run_command([sys.executable, "scripts/build_agent_lessons_index.py"])
    py_compile_run = run_command([
        sys.executable,
        "-m",
        "py_compile",
        "scripts/agent_context_pack.py",
        "scripts/build_agent_lessons_index.py",
        "scripts/summarize_context_cache.py",
        "scripts/copilot_post_edit_reminder.py",
        "scripts/copilot_session_close_reminder.py",
    ])
    cache_smoke = smoke_context_cache_summary()

    refreshed_context_pack = read_text(REPO_ROOT / "reports" / "agent_context_summary.md")
    refreshed_lesson_index = read_text(REPO_ROOT / "docs" / "AGENT_LESSONS_INDEX.md")

    governance_full_chars = sum(
        file_chars(path)
        for path in [
            "docs/AGENT_LESSONS.md",
            "docs/AGENT_HANDOFF.md",
            "docs/数云数据同步-子项目资料/superpowers内化会议纪要.md",
            "docs/copilot_agent_clone_pack.md",
            "docs/TODO_ISSUES.md",
        ]
    )
    context_pack_chars = len(refreshed_context_pack)
    lesson_ratio = len(refreshed_lesson_index) / max(1, len(lessons_text))
    context_pack_ratio = context_pack_chars / max(1, governance_full_chars)

    directions = []

    if baseline_copilot_chars:
        reduction = 1 - current_copilot_chars / baseline_copilot_chars
        score = clamp_score(70 + max(0, reduction) * 60)
    else:
        reduction = None
        score = 100 if current_copilot_chars <= 4000 else 75
    directions.append({
        "id": "D1",
        "name": "常驻规则压缩",
        "score": score,
        "status": "done" if score >= 90 else "partial",
        "evidence": {
            "current_copilot_chars": current_copilot_chars,
            "baseline_main_chars": baseline_copilot_chars,
            "reduction_ratio": reduction,
        },
    })

    hc_score = 100 if len(hcs) >= 10 and "唯一真值源" in copilot_text and "唯一真值源" in agents_text else 70
    directions.append({
        "id": "D2",
        "name": "硬约束 ID 化与入口去重",
        "score": hc_score,
        "status": "done" if hc_score >= 90 else "partial",
        "evidence": {"hard_constraint_count": len(hcs), "agents_chars": current_agents_chars},
    })

    cp_score = 100 if context_pack_run["returncode"] == 0 and context_pack_chars <= 9000 and "最新交接摘要" in refreshed_context_pack else 70
    directions.append({
        "id": "D3",
        "name": "轻量上下文入口",
        "score": cp_score,
        "status": "done" if cp_score >= 90 else "partial",
        "evidence": {"context_pack_chars": context_pack_chars, "context_pack_ratio_to_governance_docs": context_pack_ratio},
    })

    lesson_score = 100 if lesson_index_run["returncode"] == 0 and lesson_ratio <= 0.35 and "索引条目" in refreshed_lesson_index else 75
    directions.append({
        "id": "D4",
        "name": "经验台账索引化",
        "score": lesson_score,
        "status": "done" if lesson_score >= 90 else "partial",
        "evidence": {"lesson_index_chars": len(refreshed_lesson_index), "lesson_full_chars": len(lessons_text), "ratio": lesson_ratio},
    })

    minutes_score = 85 if "第四阶段：上下文压缩与防注入" in meeting_text and "上下文压缩与防注入层" in clone_pack_text else 50
    directions.append({
        "id": "D5",
        "name": "长会议纪要/迁移文档摘要化",
        "score": minutes_score,
        "status": "partial" if minutes_score < 90 else "done",
        "evidence": {"meeting_has_phase4": "第四阶段：上下文压缩与防注入" in meeting_text, "clone_pack_has_layer": "上下文压缩与防注入层" in clone_pack_text},
        "note": "已补当前状态与阶段摘要；尚未物理拆分全部历史长文。",
    })

    cache_readme_exists = (CONTEXT_CACHE_DIR / "README.md").exists()
    cache_score = 95 if cache_smoke["summary_json_exists"] and cache_smoke["summary_md_exists"] and cache_readme_exists and "reports/context_cache" in copilot_text else 75
    directions.append({
        "id": "D6",
        "name": "数据库大结果落盘摘要层",
        "score": cache_score,
        "status": "done" if cache_score >= 90 else "partial",
        "evidence": {**cache_smoke, "context_cache_readme_exists": cache_readme_exists},
    })

    agent_files = list((REPO_ROOT / ".github" / "agents").glob("*.agent.md")) if (REPO_ROOT / ".github" / "agents").exists() else []
    agent_score = 80 if len(agent_files) >= 5 else 45
    directions.append({
        "id": "D7",
        "name": "子代理承担大范围探索",
        "score": agent_score,
        "status": "partial",
        "evidence": {"github_agent_count": len(agent_files), "agent_files": [path.relative_to(REPO_ROOT).as_posix() for path in agent_files[:10]]},
        "note": "结构具备；自然语言自动路由仍需运行时观察。",
    })

    overall_score = round(sum(item["score"] for item in directions) / len(directions), 1)
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "overall_score": overall_score,
        "positive_effect": {
            "governance_full_chars": governance_full_chars,
            "context_pack_chars": context_pack_chars,
            "context_pack_ratio_to_governance_docs": context_pack_ratio,
            "lesson_index_ratio_to_full_lessons": lesson_ratio,
            "copilot_instruction_current_chars": current_copilot_chars,
            "copilot_instruction_main_chars": baseline_copilot_chars,
        },
        "directions": directions,
        "smoke_commands": {
            "context_pack": context_pack_run,
            "lesson_index": lesson_index_run,
            "py_compile": py_compile_run,
        },
    }


def render_markdown(report: dict) -> str:
    lines = [
        "# Agent Context Optimization Smoke Report",
        "",
        f"> 生成时间：{report['generated_at']}",
        f"> 总体得分：{report['overall_score']} / 100",
        "",
        "## 正面效果指标",
        "",
    ]
    for key, value in report["positive_effect"].items():
        lines.append(f"- {key}: {value}")

    lines.extend(["", "## 七方向进度", "", "| ID | 方向 | 状态 | 得分 | 证据摘要 |", "|---|---|---|---:|---|"])
    for item in report["directions"]:
        evidence = item.get("evidence", {})
        summary = "; ".join(f"{key}={value}" for key, value in evidence.items() if key not in {"command", "agent_files"})
        if item.get("note"):
            summary += f"；{item['note']}"
        lines.append(f"| {item['id']} | {item['name']} | {item['status']} | {item['score']} | {summary} |")

    lines.extend(["", "## 烟测命令", ""])
    for name, command in report["smoke_commands"].items():
        lines.append(f"- {name}: returncode={command['returncode']}, duration={command['duration_seconds']}s")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="烟测 Agent 上下文优化是否产生正向效果。")
    parser.add_argument("--json-output", default=str(SMOKE_JSON.relative_to(REPO_ROOT)))
    parser.add_argument("--md-output", default=str(SMOKE_MD.relative_to(REPO_ROOT)))
    args = parser.parse_args()

    report = build_smoke_report()
    json_output = REPO_ROOT / args.json_output
    md_output = REPO_ROOT / args.md_output
    json_output.parent.mkdir(parents=True, exist_ok=True)
    md_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    md_output.write_text(render_markdown(report), encoding="utf-8")
    print(f"已写入 {json_output}")
    print(f"已写入 {md_output}")
    print(f"overall_score={report['overall_score']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())