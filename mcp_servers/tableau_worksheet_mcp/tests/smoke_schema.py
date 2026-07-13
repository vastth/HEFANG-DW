from __future__ import annotations

import argparse
import json

from tableau_worksheet_mcp.official_schema import validate_workbook_schema_for_file


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Smoke test Tableau official TWB schema validation."
    )
    parser.add_argument("workbook", help="Path to a .twb or .twbx file")
    parser.add_argument("--schema-path", default="", help="Optional local XSD path")
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Do not download missing official schemas into the local cache",
    )
    parser.add_argument("--max-errors", type=int, default=5)
    args = parser.parse_args()

    result = validate_workbook_schema_for_file(
        args.workbook,
        schema_path=args.schema_path,
        allow_download=not args.no_download,
        max_errors=args.max_errors,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()