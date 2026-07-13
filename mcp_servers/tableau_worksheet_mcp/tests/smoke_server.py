from __future__ import annotations

import json
import sys
from pathlib import Path


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from tableau_worksheet_mcp import server


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: python smoke_server.py <path-to-twb-or-twbx> <worksheet-name>")
        return 2

    workbook_path = sys.argv[1]
    worksheet_name = sys.argv[2]

    open_result = server.open_workbook_profile(workbook_path)
    worksheet_result = server.get_worksheet_profile(worksheet_name)
    fields_result = server.list_fields(worksheet_name=worksheet_name)

    payload = {
        "open": open_result,
        "worksheet": worksheet_result,
        "fields_summary": {
            datasource_name: len(fields)
            for datasource_name, fields in fields_result.get("datasources", {}).items()
        },
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
