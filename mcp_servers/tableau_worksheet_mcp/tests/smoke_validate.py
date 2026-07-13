from __future__ import annotations

import json
import sys
from pathlib import Path


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from tableau_worksheet_mcp import server


def main() -> int:
    if len(sys.argv) < 4:
        print("Usage: python smoke_validate.py <path-to-twb-or-twbx> <worksheet-name> <datasource-name> [field-ref ...]")
        return 2

    workbook_path = sys.argv[1]
    worksheet_name = sys.argv[2]
    datasource_name = sys.argv[3]
    field_refs = sys.argv[4:]

    server.open_workbook_profile(workbook_path)
    payload = server.validate_field_refs(
        worksheet_name=worksheet_name,
        datasource_name=datasource_name,
        field_refs=field_refs,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
