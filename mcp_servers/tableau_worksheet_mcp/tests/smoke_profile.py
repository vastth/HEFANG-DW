from __future__ import annotations

import json
import sys
from pathlib import Path


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from tableau_worksheet_mcp.profiler import build_workbook_profile


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python smoke_profile.py <path-to-twb-or-twbx>")
        return 2

    workbook_path = Path(sys.argv[1]).resolve()
    profile = build_workbook_profile(str(workbook_path))

    payload = {
        "file_path": profile.file_path,
        "workbook_name": profile.workbook_name,
        "datasource_count": len(profile.datasource_profiles),
        "worksheet_count": len(profile.worksheet_profiles),
        "dashboard_count": len(profile.dashboard_names),
        "datasources": {
            name: {
                "caption": datasource.caption,
                "has_connection": datasource.has_connection,
                "field_count": len(datasource.fields),
            }
            for name, datasource in profile.datasource_profiles.items()
        },
        "worksheets": {
            name: worksheet.datasource_names
            for name, worksheet in profile.worksheet_profiles.items()
        },
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
