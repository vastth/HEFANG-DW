from __future__ import annotations

import json
import sys
from pathlib import Path


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from tableau_worksheet_mcp import server


def main() -> int:
    if len(sys.argv) != 6:
        print(
            "Usage: python smoke_patch.py <path-to-twb-or-twbx> <worksheet-name> <datasource-name> <output-path> <patch-spec-json>"
        )
        return 2

    workbook_path = sys.argv[1]
    worksheet_name = sys.argv[2]
    datasource_name = sys.argv[3]
    output_path = sys.argv[4]
    patch_spec_path = Path(sys.argv[5]).resolve()

    patch_spec = json.loads(patch_spec_path.read_text(encoding="utf-8"))

    server.open_workbook_profile(workbook_path)
    payload = server.patch_chart_bindings(
        worksheet_name=worksheet_name,
        datasource_name=datasource_name,
        output_path=output_path,
        **patch_spec,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
