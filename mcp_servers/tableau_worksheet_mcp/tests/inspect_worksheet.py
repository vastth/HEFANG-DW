from __future__ import annotations

import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def _element_to_dict(element: ET.Element) -> dict:
    return {
        "tag": element.tag,
        "attrs": dict(element.attrib),
        "text": (element.text or "").strip(),
        "children": [_element_to_dict(child) for child in list(element)],
    }


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: python inspect_worksheet.py <path-to-twb-or-twbx> <worksheet-name>")
        return 2

    workbook_path = Path(sys.argv[1]).resolve()
    worksheet_name = sys.argv[2]

    root = ET.parse(workbook_path).getroot()
    worksheets = root.find("worksheets")
    if worksheets is None:
        raise ValueError("Workbook is missing <worksheets>")

    worksheet = None
    for candidate in worksheets.findall("worksheet"):
        if candidate.get("name") == worksheet_name:
            worksheet = candidate
            break
    if worksheet is None:
        raise ValueError(f"Worksheet not found: {worksheet_name}")

    table = worksheet.find("table")
    if table is None:
        raise ValueError(f"Worksheet is missing <table>: {worksheet_name}")

    panes_parent = table.find("panes")
    panes = [] if panes_parent is None else panes_parent.findall("pane")
    table_style = table.find("style")
    axis_rules = []
    if table_style is not None:
        axis_rules = [
            _element_to_dict(style_rule)
            for style_rule in table_style.findall("style-rule")
            if style_rule.get("element") == "axis"
        ]

    view = table.find("view")
    dependencies = []
    if view is not None:
        for dependency in view.findall("datasource-dependencies"):
            dependencies.append(
                {
                    "datasource": dependency.get("datasource"),
                    "columns": [
                        {
                            "name": column.get("name"),
                            "caption": column.get("caption"),
                            "datatype": column.get("datatype"),
                            "role": column.get("role"),
                            "type": column.get("type"),
                        }
                        for column in dependency.findall("column")
                    ],
                    "instances": [
                        {
                            "name": instance.get("name"),
                            "column": instance.get("column"),
                            "derivation": instance.get("derivation"),
                            "type": instance.get("type"),
                        }
                        for instance in dependency.findall("column-instance")
                    ],
                }
            )

    payload = {
        "worksheet_name": worksheet_name,
        "table_children": [child.tag for child in list(table)],
        "rows": (table.findtext("rows") or "").strip(),
        "cols": (table.findtext("cols") or "").strip(),
        "panes_parent": None if panes_parent is None else panes_parent.tag,
        "pane_count": len(panes),
        "panes": [_element_to_dict(pane) for pane in panes],
        "axis_rules": axis_rules,
        "dependencies": dependencies,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
