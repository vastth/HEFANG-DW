from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from zipfile import ZipFile
import xml.etree.ElementTree as ET


@dataclass
class WorkbookXmlDocument:
    source_path: Path
    root: ET.Element
    twbx_twb_name: str | None = None


def load_workbook_xml(file_path: str | Path) -> WorkbookXmlDocument:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Workbook not found: {path}")

    if path.suffix.lower() == ".twbx":
        with ZipFile(path) as archive:
            twb_names = [name for name in archive.namelist() if name.lower().endswith(".twb")]
            if not twb_names:
                raise ValueError(f"No .twb found inside {path}")
            twb_name = twb_names[0]
            root = ET.fromstring(archive.read(twb_name))
        return WorkbookXmlDocument(source_path=path, root=root, twbx_twb_name=twb_name)

    root = ET.parse(path).getroot()
    return WorkbookXmlDocument(source_path=path, root=root)


def save_workbook_xml(document: WorkbookXmlDocument, output_path: str | Path = "") -> Path:
    target_path = Path(output_path) if output_path else document.source_path
    target_path.parent.mkdir(parents=True, exist_ok=True)

    xml_bytes = ET.tostring(document.root, encoding="utf-8", xml_declaration=True)

    if document.twbx_twb_name is None:
        target_path.write_bytes(xml_bytes)
        return target_path

    with ZipFile(document.source_path, "r") as source_archive, ZipFile(target_path, "w") as target_archive:
        for info in source_archive.infolist():
            payload = xml_bytes if info.filename == document.twbx_twb_name else source_archive.read(info.filename)
            target_archive.writestr(info, payload)
    return target_path


def find_worksheet(root: ET.Element, worksheet_name: str) -> ET.Element:
    container = root.find("worksheets")
    if container is None:
        raise ValueError("Workbook is missing <worksheets> container")
    for worksheet in container.findall("worksheet"):
        if worksheet.get("name") == worksheet_name:
            return worksheet
    raise ValueError(f"Worksheet not found: {worksheet_name}")
