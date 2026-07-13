from __future__ import annotations

import xml.etree.ElementTree as ET

from .models import DatasourceProfile, FieldProfile, WorkbookProfile, WorksheetProfile
from .workbook_xml import load_workbook_xml


_NUMERIC_REMOTE_TYPES = {"5", "4", "131", "20", "3", "2", "14", "6", "7"}


def build_workbook_profile(file_path: str) -> WorkbookProfile:
    document = load_workbook_xml(file_path)
    path = document.source_path
    root = document.root

    workbook = WorkbookProfile(
        file_path=str(path),
        workbook_name=path.name,
        datasource_profiles=_parse_datasources(root),
        worksheet_profiles=_parse_worksheets(root),
        dashboard_names=_parse_dashboard_names(root),
    )
    return workbook
def _parse_datasources(root: ET.Element) -> dict[str, DatasourceProfile]:
    datasources: dict[str, DatasourceProfile] = {}
    container = root.find("datasources")
    if container is None:
        return datasources

    for datasource in container.findall("datasource"):
        name = datasource.get("name", "")
        if not name:
            continue
        profile = DatasourceProfile(
            name=name,
            caption=datasource.get("caption", name),
            has_connection=datasource.get("hasconnection") != "false",
            fields=_parse_fields(datasource),
        )
        datasources[name] = profile
    return datasources


def _parse_fields(datasource: ET.Element) -> list[FieldProfile]:
    fields_by_name: dict[str, FieldProfile] = {}

    for metadata in datasource.findall(".//metadata-records/metadata-record"):
        if metadata.get("class", "") != "column":
            continue

        display_name = (metadata.findtext("remote-name") or "").strip()
        local_name = (metadata.findtext("local-name") or "").strip()
        datatype = (metadata.findtext("local-type") or "string").strip()
        remote_type = (metadata.findtext("remote-type") or "0").strip()
        if not display_name or not local_name:
            continue

        if remote_type in _NUMERIC_REMOTE_TYPES:
            role = "measure"
            field_type = "quantitative"
        else:
            role = "dimension"
            field_type = "nominal"

        fields_by_name[display_name] = FieldProfile(
            display_name=display_name,
            local_name=local_name,
            datatype=datatype,
            role=role,
            field_type=field_type,
            is_calculated=False,
        )

    for column in datasource.findall("column"):
        local_name = (column.get("name") or "").strip()
        display_name = (column.get("caption") or local_name.strip("[]")).strip()
        if not local_name or not display_name:
            continue

        fields_by_name.setdefault(
            display_name,
            FieldProfile(
                display_name=display_name,
                local_name=local_name,
                datatype=column.get("datatype", "string"),
                role=column.get("role", "dimension"),
                field_type=column.get("type", "nominal"),
                is_calculated=column.find("calculation") is not None,
            ),
        )

    return sorted(fields_by_name.values(), key=lambda item: item.display_name.lower())


def _parse_worksheets(root: ET.Element) -> dict[str, WorksheetProfile]:
    worksheets: dict[str, WorksheetProfile] = {}
    container = root.find("worksheets")
    if container is None:
        return worksheets

    for worksheet in container.findall("worksheet"):
        name = worksheet.get("name", "")
        if not name:
            continue

        rows_text = (worksheet.findtext("table/rows") or "").strip()
        cols_text = (worksheet.findtext("table/cols") or "").strip()
        mark_classes = _unique_preserve_order(
            mark.get("class", "")
            for mark in worksheet.findall(".//pane/mark")
            if mark.get("class")
        )
        encoding_tags = _unique_preserve_order(
            encoding.tag
            for encoding in worksheet.findall(".//pane/encodings/*")
        )

        dependency_fields: dict[str, list[str]] = {}
        datasource_names: list[str] = []
        for dep in worksheet.findall(".//table/view/datasource-dependencies"):
            datasource_name = (dep.get("datasource") or "").strip()
            if not datasource_name:
                continue
            datasource_names.append(datasource_name)
            field_names = dependency_fields.setdefault(datasource_name, [])
            for column in dep.findall("column"):
                field_name = _normalize_field_name(column.get("caption") or column.get("name") or "")
                if field_name and field_name not in field_names:
                    field_names.append(field_name)

        worksheets[name] = WorksheetProfile(
            name=name,
            datasource_names=_unique_preserve_order(datasource_names),
            dependency_fields=dependency_fields,
            rows_text=rows_text,
            cols_text=cols_text,
            mark_classes=mark_classes,
            encoding_tags=encoding_tags,
        )
    return worksheets


def _parse_dashboard_names(root: ET.Element) -> list[str]:
    container = root.find("dashboards")
    if container is None:
        return []
    names = []
    for dashboard in container.findall("dashboard"):
        name = dashboard.get("name", "")
        if name:
            names.append(name)
    return names


def _normalize_field_name(value: str) -> str:
    text = value.strip()
    if text.startswith("[") and text.endswith("]"):
        return text[1:-1]
    return text


def _unique_preserve_order(values) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered
