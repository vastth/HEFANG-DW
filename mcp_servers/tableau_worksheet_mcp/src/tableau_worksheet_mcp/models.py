from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass
class FieldProfile:
    display_name: str
    local_name: str
    datatype: str
    role: str
    field_type: str
    is_calculated: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class DatasourceProfile:
    name: str
    caption: str
    has_connection: bool
    fields: list[FieldProfile] = field(default_factory=list)

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["field_count"] = len(self.fields)
        return payload


@dataclass
class WorksheetProfile:
    name: str
    datasource_names: list[str] = field(default_factory=list)
    dependency_fields: dict[str, list[str]] = field(default_factory=dict)
    rows_text: str = ""
    cols_text: str = ""
    mark_classes: list[str] = field(default_factory=list)
    encoding_tags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class WorkbookProfile:
    file_path: str
    workbook_name: str
    datasource_profiles: dict[str, DatasourceProfile] = field(default_factory=dict)
    worksheet_profiles: dict[str, WorksheetProfile] = field(default_factory=dict)
    dashboard_names: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "file_path": self.file_path,
            "workbook_name": self.workbook_name,
            "datasource_count": len(self.datasource_profiles),
            "worksheet_count": len(self.worksheet_profiles),
            "dashboard_count": len(self.dashboard_names),
            "datasources": {
                name: profile.to_dict()
                for name, profile in self.datasource_profiles.items()
            },
            "worksheets": {
                name: profile.to_dict()
                for name, profile in self.worksheet_profiles.items()
            },
            "dashboards": list(self.dashboard_names),
        }
