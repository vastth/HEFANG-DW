from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from .binding_ops import patch_chart_bindings_for_workbook, validate_field_refs_for_workbook
from .models import WorkbookProfile
from .official_schema import validate_workbook_schema_for_file
from .profiler import build_workbook_profile


mcp = FastMCP("tableau-worksheet-mcp")

_active_workbook: WorkbookProfile | None = None


def _get_active_workbook() -> WorkbookProfile:
    if _active_workbook is None:
        raise RuntimeError("No active workbook. Call open_workbook_profile first.")
    return _active_workbook


@mcp.tool()
def open_workbook_profile(file_path: str) -> dict:
    """Open a TWB/TWBX and cache a workbook profile for later inspection."""

    global _active_workbook
    profile = build_workbook_profile(file_path)
    _active_workbook = profile

    return {
        "file_path": profile.file_path,
        "workbook_name": profile.workbook_name,
        "datasource_count": len(profile.datasource_profiles),
        "worksheet_count": len(profile.worksheet_profiles),
        "dashboard_count": len(profile.dashboard_names),
        "worksheets": {
            name: worksheet.datasource_names
            for name, worksheet in profile.worksheet_profiles.items()
        },
    }


@mcp.tool()
def list_datasources() -> list[dict]:
    """List datasource summaries for the active workbook."""

    profile = _get_active_workbook()
    return [
        {
            "name": datasource.name,
            "caption": datasource.caption,
            "has_connection": datasource.has_connection,
            "field_count": len(datasource.fields),
        }
        for datasource in profile.datasource_profiles.values()
    ]


@mcp.tool()
def get_worksheet_profile(worksheet_name: str) -> dict:
    """Return worksheet datasource bindings and current dependency summary."""

    profile = _get_active_workbook()
    worksheet = profile.worksheet_profiles.get(worksheet_name)
    if worksheet is None:
        raise ValueError(f"Worksheet not found: {worksheet_name}")
    return worksheet.to_dict()


@mcp.tool()
def list_fields(datasource_name: str = "", worksheet_name: str = "") -> dict:
    """List fields by datasource, never as a single flattened global field catalog."""

    profile = _get_active_workbook()

    if datasource_name:
        datasource = profile.datasource_profiles.get(datasource_name)
        if datasource is None:
            raise ValueError(f"Datasource not found: {datasource_name}")
        return {
            "datasource": datasource_name,
            "fields": [field.to_dict() for field in datasource.fields],
        }

    if worksheet_name:
        worksheet = profile.worksheet_profiles.get(worksheet_name)
        if worksheet is None:
            raise ValueError(f"Worksheet not found: {worksheet_name}")
        grouped: dict[str, list[dict]] = {}
        for bound_datasource_name in worksheet.datasource_names:
            datasource = profile.datasource_profiles.get(bound_datasource_name)
            if datasource is None:
                continue
            grouped[bound_datasource_name] = [field.to_dict() for field in datasource.fields]
        return {
            "worksheet": worksheet_name,
            "datasources": grouped,
        }

    return {
        "message": "Pass datasource_name or worksheet_name to inspect actual fields.",
        "datasources": [
            {
                "name": datasource.name,
                "caption": datasource.caption,
                "field_count": len(datasource.fields),
            }
            for datasource in profile.datasource_profiles.values()
        ],
    }


@mcp.tool()
def validate_field_refs(
    worksheet_name: str,
    datasource_name: str,
    field_refs: list[str] | None = None,
    rows_text: str = "",
    cols_text: str = "",
    encodings: dict[str, str] | None = None,
    dependency_fields: list[str] | None = None,
) -> dict:
    """Validate field references explicitly within one worksheet + datasource scope."""

    profile = _get_active_workbook()
    return validate_field_refs_for_workbook(
        workbook=profile,
        worksheet_name=worksheet_name,
        datasource_name=datasource_name,
        field_refs=field_refs,
        rows_text=rows_text,
        cols_text=cols_text,
        encodings=encodings,
        dependency_fields=dependency_fields,
    )


@mcp.tool()
def validate_workbook_schema(
    file_path: str = "",
    schema_path: str = "",
    allow_download: bool = True,
    max_errors: int = 20,
) -> dict:
    """Validate a TWB/TWBX against Tableau's official TWB XSD when the workbook version is supported."""

    target_path = file_path or _get_active_workbook().file_path
    return validate_workbook_schema_for_file(
        file_path=target_path,
        schema_path=schema_path,
        allow_download=allow_download,
        max_errors=max_errors,
    )


@mcp.tool()
def patch_chart_bindings(
    worksheet_name: str,
    datasource_name: str,
    rows_text: str = "",
    cols_text: str = "",
    mark_class: str = "",
    pane_index: int = 1,
    encodings: dict[str, str] | None = None,
    remove_encodings: list[str] | None = None,
    dependency_fields: list[str] | None = None,
    table_view_spec: dict | None = None,
    table_style_spec: dict | None = None,
    replace_panes: bool = False,
    pane_specs: list[dict] | None = None,
    output_path: str = "",
) -> dict:
    """Patch one worksheet's shelves, pane subtrees, and dependencies with minimal XML changes."""

    global _active_workbook
    profile = _get_active_workbook()
    result = patch_chart_bindings_for_workbook(
        workbook=profile,
        worksheet_name=worksheet_name,
        datasource_name=datasource_name,
        rows_text=rows_text,
        cols_text=cols_text,
        mark_class=mark_class,
        pane_index=pane_index,
        encodings=encodings,
        remove_encodings=remove_encodings,
        dependency_fields=dependency_fields,
        table_view_spec=table_view_spec,
        table_style_spec=table_style_spec,
        replace_panes=replace_panes,
        pane_specs=pane_specs,
        output_path=output_path,
    )
    _active_workbook = build_workbook_profile(result["saved_path"])
    return result


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
