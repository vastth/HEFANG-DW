from __future__ import annotations

import re
import xml.etree.ElementTree as ET

from .models import DatasourceProfile, FieldProfile, WorkbookProfile
from .profiler import build_workbook_profile
from .workbook_xml import find_worksheet, load_workbook_xml, save_workbook_xml


_INSTANCE_REF_RE = re.compile(r"\[([^\]]+)\]\.\[([^\]]+)\]")
_AGGREGATION_RE = re.compile(r"^[A-Z]+\((.+)\)$")
_PSEUDO_FIELD_NAMES = {"Multiple Values", "Measure Names", "Measure Values", ":Measure Names"}
_DERIVATION_BY_ABBR = {
    "none": "None",
    "usr": "User",
    "sum": "Sum",
    "avg": "Avg",
    "cnt": "Count",
    "cntd": "CountD",
    "min": "Min",
    "max": "Max",
    "med": "Median",
    "attr": "Attr",
    "yr": "Year",
    "qr": "Quarter",
    "mn": "Month",
    "day": "Day",
    "wk": "Week",
    "wd": "Weekday",
    "my": "MY",
    "tdy": "Day-Trunc",
}
_COLUMN_INSTANCE_TYPE_BY_SUFFIX = {
    "nk": "nominal",
    "qk": "quantitative",
    "ok": "ordinal",
}


def validate_field_refs_for_workbook(
    workbook: WorkbookProfile,
    worksheet_name: str,
    datasource_name: str,
    field_refs: list[str] | None = None,
    rows_text: str = "",
    cols_text: str = "",
    encodings: dict[str, str] | None = None,
    dependency_fields: list[str] | None = None,
) -> dict:
    worksheet = workbook.worksheet_profiles.get(worksheet_name)
    if worksheet is None:
        raise ValueError(f"Worksheet not found: {worksheet_name}")

    resolved_datasource_name, datasource = _resolve_datasource(workbook, datasource_name)
    worksheet_bound = resolved_datasource_name in worksheet.datasource_names

    valid_fields: list[dict] = []
    invalid_fields: list[dict] = []
    skipped_inputs: list[str] = []
    seen_valid_pairs: set[tuple[str, str]] = set()

    raw_inputs: list[str] = []
    raw_inputs.extend(field_refs or [])
    raw_inputs.extend(dependency_fields or [])
    if rows_text:
        raw_inputs.append(rows_text)
    if cols_text:
        raw_inputs.append(cols_text)
    for value in (encodings or {}).values():
        if value:
            raw_inputs.append(value)

    for raw_input in raw_inputs:
        tokens, input_invalids, skipped = _extract_field_tokens(raw_input, resolved_datasource_name)
        invalid_fields.extend({"input": raw_input, **item} for item in input_invalids)
        if skipped:
            skipped_inputs.append(raw_input)
        for token in tokens:
            field = _resolve_field(datasource, token)
            if field is None:
                invalid_fields.append(
                    {
                        "input": raw_input,
                        "field": token,
                        "reason": f"Field not found in datasource {resolved_datasource_name}",
                    }
                )
                continue
            dedupe_key = (raw_input, field.local_name)
            if dedupe_key in seen_valid_pairs:
                continue
            seen_valid_pairs.add(dedupe_key)
            valid_fields.append(
                {
                    "input": raw_input,
                    "field": field.display_name,
                    "local_name": field.local_name,
                    "datatype": field.datatype,
                    "role": field.role,
                    "field_type": field.field_type,
                }
            )

    return {
        "worksheet_name": worksheet_name,
        "datasource_name": resolved_datasource_name,
        "datasource_caption": datasource.caption,
        "worksheet_datasources": list(worksheet.datasource_names),
        "worksheet_bound_to_datasource": worksheet_bound,
        "valid_fields": valid_fields,
        "invalid_fields": invalid_fields,
        "skipped_inputs": skipped_inputs,
        "is_valid": worksheet_bound and not invalid_fields,
    }


def patch_chart_bindings_for_workbook(
    workbook: WorkbookProfile,
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
    pane_specs = pane_specs or []
    spec_binding_inputs: list[str] = []
    spec_binding_inputs.extend(_collect_binding_strings(table_view_spec))
    spec_binding_inputs.extend(_collect_binding_strings(table_style_spec))
    for pane_spec in pane_specs:
        spec_binding_inputs.extend(_collect_binding_strings(pane_spec))

    validation = validate_field_refs_for_workbook(
        workbook=workbook,
        worksheet_name=worksheet_name,
        datasource_name=datasource_name,
        field_refs=spec_binding_inputs,
        rows_text=rows_text,
        cols_text=cols_text,
        encodings=encodings,
        dependency_fields=dependency_fields,
    )
    if not validation["worksheet_bound_to_datasource"]:
        raise ValueError(
            f"Worksheet '{worksheet_name}' is not bound to datasource '{validation['datasource_name']}'"
        )
    if validation["invalid_fields"]:
        raise ValueError(f"Field validation failed: {validation['invalid_fields']}")

    resolved_datasource_name = validation["datasource_name"]
    datasource = workbook.datasource_profiles[resolved_datasource_name]
    document = load_workbook_xml(workbook.file_path)
    worksheet = find_worksheet(document.root, worksheet_name)

    table = worksheet.find("table")
    if table is None:
        raise ValueError(f"Worksheet '{worksheet_name}' is malformed: missing <table>")
    view = table.find("view")
    if view is None:
        raise ValueError(f"Worksheet '{worksheet_name}' is malformed: missing <view>")

    dependency = _find_or_create_datasource_dependency(view, resolved_datasource_name)

    required_field_tokens = list(dependency_fields or [])
    raw_bindings: list[str] = [rows_text, cols_text]
    raw_bindings.extend(value for value in (encodings or {}).values() if value)
    raw_bindings.extend(spec_binding_inputs)
    for raw_binding in raw_bindings:
        if not raw_binding:
            continue
        required_field_tokens.extend(_extract_required_field_tokens(raw_binding, resolved_datasource_name))

    for token in required_field_tokens:
        field = _resolve_field(datasource, token)
        if field is None:
            continue
        _ensure_dependency_column(dependency, field)

    for spec in _extract_instance_specs(rows_text, resolved_datasource_name):
        field = _resolve_field(datasource, spec["field_token"])
        if field is not None:
            _ensure_dependency_column(dependency, field)
            _ensure_dependency_column_instance(dependency, field, spec)

    for spec in _extract_instance_specs(cols_text, resolved_datasource_name):
        field = _resolve_field(datasource, spec["field_token"])
        if field is not None:
            _ensure_dependency_column(dependency, field)
            _ensure_dependency_column_instance(dependency, field, spec)

    for encoding_value in (encodings or {}).values():
        for spec in _extract_instance_specs(encoding_value or "", resolved_datasource_name):
            field = _resolve_field(datasource, spec["field_token"])
            if field is not None:
                _ensure_dependency_column(dependency, field)
                _ensure_dependency_column_instance(dependency, field, spec)

    for raw_binding in spec_binding_inputs:
        for spec in _extract_instance_specs(raw_binding, resolved_datasource_name):
            field = _resolve_field(datasource, spec["field_token"])
            if field is not None:
                _ensure_dependency_column(dependency, field)
                _ensure_dependency_column_instance(dependency, field, spec)

    if rows_text:
        rows_element = _find_or_create_table_child(table, "rows")
        rows_element.text = rows_text

    if cols_text:
        cols_element = _find_or_create_table_child(table, "cols")
        cols_element.text = cols_text

    if table_view_spec is not None:
        _replace_or_create_direct_child_from_spec(table, "view", table_view_spec)

    if table_style_spec is not None:
        _replace_or_create_direct_child_from_spec(table, "style", table_style_spec)

    pane_summaries: list[dict] = []
    if replace_panes or pane_specs:
        panes = _replace_or_create_panes(table, pane_specs or [], replace_panes=replace_panes)
        for spec, pane in zip(pane_specs, panes):
            _apply_pane_spec(pane, spec)
        pane_summaries = _summarize_panes(table)
    else:
        pane = _get_pane(worksheet, pane_index)

        if mark_class:
            mark = pane.find("mark")
            if mark is None:
                mark = ET.SubElement(pane, "mark")
            mark.set("class", mark_class)

        if encodings:
            encodings_element = pane.find("encodings")
            if encodings_element is None:
                encodings_element = ET.SubElement(pane, "encodings")
            for tag_name, column_value in encodings.items():
                _upsert_encoding(encodings_element, tag_name, column_value)

        for tag_name in remove_encodings or []:
            encodings_element = pane.find("encodings")
            if encodings_element is not None:
                for child in list(encodings_element):
                    if child.tag == tag_name:
                        encodings_element.remove(child)
        pane_summaries = _summarize_panes(table)

    saved_path = save_workbook_xml(document, output_path)
    refreshed = build_workbook_profile(str(saved_path))
    updated_worksheet = refreshed.worksheet_profiles[worksheet_name]
    return {
        "saved_path": str(saved_path),
        "worksheet_name": worksheet_name,
        "datasource_name": resolved_datasource_name,
        "updated_rows_text": updated_worksheet.rows_text,
        "updated_cols_text": updated_worksheet.cols_text,
        "updated_mark_classes": updated_worksheet.mark_classes,
        "updated_encoding_tags": updated_worksheet.encoding_tags,
        "updated_pane_count": len(pane_summaries),
        "updated_panes": pane_summaries,
        "validation": validation,
    }


def _resolve_datasource(workbook: WorkbookProfile, datasource_name: str) -> tuple[str, DatasourceProfile]:
    direct = workbook.datasource_profiles.get(datasource_name)
    if direct is not None:
        return datasource_name, direct

    normalized = datasource_name.casefold()
    for name, datasource in workbook.datasource_profiles.items():
        if datasource.caption.casefold() == normalized:
            return name, datasource
    raise ValueError(f"Datasource not found: {datasource_name}")


def _resolve_field(datasource: DatasourceProfile, token: str) -> FieldProfile | None:
    normalized = token.strip().strip("[]")
    if not normalized or normalized in _PSEUDO_FIELD_NAMES:
        return None
    target = normalized.casefold()
    for field in datasource.fields:
        local_normalized = field.local_name.strip().strip("[]").casefold()
        display_normalized = field.display_name.strip().casefold()
        if target in {local_normalized, display_normalized}:
            return field
    return None


def _extract_field_tokens(raw_input: str, expected_datasource_name: str) -> tuple[list[str], list[dict], bool]:
    text = str(raw_input or "").strip()
    if not text:
        return [], [], True

    tokens: list[str] = []
    invalids: list[dict] = []
    saw_match = False

    for datasource_name, instance_name in _INSTANCE_REF_RE.findall(text):
        saw_match = True
        if datasource_name != expected_datasource_name:
            invalids.append(
                {
                    "field": instance_name,
                    "reason": f"Binding references datasource {datasource_name}, expected {expected_datasource_name}",
                }
            )
            continue
        field_token = _field_token_from_instance_name(instance_name)
        if field_token is not None:
            tokens.append(field_token)

    if saw_match:
        return _dedupe(tokens), invalids, not tokens and not invalids

    match = _AGGREGATION_RE.match(text)
    if match:
        inner = match.group(1).strip()
        inner_tokens, inner_invalids, skipped = _extract_field_tokens(inner, expected_datasource_name)
        return inner_tokens, inner_invalids, skipped

    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1].strip()

    if text in _PSEUDO_FIELD_NAMES:
        return [], [], True

    return [text], [], False


def _extract_required_field_tokens(raw_binding: str, expected_datasource_name: str) -> list[str]:
    tokens, _, _ = _extract_field_tokens(raw_binding, expected_datasource_name)
    return tokens


def _field_token_from_instance_name(instance_name: str) -> str | None:
    cleaned = instance_name.strip().strip("[]")
    if not cleaned or cleaned in _PSEUDO_FIELD_NAMES:
        return None
    parts = cleaned.split(":")
    if len(parts) >= 3:
        token = ":".join(parts[1:-1]).strip()
        return None if token in _PSEUDO_FIELD_NAMES else token
    return cleaned


def _extract_instance_specs(raw_binding: str, expected_datasource_name: str) -> list[dict]:
    specs: list[dict] = []
    for datasource_name, instance_name in _INSTANCE_REF_RE.findall(str(raw_binding or "")):
        if datasource_name != expected_datasource_name:
            continue
        field_token = _field_token_from_instance_name(instance_name)
        if field_token is None:
            continue
        cleaned = instance_name.strip().strip("[]")
        parts = cleaned.split(":")
        derivation_abbr = parts[0] if len(parts) >= 3 else "none"
        type_suffix = parts[-1] if len(parts) >= 3 else "qk"
        specs.append(
            {
                "instance_name": f"[{cleaned}]",
                "field_token": field_token,
                "derivation": _DERIVATION_BY_ABBR.get(derivation_abbr, "None"),
                "ci_type": _COLUMN_INSTANCE_TYPE_BY_SUFFIX.get(type_suffix, "quantitative"),
                "pivot": "key",
            }
        )
    return specs


def _get_pane(worksheet: ET.Element, pane_index: int) -> ET.Element:
    table = worksheet.find("table")
    if table is None:
        raise ValueError(f"Worksheet '{worksheet.get('name')}' is malformed: missing <table>")
    panes_parent = _get_panes_parent(table)
    panes = panes_parent.findall("pane")
    if pane_index < 1 or pane_index > len(panes):
        raise ValueError(
            f"Pane index {pane_index} is out of range for worksheet '{worksheet.get('name')}'. Available panes: {len(panes)}"
        )
    return panes[pane_index - 1]


def _replace_or_create_panes(table: ET.Element, pane_specs: list[dict], replace_panes: bool) -> list[ET.Element]:
    panes_parent = _get_or_create_panes_parent(table)
    existing_direct_panes = [child for child in list(table) if child.tag == "pane"]
    for pane in existing_direct_panes:
        table.remove(pane)

    existing_panes = list(panes_parent.findall("pane"))
    if replace_panes:
        for pane in existing_panes:
            panes_parent.remove(pane)
        existing_panes = []

    panes = existing_panes
    while len(panes) < len(pane_specs):
        pane = ET.Element("pane")
        panes_parent.append(pane)
        panes = list(panes_parent.findall("pane"))
    return panes[: len(pane_specs)] if pane_specs else panes


def _apply_pane_spec(pane: ET.Element, spec: dict) -> None:
    for key in list(pane.attrib.keys()):
        pane.attrib.pop(key, None)
    for key, value in (spec.get("attrs") or {}).items():
        pane.set(key, str(value))

    replaced_children = "child_specs" in spec
    if replaced_children:
        for child in list(pane):
            pane.remove(child)
        for child_spec in spec.get("child_specs") or []:
            pane.append(_build_element_from_spec(child_spec))

    mark_class = spec.get("mark_class", "")
    existing_mark = pane.find("mark")
    if mark_class:
        if existing_mark is None:
            existing_mark = ET.SubElement(pane, "mark")
        existing_mark.set("class", mark_class)
    elif existing_mark is not None and not replaced_children and spec.get("mark_spec") is None:
        pane.remove(existing_mark)

    encodings_element = pane.find("encodings")
    if spec.get("clear_encodings"):
        if encodings_element is not None:
            pane.remove(encodings_element)
        encodings_element = None

    pane_encodings = spec.get("encodings") or {}
    if pane_encodings:
        if encodings_element is None:
            encodings_element = ET.SubElement(pane, "encodings")
        for tag_name, column_value in pane_encodings.items():
            _upsert_encoding(encodings_element, tag_name, column_value)

    for tag_name in spec.get("remove_encodings") or []:
        if encodings_element is None:
            continue
        for child in list(encodings_element):
            if child.tag == tag_name:
                encodings_element.remove(child)

    if spec.get("view_spec") is not None:
        _replace_or_create_direct_child_from_spec(pane, "view", spec["view_spec"])

    if spec.get("mark_spec") is not None:
        _replace_or_create_direct_child_from_spec(pane, "mark", spec["mark_spec"])

    if spec.get("encodings_spec") is not None:
        _replace_or_create_direct_child_from_spec(pane, "encodings", spec["encodings_spec"])

    if spec.get("customized_tooltip_spec") is not None:
        _replace_or_create_direct_child_from_spec(pane, "customized-tooltip", spec["customized_tooltip_spec"])

    if spec.get("customized_label_spec") is not None:
        _replace_or_create_direct_child_from_spec(pane, "customized-label", spec["customized_label_spec"])

    if spec.get("style_spec") is not None:
        _replace_or_create_direct_child_from_spec(pane, "style", spec["style_spec"])


def _summarize_panes(table: ET.Element) -> list[dict]:
    panes_parent = _get_panes_parent(table)
    summaries: list[dict] = []
    for index, pane in enumerate(panes_parent.findall("pane"), start=1):
        mark = pane.find("mark")
        encodings_element = pane.find("encodings")
        summaries.append(
            {
                "pane_index": index,
                "attrs": dict(pane.attrib),
                "child_tags": [child.tag for child in list(pane)],
                "mark_class": mark.get("class") if mark is not None else None,
                "has_customized_tooltip": pane.find("customized-tooltip") is not None,
                "has_customized_label": pane.find("customized-label") is not None,
                "encodings": []
                if encodings_element is None
                else [(child.tag, child.get("column")) for child in encodings_element],
            }
        )
    return summaries


def _get_panes_parent(table: ET.Element) -> ET.Element:
    panes_parent = table.find("panes")
    if panes_parent is not None:
        return panes_parent
    return table


def _get_or_create_panes_parent(table: ET.Element) -> ET.Element:
    panes_parent = table.find("panes")
    if panes_parent is not None:
        return panes_parent

    panes_parent = ET.Element("panes")
    insert_index = len(list(table))
    for index, child in enumerate(list(table)):
        if child.tag in {"rows", "cols"}:
            insert_index = index
            break
    table.insert(insert_index, panes_parent)
    return panes_parent


def _find_or_create_datasource_dependency(view: ET.Element, datasource_name: str) -> ET.Element:
    for dependency in view.findall("datasource-dependencies"):
        if dependency.get("datasource") == datasource_name:
            return dependency

    dependency = ET.Element("datasource-dependencies")
    dependency.set("datasource", datasource_name)
    aggregation = view.find("aggregation")
    if aggregation is not None:
        view.insert(list(view).index(aggregation), dependency)
    else:
        view.append(dependency)
    return dependency


def _ensure_dependency_column(dependency: ET.Element, field: FieldProfile) -> None:
    for column in dependency.findall("column"):
        if column.get("name") == field.local_name:
            if column.get("caption") is None:
                column.set("caption", field.display_name)
            return

    column = ET.Element("column")
    column.set("caption", field.display_name)
    column.set("datatype", field.datatype)
    column.set("name", field.local_name)
    column.set("role", field.role)
    column.set("type", field.field_type)

    insert_index = len(list(dependency))
    for index, child in enumerate(list(dependency)):
        if child.tag == "column-instance":
            insert_index = index
            break
    dependency.insert(insert_index, column)


def _ensure_dependency_column_instance(dependency: ET.Element, field: FieldProfile, spec: dict) -> None:
    for instance in dependency.findall("column-instance"):
        if instance.get("name") == spec["instance_name"]:
            return

    instance = ET.Element("column-instance")
    instance.set("column", field.local_name)
    instance.set("derivation", spec["derivation"])
    instance.set("name", spec["instance_name"])
    instance.set("pivot", spec["pivot"])
    instance.set("type", spec["ci_type"])
    dependency.append(instance)


def _find_or_create_table_child(table: ET.Element, tag_name: str) -> ET.Element:
    child = table.find(tag_name)
    if child is not None:
        return child
    child = ET.Element(tag_name)
    _insert_table_child(table, child)
    return child


def _replace_or_create_direct_child_from_spec(parent: ET.Element, tag_name: str, spec: dict) -> ET.Element:
    element = _build_element_from_spec(spec, default_tag=tag_name)
    existing = parent.find(tag_name)
    if existing is None:
        if parent.tag == "table":
            _insert_table_child(parent, element)
        else:
            parent.append(element)
        return element

    index = list(parent).index(existing)
    parent.remove(existing)
    parent.insert(index, element)
    return element


def _build_element_from_spec(spec: dict, default_tag: str = "") -> ET.Element:
    tag_name = str(spec.get("tag") or default_tag).strip()
    if not tag_name:
        raise ValueError("Element spec is missing tag")

    element = ET.Element(tag_name)
    for key, value in (spec.get("attrs") or {}).items():
        element.set(key, str(value))

    text = spec.get("text")
    if text is not None:
        element.text = str(text)

    for child_spec in spec.get("children") or []:
        element.append(_build_element_from_spec(child_spec))
    return element


def _collect_binding_strings(value: object) -> list[str]:
    bindings: list[str] = []
    if isinstance(value, str):
        if _looks_like_binding_text(value):
            bindings.append(value)
        return bindings

    if isinstance(value, dict):
        for item in value.values():
            bindings.extend(_collect_binding_strings(item))
        return bindings

    if isinstance(value, list):
        for item in value:
            bindings.extend(_collect_binding_strings(item))
    return bindings


def _looks_like_binding_text(text: str) -> bool:
    normalized = text.strip()
    if not normalized:
        return False
    return "[" in normalized and "]" in normalized


def _insert_table_child(table: ET.Element, child: ET.Element) -> None:
    desired_order = {
        "view": 0,
        "style": 1,
        "panes": 2,
        "rows": 3,
        "cols": 4,
    }
    child_order = desired_order.get(child.tag, len(desired_order))
    insert_index = len(list(table))
    for index, existing_child in enumerate(list(table)):
        existing_order = desired_order.get(existing_child.tag, len(desired_order))
        if existing_order > child_order:
            insert_index = index
            break
    table.insert(insert_index, child)


def _upsert_encoding(encodings_element: ET.Element, tag_name: str, column_value: str) -> None:
    matches = [child for child in encodings_element.findall(tag_name)]
    if matches:
        matches[0].set("column", column_value)
        for extra in matches[1:]:
            encodings_element.remove(extra)
        return
    child = ET.SubElement(encodings_element, tag_name)
    child.set("column", column_value)


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered
