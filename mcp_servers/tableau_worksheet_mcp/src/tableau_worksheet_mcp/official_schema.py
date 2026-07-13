from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator
from urllib.request import urlretrieve
from zipfile import ZipFile
import itertools
import xml.etree.ElementTree as ET

import xmlschema


OFFICIAL_SCHEMA_RAW_BASE_URL = (
    "https://raw.githubusercontent.com/tableau/tableau-document-schemas/main/schemas"
)
USER_NAMESPACE = "http://www.tableausoftware.com/xml/user"
USER_NAMESPACE_SCHEMA_NAME = "tableau_user_namespace_compat.xsd"


def _default_cache_dir() -> Path:
    return Path.home() / ".cache" / "hefang_dw" / "tableau_document_schemas"


def _read_workbook_root_attributes(twb_path: Path) -> dict:
    for _, elem in ET.iterparse(twb_path, events=("start",)):
        if elem.tag != "workbook":
            raise ValueError(f"Expected <workbook> root, got <{elem.tag}>")
        return {
            "version": elem.get("version", ""),
            "original_version": elem.get("original-version", ""),
            "source_build": elem.get("source-build", ""),
            "source_platform": elem.get("source-platform", ""),
        }
    raise ValueError(f"Empty TWB file: {twb_path}")


@contextmanager
def _materialize_twb(file_path: str | Path) -> Iterator[tuple[Path, str | None]]:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Workbook not found: {path}")

    if path.suffix.lower() != ".twbx":
        yield path, None
        return

    with ZipFile(path) as archive:
        twb_names = [name for name in archive.namelist() if name.lower().endswith(".twb")]
        if not twb_names:
            raise ValueError(f"No .twb found inside {path}")
        twb_name = twb_names[0]
        with TemporaryDirectory(prefix="tableau_schema_") as temp_dir:
            target = Path(temp_dir) / Path(twb_name).name
            target.write_bytes(archive.read(twb_name))
            yield target, twb_name


def _schema_candidate_for_workbook_version(workbook_version: str) -> dict:
    parts = workbook_version.split(".")
    if len(parts) < 2 or not parts[0].isdigit() or not parts[1].isdigit():
        return {
            "supported": False,
            "reason": f"Cannot map workbook version to official schema: {workbook_version!r}",
        }

    major = int(parts[0])
    release = int(parts[1])
    if major < 26:
        return {
            "supported": False,
            "reason": (
                "Official tableau-document-schemas currently start from Tableau 2026.1 "
                f"style version 26.1; workbook version is {workbook_version}."
            ),
            "candidate_folder": f"20{major:02d}_{release}",
            "candidate_file": f"twb_20{major:02d}.{release}.0.xsd",
        }

    year = 2000 + major
    return {
        "supported": True,
        "folder": f"{year}_{release}",
        "schema_file": f"twb_{year}.{release}.0.xsd",
        "schema_version": f"{year}.{release}.0",
    }


def _write_user_namespace_schema(cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    schema_path = cache_dir / USER_NAMESPACE_SCHEMA_NAME
    schema_path.write_text(
                f"""<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                     targetNamespace="{USER_NAMESPACE}"
                     xmlns:user="{USER_NAMESPACE}"
                     elementFormDefault="qualified"
                     attributeFormDefault="qualified">
    <xs:attributeGroup name="UserAttributes-AG">
        <xs:anyAttribute namespace="##targetNamespace" processContents="skip"/>
    </xs:attributeGroup>
</xs:schema>
""",
        encoding="utf-8",
    )
    return schema_path


def _ensure_official_schema(
    cache_dir: Path,
    folder: str,
    schema_file: str,
    allow_download: bool,
) -> Path:
    schema_dir = cache_dir / "schemas" / folder
    schema_path = schema_dir / schema_file
    if schema_path.exists():
        return schema_path

    if not allow_download:
        raise FileNotFoundError(
            f"Official schema not cached: {schema_path}. Enable allow_download or pass schema_path."
        )

    schema_dir.mkdir(parents=True, exist_ok=True)
    source_url = f"{OFFICIAL_SCHEMA_RAW_BASE_URL}/{folder}/{schema_file}"
    urlretrieve(source_url, schema_path)
    return schema_path


def _format_validation_error(error: xmlschema.XMLSchemaValidationError) -> dict:
    return {
        "path": getattr(error, "path", None),
        "reason": getattr(error, "reason", str(error)),
        "message": str(error).splitlines()[0] if str(error) else "",
    }


def validate_workbook_schema_for_file(
    file_path: str | Path,
    schema_path: str | Path = "",
    allow_download: bool = True,
    max_errors: int = 20,
    cache_dir: str | Path = "",
) -> dict:
    """Validate a TWB/TWBX workbook against Tableau's official TWB XSD when available.

    The official repository validates TWB XML only. For TWBX input this function extracts
    the first packaged TWB and validates that XML member, not the package envelope.
    """

    source_path = Path(file_path)
    effective_cache_dir = Path(cache_dir) if cache_dir else _default_cache_dir()
    effective_cache_dir.mkdir(parents=True, exist_ok=True)

    with _materialize_twb(source_path) as (twb_path, twbx_member):
        root_attrs = _read_workbook_root_attributes(twb_path)
        workbook_version = root_attrs["version"]

        if schema_path:
            official_schema_supported = True
            official_schema = Path(schema_path)
            schema_candidate = {
                "supported": True,
                "folder": official_schema.parent.name,
                "schema_file": official_schema.name,
                "schema_version": "custom",
            }
        else:
            schema_candidate = _schema_candidate_for_workbook_version(workbook_version)
            if not schema_candidate.get("supported"):
                return {
                    "file_path": str(source_path),
                    "twbx_member": twbx_member,
                    "status": "skipped",
                    "valid": None,
                    "official_schema_supported": False,
                    "workbook": root_attrs,
                    "schema": schema_candidate,
                    "errors": [],
                    "warnings": [
                        schema_candidate["reason"],
                        "Do not bump workbook version only to satisfy XSD validation; keep Tableau round-trip compatibility first.",
                    ],
                    "notes": _validation_notes(twbx_member),
                }
            official_schema_supported = True
            official_schema = _ensure_official_schema(
                effective_cache_dir,
                schema_candidate["folder"],
                schema_candidate["schema_file"],
                allow_download=allow_download,
            )

        if not official_schema.exists():
            raise FileNotFoundError(f"Schema file not found: {official_schema}")

        user_namespace_schema = _write_user_namespace_schema(effective_cache_dir)
        schema = xmlschema.XMLSchema([str(official_schema), str(user_namespace_schema)])
        errors = list(itertools.islice(schema.iter_errors(str(twb_path)), max_errors))

        return {
            "file_path": str(source_path),
            "twbx_member": twbx_member,
            "status": "passed" if not errors else "failed",
            "valid": not errors,
            "official_schema_supported": official_schema_supported,
            "workbook": root_attrs,
            "schema": {
                **schema_candidate,
                "path": str(official_schema),
                "user_namespace_adapter": str(user_namespace_schema),
            },
            "errors": [_format_validation_error(error) for error in errors],
            "warnings": [],
            "notes": _validation_notes(twbx_member),
        }


def _validation_notes(twbx_member: str | None) -> list[str]:
    notes = [
        "Official XSD validation is syntactic/structural only; Tableau semantic render testing is still required.",
        "Connections, calculation contents, and named references may remain unchecked because the XSD intentionally uses loose string types or processContents=skip in several areas.",
    ]
    if twbx_member:
        notes.append(
            "Official Tableau document schemas do not validate TWBX packages; this result only covers the extracted TWB XML member."
        )
    return notes