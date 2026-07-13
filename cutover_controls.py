# -*- coding: utf-8 -*-
"""cutover / rollback 控制共享工具。"""

from __future__ import annotations

import os


CUTOVER_MODE_ENV = 'HEFANG_CUTOVER_MODE'
STORE_DAILY_FRESHNESS_SOURCE_ENV = 'HEFANG_STORE_DAILY_FRESHNESS_SOURCE'

CUTOVER_MODE_LEGACY = 'legacy'
CUTOVER_MODE_SHADOW_COMPARE = 'shadow_compare'
CUTOVER_MODE_V2 = 'v2'
VALID_CUTOVER_MODES = (
    CUTOVER_MODE_LEGACY,
    CUTOVER_MODE_SHADOW_COMPARE,
    CUTOVER_MODE_V2,
)

STORE_DAILY_FRESHNESS_SOURCE_LEGACY = 'legacy'
STORE_DAILY_FRESHNESS_SOURCE_V2 = 'v2'
VALID_STORE_DAILY_FRESHNESS_SOURCES = (
    STORE_DAILY_FRESHNESS_SOURCE_LEGACY,
    STORE_DAILY_FRESHNESS_SOURCE_V2,
)


def normalize_cutover_mode(cutover_mode: str | None, *, rollback_to_legacy: bool = False) -> str:
    if rollback_to_legacy:
        return CUTOVER_MODE_LEGACY

    mode = (cutover_mode or '').strip().lower() or CUTOVER_MODE_LEGACY
    if mode not in VALID_CUTOVER_MODES:
        raise ValueError(f'不支持的 cutover_mode: {cutover_mode}')
    return mode


def resolve_cutover_mode(cutover_mode: str | None = None, *, rollback_to_legacy: bool = False) -> str:
    env_mode = os.getenv(CUTOVER_MODE_ENV, '').strip().lower()
    return normalize_cutover_mode(cutover_mode or env_mode, rollback_to_legacy=rollback_to_legacy)


def normalize_store_daily_freshness_source(source: str | None) -> str | None:
    if source is None:
        return None

    normalized = source.strip().lower()
    if normalized not in VALID_STORE_DAILY_FRESHNESS_SOURCES:
        raise ValueError(f'不支持的专题 freshness 来源: {source}')
    return normalized


def derive_store_daily_freshness_source(
    cutover_mode: str,
    explicit_source: str | None = None,
) -> str:
    normalized_source = normalize_store_daily_freshness_source(
        explicit_source or os.getenv(STORE_DAILY_FRESHNESS_SOURCE_ENV, '').strip() or None
    )
    if normalized_source is not None:
        return normalized_source

    if cutover_mode == CUTOVER_MODE_LEGACY:
        return STORE_DAILY_FRESHNESS_SOURCE_LEGACY
    return STORE_DAILY_FRESHNESS_SOURCE_V2
