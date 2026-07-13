# -*- coding: utf-8 -*-
"""Windows NAS 访问辅助工具。"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

if os.name == 'nt':
    import ctypes
    from ctypes import wintypes


PRIMARY_NAS_USERNAME_ENV = 'HEFANG_NAS_USERNAME'
PRIMARY_NAS_PASSWORD_ENV = 'HEFANG_NAS_PASSWORD'
FALLBACK_NAS_USERNAME_ENV = 'NAS_USERNAME'
FALLBACK_NAS_PASSWORD_ENV = 'NAS_PASSWORD'
NAS_USERNAME_ENV_NAMES = (PRIMARY_NAS_USERNAME_ENV, FALLBACK_NAS_USERNAME_ENV)
NAS_PASSWORD_ENV_NAMES = (PRIMARY_NAS_PASSWORD_ENV, FALLBACK_NAS_PASSWORD_ENV)

if os.name == 'nt':
    RESOURCETYPE_DISK = 0x00000001
    ERROR_SESSION_CREDENTIAL_CONFLICT = 1219
    ERROR_NOT_CONNECTED = 2250

    class _NETRESOURCEW(ctypes.Structure):
        _fields_ = [
            ('dwScope', wintypes.DWORD),
            ('dwType', wintypes.DWORD),
            ('dwDisplayType', wintypes.DWORD),
            ('dwUsage', wintypes.DWORD),
            ('lpLocalName', ctypes.c_wchar_p),
            ('lpRemoteName', ctypes.c_wchar_p),
            ('lpComment', ctypes.c_wchar_p),
            ('lpProvider', ctypes.c_wchar_p),
        ]

    _mpr = ctypes.WinDLL('mpr', use_last_error=True)
    _WNetAddConnection2W = _mpr.WNetAddConnection2W
    _WNetAddConnection2W.argtypes = [
        ctypes.POINTER(_NETRESOURCEW),
        ctypes.c_wchar_p,
        ctypes.c_wchar_p,
        wintypes.DWORD,
    ]
    _WNetAddConnection2W.restype = wintypes.DWORD

    _WNetCancelConnection2W = _mpr.WNetCancelConnection2W
    _WNetCancelConnection2W.argtypes = [
        ctypes.c_wchar_p,
        wintypes.DWORD,
        wintypes.BOOL,
    ]
    _WNetCancelConnection2W.restype = wintypes.DWORD


@dataclass(frozen=True)
class NasShareInfo:
    host: str
    share_name: str
    share_root: str


def _parse_nas_share(target_path: Path | str) -> NasShareInfo | None:
    path_text = str(target_path)
    if not path_text.startswith('\\\\'):
        return None

    path_parts = [part for part in path_text.lstrip('\\').split('\\') if part]
    if len(path_parts) < 2:
        return None

    host = path_parts[0]
    share_name = path_parts[1]
    return NasShareInfo(host=host, share_name=share_name, share_root=f'\\\\{host}\\{share_name}')


def _probe_share_root(share_root: str) -> tuple[bool, OSError | None]:
    try:
        with os.scandir(share_root):
            return True, None
    except OSError as exc:
        return False, exc


def _read_env_value(env_names: tuple[str, ...]) -> str:
    for env_name in env_names:
        env_value = os.getenv(env_name)
        if env_value:
            return env_value
    return ''


def _build_username_candidates(username: str, host: str) -> list[str]:
    candidates = [username]
    if '\\' not in username and '@' not in username:
        candidates.append(f'{host}\\{username}')
    return list(dict.fromkeys(candidate for candidate in candidates if candidate))


def _connect_share_root(share_info: NasShareInfo, username: str, password: str) -> None:
    if os.name != 'nt':
        return

    last_error: OSError | None = None
    resource = _NETRESOURCEW()
    resource.dwType = RESOURCETYPE_DISK
    resource.lpRemoteName = share_info.share_root

    for candidate_username in _build_username_candidates(username, share_info.host):
        result = _WNetAddConnection2W(ctypes.byref(resource), password, candidate_username, 0)
        if result == 0:
            return

        if result == ERROR_SESSION_CREDENTIAL_CONFLICT:
            cancel_result = _WNetCancelConnection2W(share_info.share_root, 0, True)
            if cancel_result not in (0, ERROR_NOT_CONNECTED):
                raise ctypes.WinError(cancel_result)
            result = _WNetAddConnection2W(ctypes.byref(resource), password, candidate_username, 0)
            if result == 0:
                return

        last_error = ctypes.WinError(result)

    if last_error is None:
        raise OSError(f'无法建立 NAS 连接: {share_info.share_root}')
    raise last_error


def ensure_nas_path_access(target_path: Path | str) -> None:
    share_info = _parse_nas_share(target_path)
    if share_info is None:
        return

    is_accessible, probe_error = _probe_share_root(share_info.share_root)
    if is_accessible:
        return

    username = _read_env_value(NAS_USERNAME_ENV_NAMES)
    password = _read_env_value(NAS_PASSWORD_ENV_NAMES)
    if not username or not password:
        env_hint = f'{PRIMARY_NAS_USERNAME_ENV}/{PRIMARY_NAS_PASSWORD_ENV}'
        fallback_hint = f'{FALLBACK_NAS_USERNAME_ENV}/{FALLBACK_NAS_PASSWORD_ENV}'
        message = (
            f'NAS 路径 {share_info.share_root} 当前不可访问，且未配置环境变量 {env_hint} '
            f'（兼容 {fallback_hint}）'
        )
        raise OSError(message) from probe_error

    _connect_share_root(share_info, username=username, password=password)
    is_accessible, probe_error = _probe_share_root(share_info.share_root)
    if is_accessible:
        return

    message = f'NAS 路径 {share_info.share_root} 已尝试使用环境变量自动鉴权，但仍不可访问'
    if probe_error is None:
        raise OSError(message)
    raise OSError(f'{message}: {probe_error}') from probe_error