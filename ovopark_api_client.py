# -*- coding: utf-8 -*-
"""万店掌开放平台 API 客户端。"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import requests

from config import OVOPARK_API_CONFIG


logger = logging.getLogger(__name__)


def _utc_timestamp_millis() -> str:
    return str(int(datetime.now().timestamp() * 1000))


def _stringify(value: Any) -> str:
    if value is None:
        return ''
    if isinstance(value, bool):
        return 'true' if value else 'false'
    return str(value)


def _masked_value(key: str, value: Any) -> Any:
    lowered = key.lower()
    if lowered in {'password', 'authenticator', 'ovo-authorization', '_sig'}:
        return '***'
    return value


def sanitize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: _masked_value(key, value) for key, value in payload.items()}


@dataclass(frozen=True)
class OvoparkCredentials:
    base_url: str
    app_id: str
    access_key_id: str
    access_key_secret: str
    request_mode: str
    version: str
    sign_method: str
    username: str
    password: str
    authenticator: str
    request_timeout: int


def load_ovopark_credentials(
    *,
    require_authenticator: bool = False,
    require_login: bool = False,
) -> OvoparkCredentials:
    credentials = OvoparkCredentials(
        base_url=OVOPARK_API_CONFIG['base_url'],
        app_id=OVOPARK_API_CONFIG['app_id'],
        access_key_id=OVOPARK_API_CONFIG['access_key_id'],
        access_key_secret=OVOPARK_API_CONFIG['access_key_secret'],
        request_mode=OVOPARK_API_CONFIG['request_mode'],
        version=OVOPARK_API_CONFIG['version'],
        sign_method=OVOPARK_API_CONFIG['sign_method'],
        username=OVOPARK_API_CONFIG['username'],
        password=OVOPARK_API_CONFIG['password'],
        authenticator=OVOPARK_API_CONFIG['authenticator'],
        request_timeout=OVOPARK_API_CONFIG['request_timeout'],
    )

    missing_fields = []
    if not credentials.app_id:
        missing_fields.append('OVOPARK_APP_ID')
    if not credentials.access_key_id:
        missing_fields.append('OVOPARK_ACCESS_KEY_ID')
    if not credentials.access_key_secret:
        missing_fields.append('OVOPARK_ACCESS_KEY_SECRET')
    if require_authenticator and not credentials.authenticator and not credentials.username:
        missing_fields.append('OVOPARK_AUTHENTICATOR or OVOPARK_USERNAME/OVOPARK_PASSWORD')
    if require_login:
        if not credentials.username:
            missing_fields.append('OVOPARK_USERNAME')
        if not credentials.password:
            missing_fields.append('OVOPARK_PASSWORD')
    if missing_fields:
        raise RuntimeError(f"缺少 Ovopark 环境变量: {', '.join(missing_fields)}")
    return credentials


def build_signature(secret: str, params: dict[str, Any]) -> str:
    sign_items = []
    for key in sorted(params):
        if key == '_sig':
            continue
        value = params[key]
        if value is None:
            continue
        sign_items.append(f'{key}{_stringify(value)}')
    sign_text = f"{secret}{''.join(sign_items)}{secret}"
    return hashlib.md5(sign_text.encode('utf-8')).hexdigest().upper()


class OvoparkApiClient:
    def __init__(self, credentials: OvoparkCredentials):
        self.credentials = credentials
        self.session = requests.Session()

    def _build_signed_params(self, method_name: str, business_params: dict[str, Any]) -> dict[str, Any]:
        params: dict[str, Any] = {
            '_aid': self.credentials.app_id,
            '_akey': self.credentials.access_key_id,
            '_mt': method_name,
            '_sm': self.credentials.sign_method,
            '_requestMode': self.credentials.request_mode,
            '_version': self.credentials.version,
            '_timestamp': _utc_timestamp_millis(),
        }
        for key, value in business_params.items():
            if value is not None:
                params[key] = value
        params['_sig'] = build_signature(self.credentials.access_key_secret, params)
        return params

    @staticmethod
    def _extract_authenticator(response_json: dict[str, Any], response_headers: dict[str, Any]) -> str:
        candidates = (
            response_headers.get('Ovo-Authorization'),
            response_json.get('authenticator'),
            response_json.get('token'),
            (response_json.get('data') or {}).get('authenticator'),
            (response_json.get('data') or {}).get('token'),
            (response_json.get('data') or {}).get('ovoAuthorization'),
        )
        for candidate in candidates:
            if candidate:
                return str(candidate)
        return ''

    def request(self, method_name: str, business_params: dict[str, Any], *, authenticator: str = '') -> tuple[dict[str, Any], dict[str, Any]]:
        payload = self._build_signed_params(method_name, business_params)
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        }
        effective_authenticator = authenticator or self.credentials.authenticator
        if effective_authenticator:
            headers['Ovo-Authorization'] = effective_authenticator

        response = self.session.post(
            self.credentials.base_url,
            data=payload,
            headers=headers,
            timeout=self.credentials.request_timeout,
        )
        response.raise_for_status()
        response_json = response.json()
        request_meta = {
            'method_name': method_name,
            'request_params': sanitize_payload(payload),
            'response_headers': dict(response.headers),
            'response_text': response.text,
            'authenticator': self._extract_authenticator(response_json, response.headers),
        }
        return response_json, request_meta

    def resolve_authenticator(self) -> str:
        if self.credentials.authenticator:
            return self.credentials.authenticator
        if not self.credentials.username or not self.credentials.password:
            raise RuntimeError('未提供 OVOPARK_AUTHENTICATOR，且缺少 OVOPARK_USERNAME / OVOPARK_PASSWORD，无法自动登录')

        response_json, request_meta = self.request(
            'open.shopweb.security.mobileLogin',
            {
                'userName': self.credentials.username,
                'password': self.credentials.password,
            },
        )
        authenticator = request_meta['authenticator']
        if not authenticator:
            raise RuntimeError(
                'mobileLogin 已返回响应，但未从响应头或 JSON 中解析到 authenticator；'
                '请改用 OVOPARK_AUTHENTICATOR 环境变量显式注入'
            )
        stat = response_json.get('stat') or {}
        if stat.get('code') not in (0, '0', None):
            logger.warning('mobileLogin 返回非零状态码: %s', json.dumps(stat, ensure_ascii=False))
        return authenticator

    def get_departments(self, *, page_number: int, page_size: int, authenticator: str) -> tuple[dict[str, Any], dict[str, Any]]:
        return self.request(
            'open.organize.departments.getDepartments',
            {
                'pageNumber': page_number,
                'pageSize': page_size,
            },
            authenticator=authenticator,
        )

    def get_daily_passenger_indicator(
        self,
        *,
        dep_id: int,
        start_time: str,
        end_time: str,
        authenticator: str,
        is_on_business_time: int = 0,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        return self.request(
            'open.shopweb.passengerFlow.getPassengerIndicatorData',
            {
                'depId': dep_id,
                'startTime': start_time,
                'endTime': end_time,
                'isOnBusinessTime': is_on_business_time,
            },
            authenticator=authenticator,
        )

    def get_hourly_passenger_indicator(
        self,
        *,
        dep_key: str,
        start_time: str,
        end_time: str,
        authenticator: str,
        time_type: int = 1,
        is_on_business_time: int = 0,
        start_hour: int | None = None,
        end_hour: int | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        return self.request(
            'open.shopwebpassengerflow.customerflow.getManyShopsPassengerIndicatorData',
            {
                'id': dep_key,
                'startTime': start_time,
                'endTime': end_time,
                'timeType': time_type,
                'isOnBusinessTime': is_on_business_time,
                'starthour': start_hour,
                'endhour': end_hour,
            },
            authenticator=authenticator,
        )