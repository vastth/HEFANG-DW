# -*- coding: utf-8 -*-
"""告警辅助模块：封装企业微信机器人发送逻辑，便于复用与替换。
"""
import logging
import re

import requests

logger = logging.getLogger(__name__)

EMOJI_RE = re.compile(
    '['
    '\U0001F300-\U0001FAFF'
    '\u200d'
    '\u20e3'
    '\u2600-\u27BF'
    '\ufe0f'
    ']'
)


def _strip_emoji_for_log(content):
    text = str(content)
    text = EMOJI_RE.sub('', text)
    return re.sub(r'[ \t]{2,}', ' ', text)


def send_wechat_alert(webhook_url, content, mention_mobiles=None, max_len=1500, timeout=10):
    """通过企业微信机器人 webhook 发送告警（text 类型）。

    返回 True 表示发送成功，False 表示失败。
    max_len 参数仅保留兼容旧调用；企业微信正文默认完整发送，不做截断。
    """
    if not webhook_url:
        logger.warning('No WECHAT_WEBHOOK configured, skip sending alert')
        return False

    text_to_send = str(content)
    payload = {
        "msgtype": "text",
        "text": {"content": text_to_send}
    }
    if mention_mobiles:
        payload["text"]["mentioned_mobile_list"] = mention_mobiles

    try:
        logger.info(f"Wechat alert content:\n{_strip_emoji_for_log(text_to_send)}")
        resp = requests.post(webhook_url, json=payload, timeout=timeout)
        if resp.status_code == 200:
            logger.info('Sent wechat alert successfully')
            return True
        else:
            logger.error(f'Failed to send wechat alert, status={resp.status_code}, body={resp.text}')
            return False
    except Exception:
        logger.exception('Exception when sending wechat alert')
        return False
