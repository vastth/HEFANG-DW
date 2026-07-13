# -*- coding: utf-8 -*-
"""企业微信告警发送格式测试。"""

import unittest
from unittest.mock import Mock, patch

from alerts import send_wechat_alert


class WechatAlertTests(unittest.TestCase):
    def test_send_wechat_alert_keeps_full_content_even_when_max_len_is_small(self):
        long_content = '总控调度完成\n' + '\n'.join(f'明细{i}: SUCCESS' for i in range(120))

        with patch('alerts.requests.post') as mock_post:
            mock_post.return_value = Mock(status_code=200, text='ok')

            sent = send_wechat_alert('https://example.invalid/webhook', long_content, max_len=30)

        self.assertTrue(sent)
        payload = mock_post.call_args.kwargs['json']
        self.assertEqual(payload['text']['content'], long_content)
        self.assertNotIn('内容过长，已保留开头和结尾', payload['text']['content'])


if __name__ == '__main__':
    unittest.main()
