# -*- coding: utf-8 -*-
"""
何方珠宝 - ETL配置文件
修改下方的连接信息为你的实际配置
"""

import os

# ============================================
# Oracle连接配置（伯俊ERP）
# 使用 oracledb thin模式，无需安装Oracle客户端
# 凭据与地址从环境变量读取，避免泄露到Git仓库
# ============================================
ORACLE_CONFIG = {
    'user': os.getenv('ORACLE_USER', 'change_me'),
    'password': os.getenv('ORACLE_PASSWORD', 'change_me'),
    'host': os.getenv('ORACLE_HOST', 'localhost'),
    'port': int(os.getenv('ORACLE_PORT', '1521')),
    'service_name': os.getenv('ORACLE_SERVICE', 'orcl')
}

# 构建DSN
ORACLE_DSN = f"{ORACLE_CONFIG['host']}:{ORACLE_CONFIG['port']}/{ORACLE_CONFIG['service_name']}"


# ============================================
# MySQL连接配置（本地数仓）
# ============================================
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', '3306')),
    'user': os.getenv('MYSQL_USER', 'change_me'),
    'password': os.getenv('MYSQL_PASSWORD', 'change_me'),
    'database': os.getenv('MYSQL_DB', 'hefang_dw'),
    'charset': os.getenv('MYSQL_CHARSET', 'utf8mb4')
}

# 构建SQLAlchemy连接字符串
MYSQL_CONN_STR = (
    f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}"
    f"@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}"
    f"?charset={MYSQL_CONFIG['charset']}"
)


# ============================================
# 业务配置（不要修改，除非业务规则变了）
# ============================================

# 主销品类别ID列表
MAIN_CATEGORY_IDS = (134, 142, 139, 138, 141, 143, 133, 136, 140, 137, 144, 145)

# 在售款性质ID
PROPERTY_ONSALE = (224, 296, 297)

# 新品性质ID  
PROPERTY_NEW = (225, 298, 299)

# 绝版款性质ID
PROPERTY_DISCONTINUED = (127, 126, 152)


# ============================================
# 企业微信告警配置
# 推荐通过环境变量 WECHAT_WEBHOOK 注入，不要把实际 webhook 写入代码仓库。
# 在 Windows CMD 中可使用：
#   setx WECHAT_WEBHOOK "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY"
# 在 PowerShell（永久，User 级）可使用：
#   [Environment]::SetEnvironmentVariable('WECHAT_WEBHOOK','https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY','User')
# 或者在当前会话设置（临时）：
#   $env:WECHAT_WEBHOOK = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY'
# ============================================
WECHAT_WEBHOOK = os.getenv('WECHAT_WEBHOOK', '')

# ================
# 告警与重试配置
# ================
# 任务名映射为友好中文，用于告警显示，可按需扩展或国际化
TASK_DISPLAY_NAME = {
    'oracle': 'Oracle（伯俊ERP）',
    'mysql': 'MySQL（数据仓库）',
    'dim_product': '商品维度',
    'dim_sku': 'SKU 维度',
    'dim_store': '店仓维度',
    'dws_sales': '销售数据 (dws_sales)',
    'dws_inventory': '库存数据 (dws_inventory)',
    'dabo_ready': '达播数据就绪检查',
    'ads_health': '库存健康度计算',
}

# 重试相关配置：哪些关键字表示非重试错误（认证失败、权限等），遇到这些应立即告警并放弃重试
ETL_NON_RETRYABLE_ERROR_KEYWORDS = [
    'ORA-01017', 'invalid username', 'access denied', 'authentication failed', 'ORA-01000'
]

# 可选：显式列出可重试关键字（网络类、超时等），当出现这些关键字时才重试
ETL_RETRYABLE_ERROR_KEYWORDS = [
    'timeout', 'timed out', 'Connection refused', 'connect timeout', 'deadlock', 'temporarily unavailable'
]

# 默认重试参数（可通过环境变量在运行时覆盖）
ETL_DEFAULT_MAX_RETRIES = int(os.getenv('ETL_MAX_RETRIES', '3'))
ETL_DEFAULT_RETRY_SLEEP = int(os.getenv('ETL_RETRY_SLEEP', '60'))
