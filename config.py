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
