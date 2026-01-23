# -*- coding: utf-8 -*-
"""
何方珠宝 - 数据库连接测试
使用 oracledb (thin模式，无需Oracle客户端)
"""

import sys

def test_oracle_connection():
    """测试Oracle连接"""
    print("\n" + "="*50)
    print("测试 Oracle 连接 (oracledb thin模式)...")
    print("="*50)
    
    try:
        import oracledb
        print(f"[OK] oracledb 模块导入成功 (版本: {oracledb.__version__})")
    except ImportError:
        print("[FAIL] oracledb 模块未安装")
        print("  请运行: pip install oracledb")
        return False
    
    try:
        from config import ORACLE_CONFIG, ORACLE_DSN
        print(f"[OK] 配置文件读取成功")
        print(f"  连接目标: {ORACLE_DSN}")
    except ImportError:
        print("[FAIL] config.py 文件不存在或有语法错误")
        return False
    
    try:
        # 使用thin模式连接（无需Oracle客户端）
        conn = oracledb.connect(
            user=ORACLE_CONFIG['user'],
            password=ORACLE_CONFIG['password'],
            dsn=ORACLE_DSN
        )
        print("[OK] Oracle 连接成功!")
        
        # 测试查询
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM M_PRODUCT WHERE ISACTIVE='Y'")
        count = cursor.fetchone()[0]
        print(f"[OK] 测试查询成功: M_PRODUCT有效商品 {count} 条")
        
        cursor.close()
        conn.close()
        return True
        
    except oracledb.Error as e:
        error, = e.args
        print(f"[FAIL] Oracle 连接失败!")
        print(f"  错误信息: {error.message}")
        
        # 常见错误提示
        if 'ORA-12541' in str(error.message):
            print("  → 检查Oracle服务器IP和端口是否正确")
        elif 'ORA-12514' in str(error.message):
            print("  → 检查service_name是否正确")
        elif 'ORA-01017' in str(error.message):
            print("  → 检查用户名和密码是否正确")
        elif 'ORA-12170' in str(error.message):
            print("  → 网络超时，检查防火墙设置")
        elif 'DPY-6005' in str(error.message):
            print("  → 无法连接到数据库，检查网络和端口")
        return False
    except Exception as e:
        print(f"[FAIL] Oracle 连接失败: {str(e)}")
        return False


def test_mysql_connection():
    """测试MySQL连接"""
    print("\n" + "="*50)
    print("测试 MySQL 连接...")
    print("="*50)
    
    try:
        import pymysql
        print("[OK] pymysql 模块导入成功")
    except ImportError:
        print("[FAIL] pymysql 模块未安装")
        print("  请运行: pip install pymysql")
        return False
    
    try:
        from config import MYSQL_CONFIG
        print(f"[OK] 配置文件读取成功")
        print(f"  连接目标: {MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}")
    except ImportError:
        print("[FAIL] config.py 文件不存在或有语法错误")
        return False
    
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        print("[OK] MySQL 连接成功!")
        
        # 测试查询
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM dim_date")
        count = cursor.fetchone()[0]
        print(f"[OK] 测试查询成功: dim_date有 {count} 条记录")
        
        cursor.close()
        conn.close()
        return True
        
    except pymysql.Error as e:
        print(f"[FAIL] MySQL 连接失败!")
        print(f"  错误代码: {e.args[0]}")
        print(f"  错误信息: {e.args[1]}")
        
        if e.args[0] == 2003:
            print("  → 检查MySQL服务是否启动，IP和端口是否正确")
        elif e.args[0] == 1045:
            print("  → 检查用户名和密码是否正确")
        elif e.args[0] == 1049:
            print("  → 数据库不存在，请先执行建库脚本")
        return False


def check_dependencies():
    """检查依赖包"""
    print("\n" + "="*50)
    print("检查 Python 依赖包...")
    print("="*50)
    
    packages = {
        'oracledb': 'pip install oracledb',
        'pymysql': 'pip install pymysql', 
        'pandas': 'pip install pandas',
        'sqlalchemy': 'pip install sqlalchemy'
    }
    
    missing = []
    for package, install_cmd in packages.items():
        try:
            __import__(package)
            print(f"[OK] {package}")
        except ImportError:
            print(f"[FAIL] {package} 未安装")
            missing.append(install_cmd)
    
    if missing:
        print("\n请安装缺失的包:")
        print("  pip install oracledb pymysql pandas sqlalchemy")
        return False
    
    return True


if __name__ == '__main__':
    print("\n" + "#"*50)
    print("#  何方珠宝 - ETL连接测试 (oracledb版)")
    print("#"*50)
    
    # 1. 检查依赖
    dep_ok = check_dependencies()
    
    if not dep_ok:
        print("\n[WARN] 请先安装依赖包，然后重新运行此脚本")
        sys.exit(1)
    
    # 2. 测试Oracle
    oracle_ok = test_oracle_connection()
    
    # 3. 测试MySQL
    mysql_ok = test_mysql_connection()
    
    # 4. 汇总结果
    print("\n" + "="*50)
    print("测试结果汇总")
    print("="*50)
    print(f"Oracle连接: {'OK 成功' if oracle_ok else 'FAIL 失败'}")
    print(f"MySQL连接:  {'OK 成功' if mysql_ok else 'FAIL 失败'}")
    
    if oracle_ok and mysql_ok:
        print("\n[OK] 所有连接测试通过！可以开始运行ETL了")
    else:
        print("\n[WARN] 请根据上方提示修复连接问题")
