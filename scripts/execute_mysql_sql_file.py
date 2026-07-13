# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db_connections import connect_mysql


def _resolve_sql_file(sql_file_arg):
    sql_file = Path(sql_file_arg)
    if not sql_file.is_absolute():
        sql_file = REPO_ROOT / sql_file
    if not sql_file.exists():
        raise FileNotFoundError(f'未找到 SQL 文件: {sql_file}')
    return sql_file


def _split_sql_statements(sql_text):
    return [statement.strip() for statement in sql_text.split(';') if statement.strip()]


def main():
    parser = argparse.ArgumentParser(description='执行单个 MySQL SQL 文件')
    parser.add_argument('--sql-file', required=True, help='相对仓库根目录或绝对路径的 SQL 文件')
    args = parser.parse_args()

    sql_file = _resolve_sql_file(args.sql_file)
    sql_text = sql_file.read_text(encoding='utf-8')
    statements = _split_sql_statements(sql_text)
    if not statements:
        raise RuntimeError(f'SQL 文件中没有可执行语句: {sql_file}')

    conn = connect_mysql(autocommit=True)
    try:
        with conn.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
    finally:
        conn.close()

    print(f'EXECUTED {len(statements)} STATEMENT(S): {sql_file.name}')


if __name__ == '__main__':
    main()