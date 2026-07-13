# -*- coding: utf-8 -*-
"""为 ods_m_retail.id 创建普通索引。"""

from pathlib import Path
import sys

from sqlalchemy import text

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db_connections import create_mysql_engine


def main():
    engine = create_mysql_engine()
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "ALTER TABLE ods_m_retail ADD INDEX idx_ods_m_retail_id (id), ALGORITHM=INPLACE, LOCK=NONE"
            ))
        print("INDEX_DONE", flush=True)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()