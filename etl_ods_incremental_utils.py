# -*- coding: utf-8 -*-
"""ODS 增量写入辅助工具。"""

from sqlalchemy import text


def _execute_delete_batches(conn, table_name, unique_ids, batch_size):
    for start_index in range(0, len(unique_ids), batch_size):
        batch_ids = unique_ids[start_index:start_index + batch_size]
        placeholders = ", ".join(f":id_{index}" for index in range(len(batch_ids)))
        params = {f"id_{index}": batch_id for index, batch_id in enumerate(batch_ids)}
        conn.execute(text(f"DELETE FROM {table_name} WHERE id IN ({placeholders})"), params)


def delete_existing_ids(engine, table_name, ids, batch_size=1000):
    """按业务 id 删除目标表中的旧副本，避免跨窗口晚改留下历史残影。"""

    unique_ids = []
    seen_ids = set()
    for raw_id in ids:
        if raw_id is None:
            continue
        try:
            normalized_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        if normalized_id in seen_ids:
            continue
        seen_ids.add(normalized_id)
        unique_ids.append(normalized_id)

    if not unique_ids:
        return

    if hasattr(engine, "execute"):
        _execute_delete_batches(engine, table_name, unique_ids, batch_size)
        return

    with engine.begin() as conn:
        _execute_delete_batches(conn, table_name, unique_ids, batch_size)