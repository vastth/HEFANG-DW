-- 治理 ods_m_retailitem 历史重复装载，并对业务键 id 加唯一约束。
-- 执行前请先人工备份或快照 ods_m_retailitem。

SELECT COUNT(*) AS duplicate_id_count_before
FROM (
    SELECT id
    FROM ods_m_retailitem
    GROUP BY id
    HAVING COUNT(*) > 1
) t;

CREATE TEMPORARY TABLE tmp_ods_m_retailitem_duplicate_delete AS
SELECT id, etl_batch_id, etl_loaded_at
FROM (
    SELECT
        id,
        etl_batch_id,
        etl_loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY COALESCE(modifieddate, settime, '1900-01-01 00:00:00') DESC,
                     etl_loaded_at DESC,
                     etl_batch_id DESC
        ) AS rn
    FROM ods_m_retailitem
) ranked
WHERE rn > 1;

DELETE t
FROM ods_m_retailitem t
JOIN tmp_ods_m_retailitem_duplicate_delete d
  ON t.id = d.id
 AND t.etl_batch_id = d.etl_batch_id
 AND t.etl_loaded_at <=> d.etl_loaded_at;

DROP TEMPORARY TABLE tmp_ods_m_retailitem_duplicate_delete;

ALTER TABLE ods_m_retailitem
    ADD UNIQUE KEY uk_ods_m_retailitem_id (id);

SELECT COUNT(*) AS duplicate_id_count_after
FROM (
    SELECT id
    FROM ods_m_retailitem
    GROUP BY id
    HAVING COUNT(*) > 1
) t;

SHOW INDEX FROM ods_m_retailitem WHERE Key_name = 'uk_ods_m_retailitem_id';