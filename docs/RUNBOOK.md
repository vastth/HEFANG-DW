# RUNBOOK.md — 何方珠宝数据仓库运行手册

> 适用于：数据工程师 / 运维人员 / AI 协作 Agent
>
> 前置阅读：[docs/ARCHITECTURE.md](ARCHITECTURE.md)

---

## 1. 环境准备

### 1.1 Python 环境

```powershell
# 检查 Python 版本（需要 3.10+，项目在 3.13.x 开发）
python --version

# 安装依赖（无 requirements.txt 时手动安装）
pip install python-oracledb pandas sqlalchemy pymysql requests openpyxl

# 验证关键包
python -c "import oracledb, pandas, sqlalchemy, pymysql; print('OK')"
```

> **注意**：`python-oracledb` 使用 thin 模式，**无需安装 Oracle Instant Client**。

### 1.2 环境变量配置

`.env.example` 仅作为变量清单参考；当前脚本默认不自动加载 `.env` 文件。推荐直接设置 User 级别永久环境变量：

```powershell
# Oracle（伯俊 ERP）
[Environment]::SetEnvironmentVariable('ORACLE_USER',     'your_user',    'User')
[Environment]::SetEnvironmentVariable('ORACLE_PASSWORD', 'your_pass',    'User')
[Environment]::SetEnvironmentVariable('ORACLE_HOST',     '10.x.x.x',    'User')
[Environment]::SetEnvironmentVariable('ORACLE_PORT',     '1521',         'User')
[Environment]::SetEnvironmentVariable('ORACLE_SERVICE',  'your_service', 'User')

# MySQL（何方数仓）
[Environment]::SetEnvironmentVariable('MYSQL_HOST',     'localhost', 'User')
[Environment]::SetEnvironmentVariable('MYSQL_PORT',     '3306',      'User')
[Environment]::SetEnvironmentVariable('MYSQL_USER',     'your_user', 'User')
[Environment]::SetEnvironmentVariable('MYSQL_PASSWORD', 'your_pass', 'User')
[Environment]::SetEnvironmentVariable('MYSQL_DB',       'hefang_dw', 'User')

# 企业微信告警（可选）
[Environment]::SetEnvironmentVariable('WECHAT_WEBHOOK', 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY', 'User')
```

### 1.2.1 HFSY 只读探查连接事实

当前已确认的数云源端连接元信息如下：

- 数据库版本：MySQL `5.7.42`
- 部署地址：`8.134.87.152:33066`
- 数据库名：`hfsy`
- 接入账号：`shuyun668`

真实密码已由用户提供，但按仓库安全约束，不写入任何被 git 跟踪的文档、脚本或模板文件。推荐在本地终端按会话临时注入：

```powershell
$env:HFSY_MYSQL_HOST='8.134.87.152'
$env:HFSY_MYSQL_PORT='33066'
$env:HFSY_MYSQL_USER='shuyun668'
$env:HFSY_MYSQL_PASSWORD='请通过本地安全方式注入真实密码'
$env:HFSY_MYSQL_DB='hfsy'
```

说明：
- 当前仓库脚本尚未正式消费 `HFSY_MYSQL_*` 变量；这些变量主要用于保存只读探查上下文，避免和数仓库 `MYSQL_*` 混用。
- 若临时复用 [tools/query_data.py](../tools/query_data.py)，应在独立终端里临时覆盖 `MYSQL_*` 到 `hfsy`，执行完成后关闭该终端，避免误连到生产数仓。

### 1.3 连通性验证

```powershell
# 自检脚本（检查 Python / 环境变量 / 包）
pwsh scripts/doctor.ps1

# 真实连通测试（需要配置环境变量）
python tools/test_connection.py
```

---

## 2. 常用命令

### 2.1 主 ETL 流水线

```powershell
# 正常执行（T-1 增量，约 10-20 分钟）
python run_etl.py

# 连通性测试模式（不执行真实 ETL，仅测试数据库连接）
python run_etl.py --conn-test

# 连通性测试 + 单次重试（用于 CI 验证）
$env:ETL_MAX_RETRIES=1; $env:ETL_CONN_TEST=1; python run_etl.py --conn-test
```

### 2.2 ODS 专项

```powershell
# 增量同步（生产日常，使用双水位）
python run_ods.py

# 全量覆盖（首次初始化或数据修复用）
python run_ods.py --full

# 跳过自动质量校验
python run_ods.py --skip-qc

# 仅执行质量校验（不触发同步）
python tools/check_ods_incremental.py
python tools/check_ods_retailitem_quality.py
```

### 2.3 单步 ETL 调试

```powershell
# 单独执行某一 ETL 模块（以 dws_sales 为例）
python -c "import etl_dws_sales; etl_dws_sales.run(days_back=1)"

# 历史回填（指定日期范围）
python -c "import etl_dws_sales; etl_dws_sales.backfill('2026-01-01', '2026-01-31')"

# 库存快照
python -c "import etl_dws_inventory; etl_dws_inventory.run()"

# 库存健康度重算
python -c "import etl_ads_health; etl_ads_health.run()"

# 若刚发生手工重跑或调度重叠，先等待前一轮完成再重算，避免锁冲突
# dws_sales / dws_inventory / ads_health 现在都内置命名锁与死锁重试，但仍不建议并发重复触发
```

### 2.4 数据质检

```powershell
# 通用质检
python tools/check_data.py

# 库存质检
python tools/check_dws_inventory.py

# ODS 增量对账（检查水位与行数）
python tools/check_ods_incremental.py

# ODS 明细质检（线上/线下拆分）
python tools/check_ods_retailitem_quality.py

# ODS 对账：指定截止时间（as-of）
python tools/check_ods_incremental.py --as-of "2026-03-01 08:00:00"
```

### 2.5 快照与导出

```powershell
# 生成 MySQL 数仓 Schema 快照
python tools/snapshot_mysql_hefangdw_schema.py

# 生成 Oracle ERP Schema 快照
python tools/snapshot_oracle_bosnds3_schema.py

# 导出 ADS 层数据到 Excel
python tools/export_ads.py
```

### 2.6 MCP 与只读查数

推荐顺序：MCP 优先，只读执行；若本地未配置 MCP 或连通失败，再降级到 Python 工具。

```powershell
# 查看内置查询模板
python tools/query_data.py --list-templates

# MySQL：最近 7 天销售排行
python tools/query_data.py --template mysql_sales_rank_7d

# Oracle：最近 7 天零售单据统计
python tools/query_data.py --source oracle --template oracle_retail_docs_7d

# 自由查数并导出 JSON
python tools/query_data.py --sql "SELECT snapshot_date, product_code, total_qty FROM ads_inventory_health WHERE snapshot_date = :dt" --param dt=20260318 --output json --output-path reports/query_result_ads_sample.json

# 导出指定快照的 ADS 数据
python tools/export_ads.py --snapshot-date 20260318 --output reports/output.xlsx
```

若需要对 `hfsy` 做一次性只读探查，可在独立终端临时覆盖 `MYSQL_*` 后执行：

```powershell
$env:MYSQL_HOST='8.134.87.152'
$env:MYSQL_PORT='33066'
$env:MYSQL_USER='shuyun668'
$env:MYSQL_PASSWORD='请通过本地安全方式注入真实密码'
$env:MYSQL_DB='hfsy'
python tools/query_data.py --source mysql --sql "SELECT COUNT(*) AS row_cnt FROM t_member_info"
```

结构快照命令：

```powershell
python tools/snapshot_mysql_hefangdw_schema.py --output reports/snapshot_mysql_hefangdw_schema.json
python tools/snapshot_oracle_bosnds3_schema.py --output reports/snapshot_oracle_bosnds3_schema.json
```

MCP 推荐配置片段：

```json
{
    "mcpServers": {
        "mysql": {
            "command": "npx",
            "args": [
                "-y",
                "@benborla29/mcp-server-mysql"
            ],
            "env": {
                "MYSQL_HOST": "${MYSQL_HOST}",
                "MYSQL_PORT": "${MYSQL_PORT}",
                "MYSQL_USER": "${MYSQL_USER}",
                "MYSQL_PASS": "${MYSQL_PASSWORD}",
                "MYSQL_DB": "${MYSQL_DB}"
            }
        },
        "oracle": {
            "command": "uvx",
            "args": [
                "mcp-server-oracle"
            ],
            "env": {
                "ORACLE_CONNECTION_STRING": "${ORACLE_CONNECTION_STRING}",
                "ORACLE_SCHEMA": "BOSNDS3"
            }
        }
    }
}
```

说明：
- 当前 VS Code / Copilot 会话优先读取工作区 `.vscode/mcp.json` 与用户级 `mcp.json`；仓库根 `.mcp.json` 更适合作为 Claude/OpenCode 的兼容或本地参考配置。
- MCP 更适合交互式查数与结构探查；`tools/query_data.py` 适合作为稳定兜底和导出工具。
- 快照脚本只输出结构信息，不读取业务数据值。

### 2.7 文档审计

```powershell
# 检查代码与文档是否同步
python scripts/check_doc_sync.py
```

### 2.8 经验台帐

当一次排障形成可复用结论，或用户明确纠正业务逻辑/字段语义/SQL 口径时，追加一条经验记录：

```powershell
python scripts/log_agent_lesson.py --source task --category field-mapping --trigger "Oracle 查询字段报错" --mistake "误以为 M_PRODUCT 存在 NAME_CN" --correction "以 etl_dim_product.py 为准：NAME=product_code，VALUE=product_name" --evidence "etl_dim_product.py#L33" "etl_dim_product.py#L34" --prevention "涉及源表字段时，先对照 ETL 抽取 SQL、快照或字段映射文档"

python scripts/log_agent_lesson.py --source user-feedback --category business-rule --trigger "用户指出销售口径错误" --mistake "误把业务常量或字段语义当作既定事实" --correction "以用户确认后的业务结论为准，并同步相关文档" --evidence "docs/业务逻辑与指标规范.md#L1" --prevention "涉及业务口径变更前先确认，不凭历史经验直接改"
```

说明：
- 经验台帐文件是 `docs/AGENT_LESSONS.md`。
- 与当前仓库强相关的经验，除落盘台帐外，还应同步到 repo memory。
- `.claude/settings.json` 已增加复盘提醒 Hook，但当前仓库内没有对 GitHub Copilot 会话结束的硬触发钩子，因此仍需在任务收尾时主动判断是否要记账。

### 2.9 验收测试

```powershell
# 完整自动化验收测试（需要数据库已有数据）
python test_etl_automation.py

# Oracle 对账重点：dws_inventory / dws_sales / ads_health 默认按 0.5% 误差阈值输出结果

# 通过 pytest 运行
pytest test_etl_automation.py -v
```

---

## 3. 日志说明

| 日志文件 | 内容 | 保留策略 |
|---------|------|---------|
| `logs/etl_<日期>.log` | 主 ETL 流水线执行日志 | 不 git 追踪，本地保留 |
| `logs/ods_qc_<日期时间>.log` | ODS 质检日志 | 不 git 追踪，本地保留 |
| `logs/conn_test_<日期>.log` | 连通测试日志 | 不 git 追踪，本地保留 |

查看最新日志：
```powershell
# PowerShell 查看最新 ETL 日志（最后 100 行）
Get-Content (Get-ChildItem logs/etl_*.log | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName -Tail 100
```

---

## 4. 常见报错与处理

### 4.1 Oracle 连接失败

**报错**：`ORA-01017: invalid username/password`
```powershell
# 原因：环境变量未配置或密码错误
# 排查
[System.Environment]::GetEnvironmentVariable('ORACLE_USER', 'User')
[System.Environment]::GetEnvironmentVariable('ORACLE_PASSWORD', 'User')
# 处理：重新设置环境变量，重启终端后测试
python tools/test_connection.py
```

**报错**：`DPY-6001: cannot connect to database` / `Connection refused`
```powershell
# 原因：VPN 未连接或 Oracle 主机/端口错误
# 排查
Test-NetConnection -ComputerName $env:ORACLE_HOST -Port $env:ORACLE_PORT
```

### 4.2 MySQL 连接失败

**报错**：`Access denied for user`
```powershell
# 排查：确认 MYSQL_USER / MYSQL_PASSWORD 环境变量
python -c "import os; print(os.getenv('MYSQL_USER'), os.getenv('MYSQL_HOST'))"
```

**报错**：`Unknown database 'hefang_dw'`
```sql
-- MySQL 中手动创建数据库
CREATE DATABASE hefang_dw CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
-- 然后执行建表脚本
-- MySQL Workbench 或命令行运行 SQL/create_ods_tables.sql
```

### 4.3 ODS 水位异常

**现象**：ODS 增量对账报告行数差异 > 阈值
```powershell
# 1. 查看质检日志
Get-Content logs/ods_qc_*.log -Tail 50

# 2. 手动对账（指定截止时间）
python tools/check_ods_incremental.py --as-of "2026-03-01 08:00:00"

# 3. 如确认需要重刷，执行全量
python run_ods.py --full
```

### 4.4 ADS 库存健康度异常

**现象**：`ads_inventory_health` 行数骤降或 SKU 缺失
```powershell
# 1. 检查 dws_inventory 是否正常
python tools/check_dws_inventory.py

# 2. 检查 dim_product / dim_sku 维度是否有数据
python -c "
import pymysql, os
conn = pymysql.connect(host=os.getenv('MYSQL_HOST'), user=os.getenv('MYSQL_USER'),
    password=os.getenv('MYSQL_PASSWORD'), database=os.getenv('MYSQL_DB','hefang_dw'))
c = conn.cursor()
c.execute('SELECT COUNT(*) FROM dim_product')
print('dim_product:', c.fetchone()[0])
c.execute('SELECT COUNT(*) FROM dim_sku')
print('dim_sku:', c.fetchone()[0])
conn.close()
"

# 3. 强制重算 ADS
python -c "import etl_ads_health; etl_ads_health.run()"

# 4. 如果日志出现 1213 / 1205，先确认是否有别的 ETL 会话仍在跑
python -c "import pymysql, os; conn=pymysql.connect(host=os.getenv('MYSQL_HOST'), user=os.getenv('MYSQL_USER'), password=os.getenv('MYSQL_PASSWORD'), database=os.getenv('MYSQL_DB','hefang_dw')); c=conn.cursor(); c.execute('SHOW FULL PROCESSLIST'); [print(r) for r in c.fetchall()]; conn.close()"
```

补充说明：
- `etl_dws_inventory.py` 现已在写入 `dws_inventory_daily` 前申请命名锁，并对死锁/锁等待超时做最多 3 次退避重试。
- `etl_dws_sales.py` 现已在覆盖写入 `dws_sales_daily` 前申请命名锁，并对死锁/锁等待超时做最多 3 次退避重试。
- `etl_ads_health.py` 现已将“删除当天数据 + 插入新结果”放入同一事务；若插入失败，不会留下当天 ADS 被清空的中间态。
- `run_etl.py` 现已在 `dws_sales` 或 `dws_inventory` 未成功时跳过 `ads_health`，避免下游继续放大异常。
- `test_etl_automation.py` 现已对 `dws_inventory`、`dws_sales`、`ads_health` 输出 Oracle 对账百分比；2026-03-23 复测结果分别为 0.00%、0.39%/0.11%/0.07%、0.00%。

### 4.5 企业微信告警不通

**现象**：ETL 完成但未收到消息
```powershell
# 检查 Webhook 是否配置
[System.Environment]::GetEnvironmentVariable('WECHAT_WEBHOOK', 'User')

# 手动测试（PowerShell）
$body = '{"msgtype":"text","text":{"content":"测试消息"}}'
Invoke-RestMethod -Method Post -Uri $env:WECHAT_WEBHOOK -Body $body -ContentType 'application/json'
```

### 4.6 Python 编码问题

**现象**：日志或终端输出乱码
```powershell
# 临时修复（当前会话）
$env:PYTHONIOENCODING = 'utf-8'
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 永久修复
[Environment]::SetEnvironmentVariable('PYTHONIOENCODING', 'utf-8', 'User')
```

---

## 5. 数据库初始化（首次部署）

```sql
-- Step 1: 创建数据库
CREATE DATABASE hefang_dw CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Step 2: 执行 ODS 建表
SOURCE SQL/create_ods_tables.sql;

-- Step 3: 执行结构变更（按文件名顺序）
SOURCE SQL/alter_ods_incremental.sql;
-- 其余 alter_*.sql 按实际情况执行

-- Step 4: 首次全量同步 ODS
-- python run_ods.py --full

-- Step 5: 执行主 ETL 流水线
-- python run_etl.py
```

---

## 6. 验收步骤（上线前 / 每次重大变更后）

```powershell
# Step 1: 环境自检
pwsh scripts/doctor.ps1

# Step 2: 连通测试
python tools/test_connection.py

# Step 3: ETL 连通模式（不执行真实 ETL）
$env:ETL_CONN_TEST=1; $env:ETL_MAX_RETRIES=1; python run_etl.py --conn-test

# Step 4: ODS 质检
python tools/check_ods_incremental.py
python tools/check_ods_retailitem_quality.py

# Step 5: 数据质检
python tools/check_data.py

# Step 6: 自动化验收测试
python test_etl_automation.py

# Step 7: 文档同步审计
python scripts/check_doc_sync.py

# Step 8: （可选）ODS 增量对账
python tools/check_ods_incremental.py
```

**所有步骤无红色错误 = 可以上线**。

---

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v1.7 | 2026-03-23 | 补充 dws_sales 命名锁重试与 Oracle 对账 0.5% 阈值说明 |
| v1.6 | 2026-03-23 | 补充库存/ADS 命名锁重试、ADS 单事务覆盖与 1213/1205 排查说明 |
| v1.5 | 2026-03-20 | 补充 hfsy 源端连接事实、临时环境变量约定与只读探查示例 |
| v1.0 | 2026-03-18 | 初版运行手册 |
| v1.1 | 2026-03-18 | 新增 MCP 与只读查数说明、结构快照与导出命令 |
| v1.2 | 2026-03-18 | 新增经验台帐写入命令、复盘规则与 Hook 说明 |
| v1.3 | 2026-03-18 | 将 MCP 配置示例对齐为当前实际使用的 mcpServers 格式 |
| v1.4 | 2026-03-18 | 将查数与导出示例输出名改为通用占位，避免审计高风险误报 |
