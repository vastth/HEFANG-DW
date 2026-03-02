#Requires -Version 5.1
<#
.SYNOPSIS
    何方珠宝数据仓库 - 环境自检脚本（HEFANG-DW Doctor）

.DESCRIPTION
    检查本地开发/运行环境是否满足项目要求。
    不执行真实数据库连接，仅检查可本地验证的项目。
    所有检查项输出 [PASS] / [WARN] / [FAIL] 标记。

.EXAMPLE
    pwsh scripts/doctor.ps1
    powershell -ExecutionPolicy Bypass -File scripts/doctor.ps1
#>

# ─────────────────────────────────────────
#  全局配置
# ─────────────────────────────────────────
$ErrorActionPreference = 'SilentlyContinue'
$PassCount = 0
$WarnCount = 0
$FailCount = 0

# ─────────────────────────────────────────
#  输出辅助函数
# ─────────────────────────────────────────
function Write-Pass {
    param([string]$Message)
    Write-Host "  [PASS] $Message" -ForegroundColor Green
    $script:PassCount++
}

function Write-Warn {
    param([string]$Message)
    Write-Host "  [WARN] $Message" -ForegroundColor Yellow
    $script:WarnCount++
}

function Write-Fail {
    param([string]$Message)
    Write-Host "  [FAIL] $Message" -ForegroundColor Red
    $script:FailCount++
}

function Write-Section {
    param([string]$Title)
    Write-Host ""
    Write-Host "── $Title ─────────────────────────────────────────" -ForegroundColor Cyan
}

# ─────────────────────────────────────────
#  主检查流程
# ─────────────────────────────────────────

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║    HEFANG-DW  环境自检脚本 (Doctor v1.0)        ║" -ForegroundColor Cyan
Write-Host "║    何方珠宝数据仓库 · 运行于 $(Get-Date -Format 'yyyy-MM-dd HH:mm')   ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════╝" -ForegroundColor Cyan

# ─── 1. Python 环境 ───────────────────────
Write-Section "1. Python 环境"

$pythonCmd = $null
foreach ($cmd in @('python', 'python3')) {
    $ver = & $cmd --version 2>&1
    if ($LASTEXITCODE -eq 0 -and $ver -match 'Python (\d+)\.(\d+)') {
        $major = [int]$Matches[1]
        $minor = [int]$Matches[2]
        $pythonCmd = $cmd
        if ($major -ge 3 -and $minor -ge 10) {
            Write-Pass "Python $($ver.ToString().Trim())（需要 3.10+）"
        } elseif ($major -ge 3) {
            Write-Warn "Python $($ver.ToString().Trim())（推荐 3.10+，当前版本可能兼容）"
        } else {
            Write-Fail "Python $($ver.ToString().Trim())（需要 Python 3.10+）"
        }
        break
    }
}
if ($null -eq $pythonCmd) {
    Write-Fail "未找到 python / python3 命令，请安装 Python 3.10+"
}

# pip 检查
if ($null -ne $pythonCmd) {
    $pipVer = & $pythonCmd -m pip --version 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Pass "pip 可用：$($pipVer.ToString().Split(' ')[0..2] -join ' ')"
    } else {
        Write-Warn "pip 不可用，请运行：python -m ensurepip"
    }
}

# ─── 2. 关键 Python 包 ──────────────────
Write-Section "2. 关键 Python 包"

$requiredPackages = @(
    @{ Name = 'oracledb';    ImportName = 'oracledb';    MinVersion = '' },
    @{ Name = 'pandas';      ImportName = 'pandas';      MinVersion = '' },
    @{ Name = 'sqlalchemy';  ImportName = 'sqlalchemy';  MinVersion = '' },
    @{ Name = 'pymysql';     ImportName = 'pymysql';     MinVersion = '' },
    @{ Name = 'requests';    ImportName = 'requests';    MinVersion = '' },
    @{ Name = 'openpyxl';    ImportName = 'openpyxl';    MinVersion = '' }
)

foreach ($pkg in $requiredPackages) {
    if ($null -ne $pythonCmd) {
        $result = & $pythonCmd -c "import $($pkg.ImportName); v=getattr($($pkg.ImportName),'__version__','?'); print(v)" 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Pass "$($pkg.Name) 已安装（版本：$result）"
        } else {
            Write-Fail "$($pkg.Name) 未安装 → 运行：pip install $($pkg.Name)"
        }
    }
}

# ─── 3. 环境变量（凭据类）─────────────
Write-Section "3. 环境变量（凭据类，仅检查是否已设置）"

$credVars = @(
    @{ Name = 'ORACLE_USER';     Layer = 'Oracle' },
    @{ Name = 'ORACLE_PASSWORD'; Layer = 'Oracle' },
    @{ Name = 'ORACLE_HOST';     Layer = 'Oracle' },
    @{ Name = 'ORACLE_PORT';     Layer = 'Oracle' },
    @{ Name = 'ORACLE_SERVICE';  Layer = 'Oracle' },
    @{ Name = 'MYSQL_HOST';      Layer = 'MySQL'  },
    @{ Name = 'MYSQL_PORT';      Layer = 'MySQL'  },
    @{ Name = 'MYSQL_USER';      Layer = 'MySQL'  },
    @{ Name = 'MYSQL_PASSWORD';  Layer = 'MySQL'  },
    @{ Name = 'MYSQL_DB';        Layer = 'MySQL'  }
)

foreach ($v in $credVars) {
    # 优先读 Process 级（当前会话），再读 User 级
    $val = [System.Environment]::GetEnvironmentVariable($v.Name, 'Process')
    if ([string]::IsNullOrWhiteSpace($val)) {
        $val = [System.Environment]::GetEnvironmentVariable($v.Name, 'User')
    }
    if ([string]::IsNullOrWhiteSpace($val)) {
        Write-Fail "[$($v.Layer)] $($v.Name) 未设置 → 参考 .env.example"
    } elseif ($val -match 'change_me|your_pass|your_user') {
        Write-Warn "[$($v.Layer)] $($v.Name) 仍为占位符值，请填写真实凭据"
    } else {
        # 密码类只显示长度，不显示内容
        if ($v.Name -match 'PASSWORD|password') {
            Write-Pass "[$($v.Layer)] $($v.Name) 已设置（已隐藏，长度 $($val.Length)）"
        } else {
            Write-Pass "[$($v.Layer)] $($v.Name) = $val"
        }
    }
}

# ─── 4. 环境变量（行为类）─────────────
Write-Section "4. 环境变量（行为类）"

$behaviorVars = @(
    @{ Name = 'WECHAT_WEBHOOK';  Required = $false; Desc = '企业微信告警 Webhook（可选）' },
    @{ Name = 'ETL_MAX_RETRIES'; Required = $false; Desc = '重试次数（默认 3）' },
    @{ Name = 'ETL_RETRY_SLEEP'; Required = $false; Desc = '重试间隔秒数（默认 60）' },
    @{ Name = 'ETL_CONN_TEST';   Required = $false; Desc = '连通测试模式（1=跳过真实 ETL）' }
)

foreach ($v in $behaviorVars) {
    $val = [System.Environment]::GetEnvironmentVariable($v.Name, 'Process')
    if ([string]::IsNullOrWhiteSpace($val)) {
        $val = [System.Environment]::GetEnvironmentVariable($v.Name, 'User')
    }
    if ([string]::IsNullOrWhiteSpace($val)) {
        if ($v.Required) {
            Write-Fail "$($v.Name) 未设置（$($v.Desc)）"
        } else {
            Write-Warn "$($v.Name) 未设置（$($v.Desc)，将使用默认值）"
        }
    } elseif ($v.Name -match 'WEBHOOK') {
        Write-Pass "$($v.Name) 已设置（已隐藏）"
    } else {
        Write-Pass "$($v.Name) = $val（$($v.Desc)）"
    }
}

# ─── 5. 网络端口可达性（仅提示，不真实连库）──
Write-Section "5. 网络端口可达性（提示）"

Write-Host "  [INFO] 以下命令可手动验证网络连通（本脚本不执行真实连库）：" -ForegroundColor Gray
Write-Host ""

$oracleHost = [System.Environment]::GetEnvironmentVariable('ORACLE_HOST', 'Process')
if ([string]::IsNullOrWhiteSpace($oracleHost)) {
    $oracleHost = [System.Environment]::GetEnvironmentVariable('ORACLE_HOST', 'User')
}
$oraclePort = [System.Environment]::GetEnvironmentVariable('ORACLE_PORT', 'Process')
if ([string]::IsNullOrWhiteSpace($oraclePort)) {
    $oraclePort = [System.Environment]::GetEnvironmentVariable('ORACLE_PORT', 'User')
}
if ([string]::IsNullOrWhiteSpace($oraclePort)) { $oraclePort = '1521' }

$mysqlHost = [System.Environment]::GetEnvironmentVariable('MYSQL_HOST', 'Process')
if ([string]::IsNullOrWhiteSpace($mysqlHost)) {
    $mysqlHost = [System.Environment]::GetEnvironmentVariable('MYSQL_HOST', 'User')
}
if ([string]::IsNullOrWhiteSpace($mysqlHost)) { $mysqlHost = 'localhost' }

Write-Host "  # Oracle 端口测试：" -ForegroundColor Gray
if (-not [string]::IsNullOrWhiteSpace($oracleHost)) {
    Write-Host "  Test-NetConnection -ComputerName '$oracleHost' -Port $oraclePort" -ForegroundColor DarkGray
} else {
    Write-Host "  Test-NetConnection -ComputerName '<ORACLE_HOST>' -Port 1521" -ForegroundColor DarkGray
}
Write-Host ""
Write-Host "  # MySQL 端口测试：" -ForegroundColor Gray
Write-Host "  Test-NetConnection -ComputerName '$mysqlHost' -Port 3306" -ForegroundColor DarkGray
Write-Host ""
Write-Host "  # 真实连通测试（需 Python 环境与凭据）：" -ForegroundColor Gray
Write-Host "  python tools/test_connection.py" -ForegroundColor DarkGray

# ─── 6. 仓库文件完整性 ───────────────
Write-Section "6. 仓库关键文件完整性"

# 脚本路径相对于仓库根目录
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot  = Split-Path -Parent $scriptDir

$requiredFiles = @(
    'config.py',
    'alerts.py',
    'run_etl.py',
    'run_ods.py',
    'scheduled_etl.py',
    'run_scheduled_etl.bat',
    'etl_ods_fa_storage.py',
    'etl_ods_m_retail.py',
    'etl_ods_m_retailitem.py',
    'etl_dim_product.py',
    'etl_dim_sku.py',
    'etl_dim_store.py',
    'etl_dws_sales.py',
    'etl_dws_inventory.py',
    'etl_ads_health.py',
    'test_etl_automation.py',
    '.env.example',
    'SQL/create_ods_tables.sql',
    'tools/test_connection.py',
    'tools/check_data.py',
    'tools/check_ods_incremental.py',
    'docs/ARCHITECTURE.md',
    'docs/RUNBOOK.md',
    'docs/DATA_CONTRACTS.md',
    '.claude/CLAUDE.md'
)

foreach ($file in $requiredFiles) {
    $fullPath = Join-Path $repoRoot $file
    if (Test-Path $fullPath) {
        Write-Pass "$file"
    } else {
        Write-Fail "$file 不存在（仓库文件缺失）"
    }
}

# 检查 .env 文件安全性（不应被 git 追踪）
$envFile = Join-Path $repoRoot '.env'
if (Test-Path $envFile) {
    Write-Warn ".env 文件存在，请确认已加入 .gitignore（运行 git check-ignore .env 验证）"
} else {
    Write-Pass ".env 文件不存在（未暴露敏感凭据）"
}

# settings.local.json 检查
$localSettings = Join-Path $repoRoot '.claude/settings.local.json'
if (Test-Path $localSettings) {
    Write-Warn ".claude/settings.local.json 存在，请确认已加入 .gitignore"
} else {
    Write-Pass ".claude/settings.local.json 不存在（或已被忽略）"
}

# ─── 7. .gitignore 安全检查 ──────────
Write-Section "7. .gitignore 安全配置"

$gitignorePath = Join-Path $repoRoot '.gitignore'
if (Test-Path $gitignorePath) {
    $gitignoreContent = Get-Content $gitignorePath -Raw

    $sensitivePatterns = @(
        @{ Pattern = '\.env';                          Desc = '.env 凭据文件' },
        @{ Pattern = 'settings\.local\.json';          Desc = '.claude/settings.local.json（本机代理）' },
        @{ Pattern = 'logs/';                          Desc = 'logs/ 日志目录' },
        @{ Pattern = '__pycache__';                    Desc = '__pycache__ Python 缓存' }
    )

    foreach ($item in $sensitivePatterns) {
        if ($gitignoreContent -match $item.Pattern) {
            Write-Pass ".gitignore 包含 $($item.Desc)"
        } else {
            Write-Warn ".gitignore 缺少 $($item.Desc) 规则"
        }
    }
} else {
    Write-Fail ".gitignore 文件不存在"
}

# ─── 汇总 ────────────────────────────
Write-Host ""
Write-Host "══════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  自检汇总：" -ForegroundColor Cyan
Write-Host "    PASS: $PassCount" -ForegroundColor Green
Write-Host "    WARN: $WarnCount" -ForegroundColor Yellow
Write-Host "    FAIL: $FailCount" -ForegroundColor Red
Write-Host "══════════════════════════════════════════════════════" -ForegroundColor Cyan

if ($FailCount -gt 0) {
    Write-Host ""
    Write-Host "  存在 FAIL 项，请先修复后再运行 ETL。" -ForegroundColor Red
    Write-Host "  参考：docs/RUNBOOK.md §1 环境准备" -ForegroundColor Red
    exit 1
} elseif ($WarnCount -gt 0) {
    Write-Host ""
    Write-Host "  存在 WARN 项，建议处理后继续（不阻塞运行）。" -ForegroundColor Yellow
    exit 0
} else {
    Write-Host ""
    Write-Host "  全部通过！可以运行 ETL。" -ForegroundColor Green
    Write-Host "  下一步：python tools/test_connection.py" -ForegroundColor Green
    exit 0
}
