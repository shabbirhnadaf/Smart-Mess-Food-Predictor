# ============================================================
# Smart Mess Food Demand Predictor  -  AUTO STARTUP SCRIPT
# Registers itself as a Windows Task Scheduler entry so the
# entire stack launches automatically after every reboot/login.
#
# FIRST TIME: Run once as Administrator:
#   powershell -ExecutionPolicy Bypass -File "C:\mess-predictor\scripts\auto_startup.ps1" -Register
#
# NORMAL USE: .\scripts\auto_startup.ps1
# ============================================================
param(
    [switch]$Register,
    [switch]$Unregister,
    [switch]$Status
)

$RepoRoot    = "C:\mess-predictor"
$JavaHome    = "C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot"
$HadoopHome  = "C:\hadoop\hadoop-3.4.3"
$KafkaHome   = "C:\kafka\kafka_2.13-3.7.2"
$ScalaHome   = "C:\scala\scala-2.13.18"
$SparkHome   = "C:\spark\spark-3.5.8-bin-hadoop3"
$TaskName    = "MessPredictorAutoStart"

# --- Register as a scheduled task (run once as Admin) ---
if ($Register) {
    $ScriptPath = $MyInvocation.MyCommand.Path
    $Action   = New-ScheduledTaskAction `
        -Execute "powershell.exe" `
        -Argument ("-WindowStyle Hidden -ExecutionPolicy Bypass -File `"" + $ScriptPath + "`"") `
        -WorkingDirectory $RepoRoot
    $Trigger  = New-ScheduledTaskTrigger -AtLogOn
    $Settings = New-ScheduledTaskSettingsSet `
        -StartWhenAvailable `
        -RunOnlyIfNetworkAvailable:$false `
        -MultipleInstances IgnoreNew
    Register-ScheduledTask `
        -TaskName  $TaskName `
        -Action    $Action `
        -Trigger   $Trigger `
        -Settings  $Settings `
        -RunLevel  Highest `
        -Force | Out-Null
    Write-Host "Scheduled task '$TaskName' registered - will auto-start on next login." -ForegroundColor Green
    exit 0
}

if ($Unregister) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "Scheduled task '$TaskName' removed." -ForegroundColor Yellow
    exit 0
}

if ($Status) {
    $ports = @{9092="Kafka"; 2181="ZooKeeper"; 8000="FastAPI"; 3000="React Dashboard"; 4040="Spark UI"}
    Write-Host "=== Service Status ===" -ForegroundColor Cyan
    foreach ($port in $ports.Keys | Sort-Object) {
        $listening = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
        if ($listening) {
            Write-Host "  $($ports[$port]) (port $port): RUNNING" -ForegroundColor Green
        } else {
            Write-Host "  $($ports[$port]) (port $port): STOPPED" -ForegroundColor Red
        }
    }
    exit 0
}

# ============================================================
#  MAIN STARTUP
# ============================================================
Set-Location $RepoRoot

$LogDir      = "$RepoRoot\tmp\runtime-logs"
$PythonExe   = "$RepoRoot\.venv\Scripts\python.exe"
$SparkSubmit = "$SparkHome\bin\spark-submit.cmd"
$ZKStart     = "$KafkaHome\bin\windows\zookeeper-server-start.bat"
$KBStart     = "$KafkaHome\bin\windows\kafka-server-start.bat"
$KTopics     = "$KafkaHome\bin\windows\kafka-topics.bat"
$ZKShell     = "$KafkaHome\bin\windows\zookeeper-shell.bat"
$ZKConfig    = "$RepoRoot\config\kafka\zookeeper.properties"
$KBConfig    = "$RepoRoot\config\kafka\server.properties"
$DashDir     = "$RepoRoot\dashboard"
$NpmExe = $null
$npmCommand = Get-Command npm -ErrorAction SilentlyContinue
if ($npmCommand -and $npmCommand.Source -and $npmCommand.Source.ToLower().EndsWith("npm.cmd")) {
    $NpmExe = $npmCommand.Source
} elseif (Test-Path "C:\Program Files\nodejs\npm.cmd") {
    $NpmExe = "C:\Program Files\nodejs\npm.cmd"
} else {
    $NpmExe = "npm.cmd"
}

# Set environment variables
$env:JAVA_HOME   = $JavaHome
$env:HADOOP_HOME = $HadoopHome
$env:KAFKA_HOME  = $KafkaHome
$env:SCALA_HOME  = $ScalaHome
$env:SPARK_HOME  = $SparkHome
$env:PATH        = "$JavaHome\bin;$HadoopHome\bin;$HadoopHome\sbin;$KafkaHome\bin\windows;$SparkHome\bin;$ScalaHome\bin;$env:PATH"

function Write-Step {
    param([string]$Msg)
    Write-Host ""
    Write-Host "==> $Msg" -ForegroundColor Cyan
}

function Is-PortListening {
    param([int]$Port)
    $result = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    return [bool]$result
}

function Wait-Port {
    param([int]$Port, [int]$Secs = 45)
    $end = (Get-Date).AddSeconds($Secs)
    while ((Get-Date) -lt $end) {
        if (Is-PortListening $Port) { return $true }
        Start-Sleep 3
    }
    return $false
}

function Launch-Background {
    param(
        [string]$Name,
        [string]$Cmd,
        [string]$Dir = $RepoRoot
    )
    $logFile = "$LogDir\$Name.log"
    $envBlock = "set JAVA_HOME=$JavaHome&&set HADOOP_HOME=$HadoopHome&&set KAFKA_HOME=$KafkaHome&&set SPARK_HOME=$SparkHome&&"
    $envBlock += "set PATH=$JavaHome\bin;$HadoopHome\bin;$KafkaHome\bin\windows;$SparkHome\bin;%PATH%&&"
    $fullCmd  = $envBlock + "cd /d `"$Dir`"&&" + $Cmd + " >`"$logFile`" 2>&1"
    Start-Process cmd.exe -ArgumentList "/c", $fullCmd -WindowStyle Hidden
    Write-Host "    Launched: $Name  (log: $logFile)" -ForegroundColor DarkGray
}

function Remove-StaleKafkaBrokerNode {
    if (-not (Test-Path $ZKShell)) {
        return
    }
    if (Is-PortListening 9092) {
        return
    }
    try {
        $cleanupCmd = "echo delete /brokers/ids/0 | `"$ZKShell`" localhost:2181"
        cmd.exe /c $cleanupCmd | Out-Null
        Write-Host "    Cleared stale ZooKeeper broker node (/brokers/ids/0) if present." -ForegroundColor DarkGray
    } catch {
        Write-Host "    Could not clear stale broker node automatically; continuing startup." -ForegroundColor Yellow
    }
}

# --- Create all required runtime directories ---
Write-Step "Preparing runtime directories"
$dirs = @(
    "tmp\runtime-logs",
    "tmp\checkpoints",
    "tmp\spark-local",
    "tmp\zookeeper",
    "tmp\kafka-logs",
    "data\streaming_output\demand_agg_live",
    "data\streaming_output\demand_agg",
    "data\streaming_output\hostel_agg",
    "data\hdfs_mock\mess-predictor\raw\orders",
    "data\hdfs_mock\mess-predictor\processed\features",
    "data\hdfs_mock\mess-predictor\processed\streaming_agg",
    "data\hdfs_mock\mess-predictor\models",
    "data\hdfs_mock\mess-predictor\checkpoints",
    "ml\saved_models\sarimax"
)
foreach ($d in $dirs) {
    New-Item -ItemType Directory -Force -Path "$RepoRoot\$d" | Out-Null
}
Write-Host "    All directories ready." -ForegroundColor DarkGray

# --- ZooKeeper ---
if (-not (Is-PortListening 2181)) {
    Write-Step "Starting ZooKeeper"
    Launch-Background "zookeeper" "call `"$ZKStart`" `"$ZKConfig`""
    if (Wait-Port 2181 45) {
        Write-Host "    ZooKeeper UP on :2181" -ForegroundColor Green
    } else {
        Write-Warning "ZooKeeper not ready in time - continuing anyway"
    }
} else {
    Write-Host ""
    Write-Host "==> ZooKeeper already running on :2181" -ForegroundColor Green
}

# --- Kafka Broker ---
if (-not (Is-PortListening 9092)) {
    Write-Step "Starting Kafka Broker"
    Remove-StaleKafkaBrokerNode
    Launch-Background "kafka-broker" "call `"$KBStart`" `"$KBConfig`""
    if (Wait-Port 9092 45) {
        Write-Host "    Kafka Broker UP on :9092" -ForegroundColor Green
    } else {
        Write-Host "    Kafka first attempt failed, retrying once after stale-node cleanup..." -ForegroundColor Yellow
        Remove-StaleKafkaBrokerNode
        Launch-Background "kafka-broker" "call `"$KBStart`" `"$KBConfig`""
        if (Wait-Port 9092 45) {
            Write-Host "    Kafka Broker UP on retry :9092" -ForegroundColor Green
        } else {
            throw "Kafka broker failed to start on port 9092. Check tmp\runtime-logs\kafka-broker.log"
        }
    }
} else {
    Write-Host ""
    Write-Host "==> Kafka already running on :9092" -ForegroundColor Green
}

# --- Kafka Topics ---
Write-Step "Ensuring Kafka topics exist"
Start-Sleep 3
& $KTopics --create --if-not-exists --bootstrap-server localhost:9092 --topic mess-orders  --partitions 3 --replication-factor 1 2>$null
& $KTopics --create --if-not-exists --bootstrap-server localhost:9092 --topic weather-feed --partitions 1 --replication-factor 1 2>$null
& $KTopics --create --if-not-exists --bootstrap-server localhost:9092 --topic event-feed   --partitions 1 --replication-factor 1 2>$null

# --- Kafka Producers ---
Write-Step "Starting Kafka Producers"
Launch-Background "order-producer"   "`"$PythonExe`" `"$RepoRoot\kafka\producer\order_producer.py`" --rate 5"
Launch-Background "weather-producer" "`"$PythonExe`" `"$RepoRoot\kafka\producer\weather_producer.py`""
Launch-Background "event-producer"   "`"$PythonExe`" `"$RepoRoot\kafka\producer\event_producer.py`""

# --- Spark Streaming Aggregator ---
Write-Step "Starting Spark Streaming Aggregator"
$sparkPkg = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
Launch-Background "streaming-aggregator" "`"$SparkSubmit`" --master local[2] --driver-memory 1500m --packages $sparkPkg `"$RepoRoot\spark\streaming_aggregator.py`""

# --- FastAPI ---
Write-Step "Starting FastAPI backend"
Launch-Background "api" "`"$PythonExe`" -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload"
Start-Sleep 5
if (Is-PortListening 8000) {
    Write-Host "    FastAPI UP on :8000" -ForegroundColor Green
} else {
    Write-Host "    FastAPI starting... (check tmp\runtime-logs\api.log)" -ForegroundColor Yellow
}

# --- React Dashboard ---
Write-Step "Starting React Dashboard"
Launch-Background "dashboard" "call `"$NpmExe`" start" $DashDir

# --- Summary ---
Write-Host ""
Write-Host "=============================================" -ForegroundColor Magenta
Write-Host "  Smart Mess Food Demand Predictor - LIVE" -ForegroundColor Magenta
Write-Host "=============================================" -ForegroundColor Magenta
Write-Host "  Dashboard : http://localhost:3000"         -ForegroundColor Green
Write-Host "  API       : http://localhost:8000"         -ForegroundColor Green
Write-Host "  API Docs  : http://localhost:8000/docs"    -ForegroundColor Green
Write-Host "  Spark UI  : http://localhost:4040"         -ForegroundColor Green
Write-Host "  Logs dir  : $LogDir"                       -ForegroundColor DarkGray
Write-Host "=============================================" -ForegroundColor Magenta
Write-Host ""
Write-Host "Services starting in background. Dashboard ready in ~60s." -ForegroundColor Yellow
