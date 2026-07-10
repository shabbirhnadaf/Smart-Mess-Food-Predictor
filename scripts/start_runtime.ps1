param(
    [string]$JavaHome = "C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot",
    [string]$HadoopHome = "C:\hadoop\hadoop-3.4.3",
    [string]$KafkaHome = "C:\kafka\kafka_2.13-3.7.2",
    [string]$ScalaHome = "C:\scala\scala-2.13.18",
    [string]$SparkHome = "C:\spark\spark-3.5.8-bin-hadoop3",
    [switch]$RetrainModels
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$DashboardRoot = Join-Path $RepoRoot "dashboard"
$VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$HadoopConfDir = Join-Path $RepoRoot "config\hadoop"
$SparkConfDir = Join-Path $RepoRoot "config\spark"
$RuntimeLogDir = Join-Path $RepoRoot "tmp\runtime-logs"
$NpmCommand = $null

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Assert-Path {
    param([string]$PathValue, [string]$Label)
    if (-not (Test-Path $PathValue)) {
        throw "$Label not found: $PathValue"
    }
}

function Resolve-PythonCommand {
    param([string]$PreferredPath)

    if (Test-Path $PreferredPath) {
        return $PreferredPath
    }

    $pythonCmd = Get-Command python -ErrorAction SilentlyContinue
    if ($pythonCmd) {
        return $pythonCmd.Source
    }

    throw "Python executable not found. Create .venv or install python on PATH."
}

function Resolve-NpmCommand {
    param([string]$PreferredPath)

    if ($PreferredPath -and (Test-Path $PreferredPath)) {
        return $PreferredPath
    }

    if (Test-Path "C:\Program Files\nodejs\npm.cmd") {
        return "C:\Program Files\nodejs\npm.cmd"
    }

    $npmCmd = Get-Command npm -ErrorAction SilentlyContinue
    if ($npmCmd -and $npmCmd.Source -and $npmCmd.Source.ToLower().EndsWith("npm.cmd")) {
        return $npmCmd.Source
    }

    return "npm.cmd"
}

function Test-PortListening {
    param([int]$Port)
    return [bool](Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue | Where-Object { $_.State -eq "Listen" })
}

function Wait-ForPort {
    param(
        [int]$Port,
        [int]$TimeoutSeconds = 30
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if (Test-PortListening -Port $Port) {
            return $true
        }
        Start-Sleep -Seconds 2
    }
    return $false
}

function Start-BackgroundCommand {
    param(
        [string]$Name,
        [string]$Command,
        [string]$WorkingDirectory = $RepoRoot
    )

    $logPath = Join-Path $RuntimeLogDir ($Name + ".log")
    $pathValue = "$JavaHome\bin;$HadoopHome\bin;$HadoopHome\sbin;$KafkaHome\bin\windows;$SparkHome\bin;$ScalaHome\bin;$env:PATH"
    $commandLine =
        'set JAVA_HOME=' + $JavaHome +
        '&& set HADOOP_HOME=' + $HadoopHome +
        '&& set HADOOP_CONF_DIR=' + $HadoopConfDir +
        '&& set KAFKA_HOME=' + $KafkaHome +
        '&& set SCALA_HOME=' + $ScalaHome +
        '&& set SPARK_HOME=' + $SparkHome +
        '&& set SPARK_CONF_DIR=' + $SparkConfDir +
        '&& set PATH=' + $pathValue +
        '&& cd /d "' + $WorkingDirectory + '"' +
        ' && ' + $Command +
        ' > "' + $logPath + '" 2>&1'

    Start-Process cmd.exe -ArgumentList "/c", $commandLine -WindowStyle Hidden | Out-Null
}

function Remove-StaleKafkaBrokerNode {
    if (-not (Test-Path $ZooKeeperShell)) {
        return
    }
    if (Test-PortListening -Port 9092) {
        return
    }
    try {
        $cleanupCmd = "echo delete /brokers/ids/0 | `"$ZooKeeperShell`" localhost:2181"
        cmd.exe /c $cleanupCmd | Out-Null
    } catch {
        Write-Host "Could not clean stale /brokers/ids/0 node automatically." -ForegroundColor Yellow
    }
}

Assert-Path $JavaHome "JAVA_HOME"
Assert-Path $HadoopHome "HADOOP_HOME"
Assert-Path $KafkaHome "KAFKA_HOME"
Assert-Path $ScalaHome "SCALA_HOME"
Assert-Path $SparkHome "SPARK_HOME"
$PythonCommand = Resolve-PythonCommand -PreferredPath $VenvPython
$NpmCommand = Resolve-NpmCommand -PreferredPath $NpmCommand

$env:JAVA_HOME = $JavaHome
$env:HADOOP_HOME = $HadoopHome
$env:HADOOP_CONF_DIR = $HadoopConfDir
$env:KAFKA_HOME = $KafkaHome
$env:SCALA_HOME = $ScalaHome
$env:SPARK_HOME = $SparkHome
$env:SPARK_CONF_DIR = $SparkConfDir
$env:PATH = "$JavaHome\bin;$HadoopHome\bin;$HadoopHome\sbin;$KafkaHome\bin\windows;$SparkHome\bin;$ScalaHome\bin;$env:PATH"
New-Item -ItemType Directory -Force -Path $RuntimeLogDir | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $RepoRoot "tmp\zookeeper") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $RepoRoot "tmp\kafka-logs") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $RepoRoot "tmp\hadoop\dfs\name") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $RepoRoot "tmp\hadoop\dfs\data") | Out-Null

$HdfsStart = Join-Path $HadoopHome "sbin\start-dfs.cmd"
$ZooKeeperStart = Join-Path $KafkaHome "bin\windows\zookeeper-server-start.bat"
$KafkaStart = Join-Path $KafkaHome "bin\windows\kafka-server-start.bat"
$KafkaTopics = Join-Path $KafkaHome "bin\windows\kafka-topics.bat"
$ZooKeeperShell = Join-Path $KafkaHome "bin\windows\zookeeper-shell.bat"
$ZooKeeperConfig = Join-Path $RepoRoot "config\kafka\zookeeper.properties"
$KafkaConfig = Join-Path $RepoRoot "config\kafka\server.properties"
$SparkSubmit = Join-Path $SparkHome "bin\spark-submit.cmd"

Write-Step "Starting HDFS"
& $HdfsStart
Start-Sleep -Seconds 6

Write-Step "Creating HDFS directories"
& hdfs dfs -mkdir -p /mess-predictor/raw/orders
& hdfs dfs -mkdir -p /mess-predictor/processed/features
& hdfs dfs -mkdir -p /mess-predictor/processed/streaming_agg
& hdfs dfs -mkdir -p /mess-predictor/models
& hdfs dfs -mkdir -p /mess-predictor/predictions/live
& hdfs dfs -mkdir -p /mess-predictor/checkpoints/stream_agg

if (-not (Test-PortListening -Port 2181)) {
    Write-Step "Starting ZooKeeper"
    Start-BackgroundCommand -Name "zookeeper" -Command ('call "' + $ZooKeeperStart + '" "' + $ZooKeeperConfig + '"')
    if (-not (Wait-ForPort -Port 2181 -TimeoutSeconds 40)) {
        throw "ZooKeeper did not start on port 2181."
    }
}
else {
    Write-Step "ZooKeeper already listening on 2181"
}

if (-not (Test-PortListening -Port 9092)) {
    Write-Step "Starting Kafka broker"
    Remove-StaleKafkaBrokerNode
    Start-BackgroundCommand -Name "kafka-broker" -Command ('call "' + $KafkaStart + '" "' + $KafkaConfig + '"')
    if (-not (Wait-ForPort -Port 9092 -TimeoutSeconds 40)) {
        Write-Step "Kafka first attempt failed, retrying once"
        Remove-StaleKafkaBrokerNode
        Start-BackgroundCommand -Name "kafka-broker" -Command ('call "' + $KafkaStart + '" "' + $KafkaConfig + '"')
        if (-not (Wait-ForPort -Port 9092 -TimeoutSeconds 40)) {
            throw "Kafka broker did not start on port 9092."
        }
    }
}
else {
    Write-Step "Kafka broker already listening on 9092"
}

Write-Step "Ensuring Kafka topics exist"
& $KafkaTopics --create --if-not-exists --bootstrap-server localhost:9092 --topic mess-orders --partitions 3 --replication-factor 1
& $KafkaTopics --create --if-not-exists --bootstrap-server localhost:9092 --topic weather-feed --partitions 1 --replication-factor 1
& $KafkaTopics --create --if-not-exists --bootstrap-server localhost:9092 --topic event-feed --partitions 1 --replication-factor 1

if ($RetrainModels) {
    Write-Step "Refreshing seed data and retraining models"
    & $PythonCommand (Join-Path $RepoRoot "data\seed\generate_seed_data.py")
    & $SparkSubmit --master local[*] (Join-Path $RepoRoot "spark\batch_feature_engineering.py")
    & $SparkSubmit --master local[*] (Join-Path $RepoRoot "spark\train_demand_model.py")
    & $PythonCommand (Join-Path $RepoRoot "ml\prophet_trainer.py")
}
else {
    Write-Step "Skipping retraining and seed generation"
}

Write-Step "Starting long-running project services"
Start-BackgroundCommand -Name "order-producer" -Command ('"' + $PythonCommand + '" "' + (Join-Path $RepoRoot "kafka\producer\order_producer.py") + '" --rate 5')
Start-BackgroundCommand -Name "weather-producer" -Command ('"' + $PythonCommand + '" "' + (Join-Path $RepoRoot "kafka\producer\weather_producer.py") + '"')
Start-BackgroundCommand -Name "event-producer" -Command ('"' + $PythonCommand + '" "' + (Join-Path $RepoRoot "kafka\producer\event_producer.py") + '"')
Start-BackgroundCommand -Name "streaming-aggregator" -Command ('"' + $SparkSubmit + '" --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 "' + (Join-Path $RepoRoot "spark\streaming_aggregator.py") + '"')
Start-BackgroundCommand -Name "api" -Command ('"' + $PythonCommand + '" -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload')
Start-BackgroundCommand -Name "dashboard" -Command ('call "' + $NpmCommand + '" start') -WorkingDirectory $DashboardRoot

Write-Step "Startup commands launched"
Write-Host "Dashboard: http://localhost:3000" -ForegroundColor Green
Write-Host "API:       http://localhost:8000" -ForegroundColor Green
Write-Host "HDFS UI:   http://localhost:9870" -ForegroundColor Green
Write-Host "Logs:      $RuntimeLogDir" -ForegroundColor Green
