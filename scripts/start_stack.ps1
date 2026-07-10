Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$DashboardRoot = Join-Path $RepoRoot "dashboard"
$Python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$SparkSubmit = if ($env:SPARK_HOME) { Join-Path $env:SPARK_HOME "bin\spark-submit.cmd" } else { "spark-submit" }

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Require-Path {
    param([string]$Value, [string]$Name)
    if ([string]::IsNullOrWhiteSpace($Value)) {
        throw "$Name is not set."
    }
}

function Start-CommandWindow {
    param(
        [string]$Title,
        [string]$Command
    )

    Start-Process powershell -ArgumentList @(
        "-NoExit",
        "-Command",
        "$host.UI.RawUI.WindowTitle = '$Title'; Set-Location '$RepoRoot'; $Command"
    )
}

# Require-Path $env:HADOOP_HOME "HADOOP_HOME"
Require-Path $env:KAFKA_HOME "KAFKA_HOME"

if (-not (Test-Path $Python)) {
    throw "Python environment not found. Run .\scripts\first_run.ps1 first."
}

$HdfsStart = Join-Path $env:HADOOP_HOME "sbin\start-dfs.cmd"
$ZooKeeperStart = Join-Path $env:KAFKA_HOME "bin\windows\zookeeper-server-start.bat"
$KafkaStart = Join-Path $env:KAFKA_HOME "bin\windows\kafka-server-start.bat"
$KafkaTopics = Join-Path $env:KAFKA_HOME "bin\windows\kafka-topics.bat"
$ZooKeeperConfig = Join-Path $RepoRoot "config\kafka\zookeeper.properties"
$KafkaConfig = Join-Path $RepoRoot "config\kafka\server.properties"

Write-Step "Creating HDFS mock directory layout locally"
New-Item -ItemType Directory -Force -Path "$RepoRoot\data\hdfs_mock\raw\orders" | Out-Null
New-Item -ItemType Directory -Force -Path "$RepoRoot\data\hdfs_mock\processed\features" | Out-Null
New-Item -ItemType Directory -Force -Path "$RepoRoot\data\hdfs_mock\processed\streaming_agg" | Out-Null
New-Item -ItemType Directory -Force -Path "$RepoRoot\data\hdfs_mock\models" | Out-Null
New-Item -ItemType Directory -Force -Path "$RepoRoot\data\hdfs_mock\predictions\live" | Out-Null
New-Item -ItemType Directory -Force -Path "$RepoRoot\data\hdfs_mock\checkpoints\stream_agg" | Out-Null

Write-Step "Starting ZooKeeper and Kafka broker"
Start-CommandWindow -Title "mess-zookeeper" -Command "& '$ZooKeeperStart' '$ZooKeeperConfig'"
Start-Sleep -Seconds 15
Start-CommandWindow -Title "mess-kafka-broker" -Command "& '$KafkaStart' '$KafkaConfig'"
Start-Sleep -Seconds 20

Write-Step "Creating Kafka topics"
& $KafkaTopics --create --if-not-exists --bootstrap-server localhost:9092 --topic mess-orders --partitions 3 --replication-factor 1
& $KafkaTopics --create --if-not-exists --bootstrap-server localhost:9092 --topic weather-feed --partitions 1 --replication-factor 1
& $KafkaTopics --create --if-not-exists --bootstrap-server localhost:9092 --topic event-feed --partitions 1 --replication-factor 1

Write-Step "Generating seed data and training inputs"
& $Python (Join-Path $RepoRoot "data\seed\generate_seed_data.py")
& $SparkSubmit --master local[*] (Join-Path $RepoRoot "spark\batch_feature_engineering.py")
& $SparkSubmit --master local[*] (Join-Path $RepoRoot "spark\train_demand_model.py")
& $Python (Join-Path $RepoRoot "ml\prophet_trainer.py")

Write-Step "Starting long-running services"
Start-CommandWindow -Title "mess-order-producer" -Command "& '$Python' '$RepoRoot\kafka\producer\order_producer.py' --rate 5"
Start-CommandWindow -Title "mess-weather-producer" -Command "& '$Python' '$RepoRoot\kafka\producer\weather_producer.py'"
Start-CommandWindow -Title "mess-event-producer" -Command "& '$Python' '$RepoRoot\kafka\producer\event_producer.py'"
Start-CommandWindow -Title "mess-streaming-aggregator" -Command "& '$SparkSubmit' --master local[*] '$RepoRoot\spark\streaming_aggregator.py'"
Start-CommandWindow -Title "mess-api" -Command "& '$Python' -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload"
Start-CommandWindow -Title "mess-dashboard" -Command "Set-Location '$DashboardRoot'; npm start"

Write-Step "Stack startup triggered"
Write-Host "Open the dashboard at http://localhost:3000 once the frontend finishes compiling." -ForegroundColor Green
