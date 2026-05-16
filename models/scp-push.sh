#!/bin/bash

DEFAULT_PORT="35401"
DEFAULT_IP="142.188.39.36"

PORT=${1:-$DEFAULT_PORT}
IP=${2:-$DEFAULT_IP}

scp -P "$PORT" -r "./data/train.parquet" "root@$IP:/workspace/models/data/"
scp -P "$PORT" -r "./data/train_labels.csv" "root@$IP:/workspace/models/data/"
scp -P "$PORT" -r "./data/val.parquet" "root@$IP:/workspace/models/data/"
scp -P "$PORT" -r "./data/val_labels.csv" "root@$IP:/workspace/models/data/"
scp -P "$PORT" -r "./data/test.parquet" "root@$IP:/workspace/models/data/"
scp -P "$PORT" -r "./data/test_labels.csv" "root@$IP:/workspace/models/data/"
scp -P "$PORT" -r "./configs" "root@$IP:/workspace/models/"
scp -P "$PORT" -r "./loaders" "root@$IP:/workspace/models/"
scp -P "$PORT" -r "./main.py" "root@$IP:/workspace/models/"
scp -P "$PORT" -r "./models" "root@$IP:/workspace/models/"
scp -P "$PORT" -r "./scripts" "root@$IP:/workspace/models/"
scp -P "$PORT" -r "./utils" "root@$IP:/workspace/models/"
