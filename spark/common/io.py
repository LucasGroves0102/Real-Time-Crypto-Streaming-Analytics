# spark/common/io.py
import os

def bronze_trades_path(root="data"):
    return os.path.join(root, "bronze", "trades")

def bronze_trades_dlq_path(root="data"):
    return os.path.join(root, "bronze", "trades_dlq")

def bronze_checkpoint_path():
    return os.path.join("checkpoints", "bronze", "trades")

def bronze_dlq_checkpoint_path():
    return os.path.join("checkpoints", "bronze", "trades_dlq")
