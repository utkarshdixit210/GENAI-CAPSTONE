"""
config/logger.py
Loguru logger — console (coloured) + rotating file sink.
"""
import sys
import os
from loguru import logger

os.makedirs("logs", exist_ok=True)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logger.remove()

logger.add(
    sys.stdout,
    level=LOG_LEVEL,
    format=(
        "<green>{time:HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{message}</cyan>"
    ),
    colorize=True,
)

logger.add(
    "logs/pipeline_{time:YYYYMMDD}.log",
    level="DEBUG",
    rotation="10 MB",
    retention="7 days",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
)
