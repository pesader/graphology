import logging
import sys

# Create logger
logger = logging.getLogger("graphology")
logger.setLevel(logging.DEBUG)

# Formatter
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
)

# File handler
file_handler = logging.FileHandler("graphology.log")
file_handler.setFormatter(formatter)

# Stream (stdout) handler
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)

# Add handlers
logger.addHandler(file_handler)
logger.addHandler(stream_handler)


def log(level: int, timestamp: str, message: object):
    logger.log(level, f"(data timestamp: {timestamp}) {message}", stacklevel=2)
