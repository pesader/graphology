import logging
import sys
import uuid

RUN_ID = str(uuid.uuid4())[:8]

# Create logger
logger = logging.getLogger("graphology")
logger.setLevel(logging.DEBUG)

# Formatter
formatter = logging.Formatter(
    f"%(asctime)s - run:{RUN_ID} - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
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


def log(message: object, timestamp: str | None = None, level: int = logging.INFO):
    logger.log(
        level,
        f"timestamp:{timestamp} - {message}",
        stacklevel=2,
    )
