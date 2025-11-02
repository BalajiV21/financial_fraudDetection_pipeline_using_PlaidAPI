import logging
import os
from datetime import datetime

def get_logger(name: str):
    """Create and return a logger with consistent formatting."""
    logs_dir = "logs"
    os.makedirs(logs_dir, exist_ok=True)

    log_file = os.path.join(logs_dir, f"{datetime.now():%Y-%m-%d}.log")

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Avoid adding handlers multiple times
    if not logger.handlers:
        file_handler = logging.FileHandler(log_file)
        stream_handler = logging.StreamHandler()

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )

        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger


# Example use:
if __name__ == "__main__":
    log = get_logger("TestLogger")
    log.info("Logger initialized correctly.")
