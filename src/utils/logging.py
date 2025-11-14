import logging
import os

def custom_logging(log_file: str):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Make sure logs directory exists
    os.makedirs("logs", exist_ok=True)

    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(log_file, encoding="utf-8")

    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger