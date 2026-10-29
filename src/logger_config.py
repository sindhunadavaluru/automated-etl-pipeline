
import logging
from pathlib import Path


def setup_logger(project_root):
    """Configure logging to both a file and the terminal."""

    log_dir = Path(project_root) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / "etl_pipeline.log"

    logger = logging.getLogger("etl_pipeline")
    logger.setLevel(logging.INFO)

    # Avoid adding duplicate handlers if called more than once
    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s"
        )

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
