import logging
import sys
from pathlib import Path
from config import TRAINIG_CRAWL_LOG_PATH, CRAWL_LOG_PATH
def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # formatter
    fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    date_fmt = "%Y-%m-%d %H:%M:%S"

    # console handler (with color)
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG)

    try:
        import colorlog
        color_fmt = (
            "%(log_color)s%(asctime)s | %(levelname)-8s%(reset)s |"
            "%(cyan)s%(name)s%(reset)s | %(message)s"
        )
        console.setFormatter(colorlog.ColoredFormatter(
            color_fmt,
            datefmt=date_fmt,
            log_colors={
                "DEBUG": "white",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red"
            }
        ))
    except ImportError:
        console.setFormatter(logging.Formatter(
            fmt, 
            datefmt=date_fmt))
    
    #file handler
    file_handler = logging.FileHandler(CRAWL_LOG_PATH, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(fmt, datefmt=date_fmt))

    logger.addHandler(console)
    logger.addHandler(file_handler)

    return logger

