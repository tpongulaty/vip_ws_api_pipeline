import logging
from logging.handlers import RotatingFileHandler
import os

def setup_logger(name='wv_scraping'):
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    handler = RotatingFileHandler("logs/wv_scraping.log", maxBytes=5_000_000, backupCount=3)
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
    handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(handler)

    return logger