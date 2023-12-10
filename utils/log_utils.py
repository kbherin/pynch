from os import environ
import logging

LOG_LEVEL = (environ.get("LOG_LEVEL") or "INFO").upper()
LOG_FORMAT = (environ.get("LOG_FORMAT") or "%(asctime)s %(name)s %(levelname)s - %(message)s")

def get_logger(name, log_level=LOG_LEVEL, log_format=LOG_FORMAT):
    handler = logging.StreamHandler()
    formatter = logging.Formatter(log_format)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger = logging.getLogger(name)
    logger.setLevel(logging._nameToLevel.get(log_level))
    return logger
