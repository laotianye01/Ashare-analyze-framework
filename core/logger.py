import logging
import os

def get_logger(name):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s')

        os.makedirs('logs', exist_ok=True)
        file_handler = logging.FileHandler(f'logs/{name}.log', encoding='utf-8')
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
    return logger
