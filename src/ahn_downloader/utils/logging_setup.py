import logging
import sys
from ..config import ERROR_LOG

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(ERROR_LOG),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger("ahn_downloader")
