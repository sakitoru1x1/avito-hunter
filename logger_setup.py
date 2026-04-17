import sys
import logging
import traceback
from logging.handlers import RotatingFileHandler

from config import LOG_FILE, MAX_LOG_SIZE, BACKUP_COUNT


logger = logging.getLogger("AvitoParser")
logger.setLevel(logging.INFO)

file_handler = RotatingFileHandler(LOG_FILE, maxBytes=MAX_LOG_SIZE, backupCount=BACKUP_COUNT, encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)


def setup_global_exception_handler():
    """Устанавливает глобальный перехват необработанных исключений."""
    from telegram import send_crash_report_to_telegram

    def global_exception_handler(exc_type, exc_value, exc_traceback):
        error_msg = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        logger.critical("Необработанное исключение:\n%s", error_msg)
        send_crash_report_to_telegram(error_msg)
        sys.__excepthook__(exc_type, exc_value, exc_traceback)

    sys.excepthook = global_exception_handler
