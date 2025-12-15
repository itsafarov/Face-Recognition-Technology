"""
Настройка системы логирования
"""

import os
import logging
import platform

def setup_logging():
    """Настройка системы логирования"""
    # Очищаем существующие обработчики
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    logger = logging.getLogger("FaceRecognitionProcessor")
    logger.setLevel(logging.INFO)
    
    # Удаляем существующие обработчики у логгера
    logger.handlers.clear()
    
    class ColorFormatter(logging.Formatter):
        COLORS = {
            'INFO': '\033[92m',
            'WARNING': '\033[93m',
            'ERROR': '\033[91m',
            'CRITICAL': '\033[41m',
            'RESET': '\033[0m'
        }
        
        def format(self, record):
            msg = super().format(record)
            if platform.system() == "Windows":
                try:
                    os.system('')
                    color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
                    return f"{color}{msg}{self.COLORS['RESET']}"
                except:
                    return msg
            return msg
    
    console = logging.StreamHandler()
    console.setFormatter(ColorFormatter('%(asctime)s - %(levelname)s - %(message)s', '%H:%M:%S'))
    console.setLevel(logging.INFO)
    
    file_handler = logging.FileHandler("processing.log", encoding='utf-8', mode='w')
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    
    logger.addHandler(console)
    logger.addHandler(file_handler)
    
    return logger