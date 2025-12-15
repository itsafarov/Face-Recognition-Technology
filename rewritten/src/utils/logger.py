"""
Clean and improved logging system with rotating file handlers
"""
import os
import logging
import logging.handlers
from datetime import datetime
from pathlib import Path
from typing import Optional
import sys
import platform
import threading


def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5,
    log_dir: str = "logs"
) -> logging.Logger:
    """
    Setup application logging with rotating file handler
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Custom log file name
        max_bytes: Max size of log file before rotation
        backup_count: Number of backup files to keep
        log_dir: Directory for log files
    
    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger('FaceRecognitionApp')
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Create logs directory if it doesn't exist
    logs_path = Path(log_dir)
    logs_path.mkdir(exist_ok=True)
    
    # Generate log filename with timestamp
    if log_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"app_{timestamp}.log"
    
    log_file_path = logs_path / log_file
    
    # Create formatter
    detailed_formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(threadName)s | %(funcName)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Console handler with colored output
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    console_handler.setFormatter(ColoredFormatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S"
    ))
    
    # File handler with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        filename=str(log_file_path),
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)  # Log everything to file
    file_handler.setFormatter(detailed_formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    # Log initialization
    logger.info("=" * 80)
    logger.info(f"LOGGING SYSTEM INITIALIZED")
    logger.info(f"Platform: {platform.system()} {platform.release()}")
    logger.info(f"Python: {platform.python_version()}")
    logger.info(f"Log level: {log_level}")
    logger.info(f"Log file: {log_file_path}")
    logger.info(f"PID: {os.getpid()}")
    logger.info(f"Thread: {threading.current_thread().name}")
    logger.info("=" * 80)
    
    return logger


class ColoredFormatter(logging.Formatter):
    """Colored console formatter"""
    
    # Color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'        # Reset
    }
    
    def __init__(self, fmt=None, datefmt=None, style='%'):
        super().__init__(fmt, datefmt, style)
    
    def format(self, record):
        # Get original formatted message
        log_message = super().format(record)
        
        # Add color based on log level
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset = self.COLORS['RESET']
        
        # Apply color to the entire message
        return f"{color}{log_message}{reset}"


def get_logger(name: str = 'FaceRecognitionApp') -> logging.Logger:
    """Get logger by name"""
    return logging.getLogger(name)


def log_system_info(logger: logging.Logger):
    """Log detailed system information"""
    import psutil
    
    try:
        # Memory info
        memory = psutil.virtual_memory()
        disk_usage = psutil.disk_usage('.')
        
        logger.info("SYSTEM INFORMATION")
        logger.info(f"  OS: {platform.system()} {platform.release()}")
        logger.info(f"  Architecture: {platform.architecture()[0]}")
        logger.info(f"  Python: {platform.python_version()}")
        logger.info(f"  CPU Cores: {psutil.cpu_count(logical=True)}")
        logger.info(f"  Memory Total: {memory.total / (1024**3):.2f} GB")
        logger.info(f"  Memory Available: {memory.available / (1024**3):.2f} GB")
        logger.info(f"  Memory Used: {memory.used / (1024**3):.2f} GB")
        logger.info(f"  Memory Percent: {memory.percent}%")
        logger.info(f"  Disk Total: {disk_usage.total / (1024**3):.2f} GB")
        logger.info(f"  Disk Free: {disk_usage.free / (1024**3):.2f} GB")
        logger.info(f"  Disk Used: {disk_usage.used / (1024**3):.2f} GB")
        logger.info(f"  Disk Percent: {disk_usage.percent}%")
        logger.info(f"  Current Directory: {os.getcwd()}")
        logger.info(f"  Process ID: {os.getpid()}")
    except Exception as e:
        logger.warning(f"Could not log system info: {e}")


def log_performance_metrics(logger: logging.Logger, metrics: dict):
    """Log performance metrics"""
    logger.info("PERFORMANCE METRICS")
    for key, value in metrics.items():
        logger.info(f"  {key}: {value}")


def log_error_details(logger: logging.Logger, error: Exception, context: str = ""):
    """Log detailed error information"""
    import traceback
    
    logger.error(f"ERROR DETAILS - {context}")
    logger.error(f"  Error Type: {type(error).__name__}")
    logger.error(f"  Error Message: {str(error)}")
    logger.error(f"  Traceback:")
    for line in traceback.format_tb(error.__traceback__):
        logger.error(f"    {line.strip()}")


# Global logger instance
app_logger: Optional[logging.Logger] = None


def get_global_logger() -> logging.Logger:
    """Get global logger instance"""
    global app_logger
    
    if app_logger is None:
        app_logger = setup_logging()
    
    return app_logger


def set_global_logger(logger: logging.Logger):
    """Set global logger instance"""
    global app_logger
    app_logger = logger


# Convenience functions
def debug(msg, *args, **kwargs):
    get_global_logger().debug(msg, *args, **kwargs)


def info(msg, *args, **kwargs):
    get_global_logger().info(msg, *args, **kwargs)


def warning(msg, *args, **kwargs):
    get_global_logger().warning(msg, *args, **kwargs)


def error(msg, *args, **kwargs):
    get_global_logger().error(msg, *args, **kwargs)


def critical(msg, *args, **kwargs):
    get_global_logger().critical(msg, *args, **kwargs)


def exception(msg, *args, **kwargs):
    get_global_logger().exception(msg, *args, **kwargs)


# Initialize global logger on import
get_global_logger()