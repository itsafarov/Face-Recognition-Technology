"""
Конфигурация системы
"""

import os
import psutil
import platform
from datetime import datetime

class Config:
    """Конфигурация системы с контролем памяти"""
    
    # Версия программы
    VERSION = "13.0"
    
    # Размеры изображений
    THUMBNAIL_SIZE = (120, 120)  # Миниатюры в HTML
    PREVIEW_SIZE = (300, 300)    # Для просмотра
    IMAGE_QUALITY = 85
    
    # Производительность (оптимизировано для максимальной скорости)
    INITIAL_BATCH_SIZE = 1000  # Уменьшили для лучшей стабильности (было 8000)
    MAX_WORKERS = 15  # Увеличили для лучшей производительности
    REQUEST_TIMEOUT = 30  # Увеличили таймаут для обработки больших батчей
    REQUEST_RETRIES = 2  # Уменьшили количество попыток для скорости
    CHUNK_SIZE = 1024 * 1024 * 2  # 2MB для чтения файлов
    
    # Чекпоинты
    CHECKPOINT_INTERVAL = 100000  # Каждые 100к записей
    CHECKPOINT_FILE = "processing_checkpoint.json"
    
    # Контроль памяти
    MAX_MEMORY_PERCENT = 85  # Максимум 85% ОЗУ
    MEMORY_CHECK_INTERVAL = 500  # Проверять память каждые 500 записей
    
    # Лимиты для больших файлов
    MAX_IMAGE_SIZE_MB = 10  # Максимальный размер одного изображения
    MAX_CACHE_SIZE_MB = 800  # Увеличили максимальный размер кэша в памяти
    
    # Структура папок
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    INPUT_FOLDER = "input_data"
    OUTPUT_FOLDER = "output_results"
    IMAGE_FOLDER = "photos"
    REPORTS_FOLDER = "reports"
    CACHE_FOLDER = "image_cache"
    TEMP_FOLDER = "temp"
    
    # Настройки отчетов
    HTML_REPORT = "face_recognition_report.html"
    PDF_REPORT = "face_recognition_report.pdf"
    EXCEL_REPORT = "face_recognition_data.xlsx"
    SUMMARY_REPORT = "processing_summary.json"
    
    # Настройки прогресса
    PROGRESS_UPDATE_INTERVAL = 1000  # Обновлять прогресс каждые 1000 записей
    DETAILED_PROGRESS_INTERVAL = 10000  # Подробный прогресс каждые 10к
    
    # Настройки для разных операционных систем
    WINDOWS_MAX_WORKERS = 12
    LINUX_MAX_WORKERS = 25
    MACOS_MAX_WORKERS = 20
    
    @classmethod
    def init_config(cls):
        """Инициализация конфигурации в зависимости от ОС"""
        system = platform.system()
        
        # Настройка максимального количества рабочих процессов в зависимости от ОС
        if system == "Windows":
            cls.MAX_WORKERS = cls.WINDOWS_MAX_WORKERS
            cls.INITIAL_BATCH_SIZE = 3000  # Меньше для Windows
        elif system == "Linux":
            cls.MAX_WORKERS = cls.LINUX_MAX_WORKERS
            cls.INITIAL_BATCH_SIZE = 10000
        elif system == "Darwin":  # macOS
            cls.MAX_WORKERS = cls.MACOS_MAX_WORKERS
            cls.INITIAL_BATCH_SIZE = 5000
        else:
            cls.MAX_WORKERS = 15
            cls.INITIAL_BATCH_SIZE = 5000
        
        # Автоматическая настройка на основе доступной памяти
        memory_gb = psutil.virtual_memory().total / (1024**3)
        
        if memory_gb < 4:  # Менее 4 GB RAM
            cls.MAX_WORKERS = max(4, cls.MAX_WORKERS // 2)
            cls.INITIAL_BATCH_SIZE = 2000
            cls.MAX_CACHE_SIZE_MB = 200
        elif memory_gb < 8:  # 4-8 GB RAM
            cls.MAX_WORKERS = max(8, cls.MAX_WORKERS)
            cls.INITIAL_BATCH_SIZE = 4000
            cls.MAX_CACHE_SIZE_MB = 300
        elif memory_gb >= 16:  # 16+ GB RAM
            cls.MAX_WORKERS = min(30, cls.MAX_WORKERS + 5)
            cls.INITIAL_BATCH_SIZE = 8000
            cls.MAX_CACHE_SIZE_MB = 800
    
    @classmethod
    def get_available_memory(cls):
        """Получить доступную память в байтах"""
        total_memory = psutil.virtual_memory().total
        available_memory = total_memory * (cls.MAX_MEMORY_PERCENT / 100)
        return available_memory
    
    @classmethod
    def get_memory_usage_percent(cls):
        """Получить процент использования памяти"""
        try:
            return psutil.virtual_memory().percent
        except:
            return 50.0  # Значение по умолчанию при ошибке
    
    @classmethod
    def is_memory_safe(cls, additional_bytes=0):
        """Проверить, безопасно ли выделять дополнительную память"""
        try:
            used = psutil.virtual_memory().used + additional_bytes
            total = psutil.virtual_memory().total
            return (used / total) * 100 < cls.MAX_MEMORY_PERCENT
        except:
            return True  # При ошибке продолжаем работу
    
    @classmethod
    def get_disk_space_info(cls):
        """Получить информацию о свободном месте на диске"""
        try:
            disk_usage = psutil.disk_usage(cls.BASE_DIR)
            return {
                'total': disk_usage.total,
                'used': disk_usage.used,
                'free': disk_usage.free,
                'percent': disk_usage.percent
            }
        except:
            return {'total': 0, 'used': 0, 'free': 0, 'percent': 0}
    
    @classmethod
    def get_input_dir(cls):
        """Получить путь к папке с входными данными"""
        return os.path.join(cls.BASE_DIR, cls.INPUT_FOLDER)
    
    @classmethod
    def get_output_dir(cls):
        """Получить путь к папке для результатов"""
        return os.path.join(cls.BASE_DIR, cls.OUTPUT_FOLDER)
    
    @classmethod
    def get_output_subdir(cls, timestamp: str = None):
        """Получить путь к подпапке с результатами"""
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return os.path.join(cls.get_output_dir(), f"results_{timestamp}")
    
    @classmethod
    def setup_directories(cls, output_path: str = None):
        """Создание структуры папок"""
        if output_path is None:
            output_path = cls.get_output_subdir()
        
        folders = [
            os.path.join(output_path, cls.IMAGE_FOLDER),
            os.path.join(output_path, cls.REPORTS_FOLDER),
            os.path.join(output_path, cls.CACHE_FOLDER),
            os.path.join(output_path, cls.TEMP_FOLDER),
        ]
        
        for folder in folders:
            os.makedirs(folder, exist_ok=True)
        
        return output_path
    
    @classmethod
    def ensure_base_directories(cls):
        """Создание базовых директорий проекта"""
        base_folders = [
            cls.get_input_dir(),
            cls.get_output_dir()
        ]
        
        for folder in base_folders:
            os.makedirs(folder, exist_ok=True)
    
    @classmethod
    def get_system_info(cls):
        """Получить информацию о системе"""
        try:
            memory = psutil.virtual_memory()
            disk_info = cls.get_disk_space_info()
            
            return {
                'os': platform.system(),
                'os_version': platform.release(),
                'python_version': platform.python_version(),
                'cpu_count': psutil.cpu_count(),
                'cpu_logical_count': psutil.cpu_count(logical=True),
                'memory_total_gb': memory.total / (1024**3),
                'memory_available_gb': memory.available / (1024**3),
                'memory_used_gb': memory.used / (1024**3),
                'memory_percent': memory.percent,
                'disk_total_gb': disk_info['total'] / (1024**3),
                'disk_free_gb': disk_info['free'] / (1024**3),
                'disk_used_gb': disk_info['used'] / (1024**3),
                'disk_percent': disk_info['percent']
            }
        except Exception as e:
            print(f"⚠️ Ошибка получения информации о системе: {e}")
            return {
                'os': platform.system(),
                'os_version': platform.release(),
                'python_version': platform.python_version(),
                'cpu_count': 1,
                'memory_total_gb': 0,
                'memory_available_gb': 0,
                'memory_percent': 0,
                'disk_total_gb': 0,
                'disk_free_gb': 0,
                'disk_percent': 0
            }
    
    @classmethod
    def get_optimal_batch_size(cls, current_batch_size: int = None) -> int:
        """Получить оптимальный размер батча на основе текущих условий"""
        if current_batch_size is None:
            current_batch_size = cls.INITIAL_BATCH_SIZE
        
        try:
            # Получаем текущую нагрузку
            memory_percent = cls.get_memory_usage_percent()
            cpu_percent = psutil.cpu_percent(interval=0.1)
            
            # Корректируем размер батча на основе нагрузки
            if memory_percent > 80 or cpu_percent > 80:
                # Высокая нагрузка - уменьшаем
                new_size = max(1000, current_batch_size // 2)
            elif memory_percent > 60 or cpu_percent > 60:
                # Средняя нагрузка - немного уменьшаем
                new_size = max(2000, int(current_batch_size * 0.7))
            elif memory_percent < 40 and cpu_percent < 40 and current_batch_size < 20000:
                # Низкая нагрузка - увеличиваем
                new_size = min(20000, current_batch_size * 2)
            else:
                # Оставляем как есть
                new_size = current_batch_size
            
            # Ограничиваем минимальный и максимальный размер
            new_size = max(100, new_size)  # Минимум 100 записей
            new_size = min(50000, new_size)  # Максимум 50к записей
            
            return new_size
            
        except Exception:
            # При ошибке возвращаем текущий размер
            return current_batch_size
    
    @classmethod
    def get_cache_settings(cls):
        """Получить настройки кэша"""
        return {
            'max_size_mb': cls.MAX_CACHE_SIZE_MB,
            'max_image_size_mb': cls.MAX_IMAGE_SIZE_MB,
            'memory_limit_percent': cls.MAX_MEMORY_PERCENT
        }
    
    @classmethod
    def validate_settings(cls):
        """Проверить и исправить настройки если нужно"""
        warnings = []
        
        # Проверка памяти
        try:
            memory_percent = cls.get_memory_usage_percent()
            if memory_percent > 90:
                warnings.append(f"⚠️ Высокое использование памяти: {memory_percent:.1f}%")
                cls.MAX_WORKERS = max(4, cls.MAX_WORKERS // 2)
        except:
            warnings.append("⚠️ Не удалось проверить использование памяти")
        
        # Проверка диска
        try:
            disk_info = cls.get_disk_space_info()
            if disk_info['percent'] > 90:
                warnings.append(f"⚠️ Мало свободного места на диске: {disk_info['percent']:.1f}% занято")
        except:
            warnings.append("⚠️ Не удалось проверить свободное место на диске")
        
        return warnings
    
    @classmethod
    def print_config_summary(cls):
        """Вывести сводку конфигурации"""
        system_info = cls.get_system_info()
        warnings = cls.validate_settings()
        
        print("\n" + "="*80)
        print("⚙️  КОНФИГУРАЦИЯ СИСТЕМЫ")
        print("="*80)
        
        print(f"📊 Система: {system_info['os']} {system_info['os_version']}")
        print(f"🐍 Python: {system_info['python_version']}")
        print(f"💾 Память: {system_info['memory_total_gb']:.1f} GB всего")
        print(f"   Используется: {system_info['memory_percent']:.1f}%")
        print(f"   Доступно: {system_info['memory_available_gb']:.1f} GB")
        print(f"💿 Диск: {system_info['disk_total_gb']:.1f} GB всего")
        print(f"   Свободно: {system_info['disk_free_gb']:.1f} GB")
        print(f"   Используется: {system_info['disk_percent']:.1f}%")
        print("─" * 80)
        
        print(f"⚡ Производительность:")
        print(f"   Макс. рабочих процессов: {cls.MAX_WORKERS}")
        print(f"   Начальный размер батча: {cls.INITIAL_BATCH_SIZE:,}")
        print(f"   Таймаут запросов: {cls.REQUEST_TIMEOUT} сек")
        print(f"   Попыток загрузки: {cls.REQUEST_RETRIES}")
        print("─" * 80)
        
        print(f"🔒 Безопасность:")
        print(f"   Макс. использование памяти: {cls.MAX_MEMORY_PERCENT}%")
        print(f"   Интервал чекпоинтов: {cls.CHECKPOINT_INTERVAL:,} записей")
        print(f"   Макс. размер кэша: {cls.MAX_CACHE_SIZE_MB} MB")
        print(f"   Макс. размер изображения: {cls.MAX_IMAGE_SIZE_MB} MB")
        
        if warnings:
            print("\n" + "="*80)
            print("⚠️  ПРЕДУПРЕЖДЕНИЯ:")
            for warning in warnings:
                print(f"   • {warning}")
        
        print("="*80)

# Инициализация конфигурации при импорте
Config.init_config()