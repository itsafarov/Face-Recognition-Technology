"""
Основной процессор обработки данных с контролем памяти и чекпоинтами
"""

import os
import sys
import gc
import hashlib
import datetime
import asyncio
import time
import threading
import psutil
import tracemalloc
import traceback
import json
import signal
import platform
from typing import List, Tuple, Set, Dict, Any, Optional, Deque
from concurrent.futures import ThreadPoolExecutor
from collections import deque

# Используем относительные импорты
from .config import Config
from .models import ProcessingMetrics, FaceRecord
from .data_parser import parse_batch_records, get_global_parser
from .checkpoint_manager import CheckpointManager
from .statistics import StatisticsAnalyzer
try:
    # Попытка импорта для запуска из папки src
    from processing.image_processor import ImageProcessorWithEmbedding, process_images_batch
except ImportError:
    # Попытка импорта для запуска из корня
    from src.processing.image_processor import ImageProcessorWithEmbedding, process_images_batch
from src.utils.logger import setup_logging
from src.utils.memory_monitor import MemoryMonitor
from src.utils.windows_paths import get_windows_safe_path, enable_windows_long_paths

logger = setup_logging()


class OptimizedProgressTracker:
    """Трекер прогресса с улучшенным отображением и мониторингом"""
    
    def __init__(self, total_records: int):
        self.total_records = total_records
        self.processed = 0
        self.start_time = time.time()
        self.last_update_time = time.time()
        self.last_update_count = 0
        self.speeds = deque(maxlen=20)  # Фиксированный размер для экономии памяти
        self.batch_times = deque(maxlen=10)
        self.eta_history = deque(maxlen=3)
        
        # Мониторинг памяти
        self.memory_samples = deque(maxlen=100)
        self.max_memory_usage = 0
        
        # Производительность
        self.records_per_second = 0
        self.avg_batch_size = 0
        
    def update(self, processed: int, batch_size: int = 0, memory_usage_mb: float = 0):
        """Обновить прогресс с мониторингом памяти"""
        self.processed = processed
        
        current_time = time.time()
        time_since_last = current_time - self.last_update_time
        records_since_last = processed - self.last_update_count
        
        # Обновляем статистику скорости
        if time_since_last >= 0.5 and records_since_last > 0:  # Раз в 0.5 секунды
            speed = records_since_last / time_since_last
            self.speeds.append(speed)
            
            # Рассчитываем среднюю скорость
            if self.speeds:
                self.records_per_second = sum(self.speeds) / len(self.speeds)
            
            self.last_update_time = current_time
            self.last_update_count = processed
        
        # Обновляем статистику батчей
        if batch_size > 0:
            self.batch_times.append((batch_size, time_since_last))
            
            # Рассчитываем средний размер батча
            if self.batch_times:
                self.avg_batch_size = sum(b[0] for b in self.batch_times) / len(self.batch_times)
        
        # Мониторинг памяти
        if memory_usage_mb > 0:
            self.memory_samples.append(memory_usage_mb)
            self.max_memory_usage = max(self.max_memory_usage, memory_usage_mb)
    
    def get_progress_string(self, metrics: ProcessingMetrics) -> str:
        """Получить строку прогресса с подробной информацией"""
        if self.total_records == 0:
            return "Ожидание..."
        
        progress_percent = (self.processed / self.total_records) * 100
        
        # Время
        elapsed = time.time() - self.start_time
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)
        
        # Рассчитываем ETA
        eta_seconds = 0
        if self.records_per_second > 0:
            remaining = self.total_records - self.processed
            eta_seconds = remaining / self.records_per_second
            
            # Сглаживание ETA
            self.eta_history.append(eta_seconds)
            if self.eta_history:
                avg_eta = sum(self.eta_history) / len(self.eta_history)
                eta_seconds = avg_eta
            
            eta_hours = int(eta_seconds // 3600)
            eta_minutes = int((eta_seconds % 3600) // 60)
            eta_seconds = int(eta_seconds % 60)
            eta_str = f"{eta_hours:02d}:{eta_minutes:02d}:{eta_seconds:02d}"
        else:
            eta_str = "??:??:??"
        
        # Использование памяти
        try:
            memory_usage = psutil.virtual_memory().percent
            # Статус памяти
            if memory_usage < 60:
                memory_status = "🟢"
            elif memory_usage < 80:
                memory_status = "🟡"
            else:
                memory_status = "🔴"
        except:
            memory_usage = 0
            memory_status = "⚪"
        
        # Форматированная строка
        lines = [
            f"📊 {progress_percent:6.2f}% | 📈 {self.processed:,}/{self.total_records:,}",
            f"⚡ {self.records_per_second:.0f}/сек | ⏱️ {hours:02d}:{minutes:02d}:{seconds:02d}",
            f"⏳ ETA: {eta_str} | 🧠 {memory_status} {memory_usage:5.1f}%",
            f"🖼️ {metrics.valid_images:,}✅ {metrics.failed_images:,}❌ | 💾 {self.max_memory_usage/1024:.1f}GB"
        ]
        
        return " | ".join(lines)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Получить статистику трекера"""
        if self.memory_samples:
            avg_memory = sum(self.memory_samples) / len(self.memory_samples)
        else:
            avg_memory = 0
        
        return {
            'processed': self.processed,
            'total': self.total_records,
            'progress_percent': (self.processed / self.total_records * 100) if self.total_records > 0 else 0,
            'records_per_second': self.records_per_second,
            'avg_batch_size': self.avg_batch_size,
            'elapsed_time': time.time() - self.start_time,
            'max_memory_usage_mb': self.max_memory_usage,
            'avg_memory_usage_mb': avg_memory
        }


class BatchProcessor:
    """Обработчик батчей с оптимизацией памяти"""
    
    def __init__(self, image_processor: ImageProcessorWithEmbedding, metrics: ProcessingMetrics):
        self.image_processor = image_processor
        self.metrics = metrics
        self.batch_results = []
        self.last_memory_check = time.time()
        self.memory_lock = asyncio.Lock()  # Добавляем блокировку для памяти
        
    async def process_batch_optimized(self, batch_data: List[Tuple[str, str]], 
                                    current_position: int) -> List[FaceRecord]:
        """Оптимизированная обработка батча записей"""
        if not batch_data:
            return []
        
        batch_start_time = time.time()
        batch_size = len(batch_data)
        
        # Проверка памяти перед обработкой
        await self._check_memory_and_adjust()
        
        try:
            # Шаг 1: Быстрый парсинг всех записей в батче
            lines = [line for line, _ in batch_data]
            parsed_records = parse_batch_records(lines, self.metrics)
            
            if not parsed_records:
                return []
            
            # Шаг 2: Подготовка данных для обработки изображений
            image_urls = []
            record_indices = []
            
            for i, record_data in enumerate(parsed_records):
                if record_data and record_data.get('image_url'):
                    image_urls.append(record_data['image_url'])
                    record_indices.append(i)
            
            # Шаг 3: Пакетная обработка изображений
            image_results = []
            if image_urls:
                try:
                    image_results = await process_images_batch(
                        self.image_processor, 
                        image_urls, 
                        self.metrics
                    )
                except Exception as e:
                    logger.error(f"Ошибка при обработке изображений батча: {e}")
                    # Создаем результаты с ошибками
                    image_results = [None] * len(image_urls)
            
            # Шаг 4: Создание объектов FaceRecord
            face_records = []
            image_result_idx = 0
            
            for i, record_data in enumerate(parsed_records):
                if not record_data:
                    continue
                
                # Создаем объект записи
                try:
                    record = FaceRecord(**record_data)
                    
                    # Добавляем результаты обработки изображений
                    if i in record_indices and image_result_idx < len(image_results):
                        img_result = image_results[image_result_idx]
                        image_result_idx += 1
                        
                        if img_result and hasattr(img_result, '_fields'):  # NamedTuple проверка
                            filepath, base64_str, img_info = img_result
                            
                            if filepath and base64_str:
                                record.image_path = filepath
                                record.image_base64 = base64_str
                                if img_info:
                                    record.image_width = img_info.get('width', 0)
                                    record.image_height = img_info.get('height', 0)
                                    record.image_size_kb = img_info.get('file_size_kb', 0)
                                    record.download_time_ms = img_info.get('download_time_ms', 0)
                                    record.is_cached = img_info.get('is_cached', False)
                                record.image_hash = hashlib.md5(record.image_url.encode()).hexdigest()
                            elif record.image_url:
                                record.failed_reason = img_info.get('failed_reason', 'Ошибка загрузки') if img_info else 'Ошибка загрузки'
                                record.image_hash = hashlib.md5(record.image_url.encode()).hexdigest()
                    
                    face_records.append(record)
                    
                except Exception as e:
                    logger.warning(f"Ошибка создания FaceRecord: {e}")
                    continue
            
            # Шаг 5: Обновление статистики
            batch_time = time.time() - batch_start_time
            self.metrics.add_batch_time(batch_time)
            
            # Шаг 6: Обновление уникальных IP
            for record in face_records:
                if record.ip_address and record.ip_address != 'Н/Д':
                    self.metrics.unique_ips.add(record.ip_address)
            
            logger.debug(f"Обработан батч из {batch_size} записей за {batch_time:.2f} сек")
            return face_records
            
        except Exception as e:
            logger.error(f"Ошибка при обработке батча: {e}", exc_info=True)
            # Возвращаем записи без изображений в случае ошибки
            return self._create_fallback_records(batch_data)
    
    async def _check_memory_and_adjust(self):
        """Проверить память и при необходимости приостановить обработку"""
        async with self.memory_lock:
            current_time = time.time()
            
            # Проверяем не чаще чем раз в 2 секунды
            if current_time - self.last_memory_check < 2:
                return
            
            self.last_memory_check = current_time
            
            try:
                memory_percent = psutil.virtual_memory().percent
                available_gb = psutil.virtual_memory().available / (1024**3)
                
                # Критическое использование памяти
                if memory_percent > 90 or available_gb < 0.2:
                    logger.warning(f"Критическое использование памяти: {memory_percent:.1f}%, {available_gb:.2f}GB свободно")
                    await asyncio.sleep(5)
                    gc.collect()
                elif memory_percent > 80 or available_gb < 0.5:
                    logger.debug(f"Высокое использование памяти: {memory_percent:.1f}%")
                    await asyncio.sleep(1)
                    gc.collect()
                elif memory_percent > 70:
                    await asyncio.sleep(0.5)
                    
            except Exception as e:
                logger.debug(f"Ошибка проверки памяти: {e}")
    
    def _create_fallback_records(self, batch_data: List[Tuple[str, str]]) -> List[FaceRecord]:
        """Создать записи без изображений в случае ошибки"""
        records = []
        parser = get_global_parser()
        
        for line, line_hash in batch_data:
            try:
                record_data = parser.parse_record(line, self.metrics)
                if record_data:
                    record = FaceRecord(**record_data)
                    records.append(record)
            except Exception as e:
                logger.debug(f"Ошибка создания fallback записи: {e}")
                continue
        
        return records


class OptimizedMemoryManager:
    """Менеджер памяти с улучшенным контролем"""
    
    def __init__(self):
        self.peak_memory = 0
        self.memory_samples = deque(maxlen=1000)  # Ограничиваем размер истории
        self.last_cleanup = time.time()
        self.cleanup_lock = threading.Lock()
        
    def check_memory_safe(self, additional_mb: float = 0) -> bool:
        """Проверить, безопасно ли выделять дополнительную память"""
        try:
            memory = psutil.virtual_memory()
            current_usage = memory.percent
            available_mb = memory.available / (1024**2)
            
            # Обновляем пиковое значение
            self.peak_memory = max(self.peak_memory, memory.used / (1024**3))
            
            # Сохраняем сэмпл
            with self.cleanup_lock:
                self.memory_samples.append({
                    'time': time.time(),
                    'percent': current_usage,
                    'available_mb': available_mb,
                    'used_gb': memory.used / (1024**3)
                })
            
            # Проверяем условия безопасности
            safe_percent = current_usage < Config.MAX_MEMORY_PERCENT
            safe_available = (available_mb - additional_mb) > 200  # Минимум 200MB свободно
            
            # Автоматическая очистка если нужно
            if not safe_percent or not safe_available:
                if time.time() - self.last_cleanup > 30:
                    self.force_cleanup()
            
            return safe_percent and safe_available
            
        except Exception as e:
            logger.debug(f"Ошибка проверки памяти: {e}")
            return True
    
    def force_cleanup(self):
        """Принудительная очистка памяти"""
        with self.cleanup_lock:
            self.last_cleanup = time.time()
            
            try:
                # Очищаем кэш парсера
                parser = get_global_parser()
                if hasattr(parser, 'clear_cache'):
                    parser.clear_cache()
                
                # Принудительный сбор мусора
                for _ in range(2):  # Уменьшаем количество циклов
                    gc.collect()
                
                logger.info("Выполнена принудительная очистка памяти")
                
            except Exception as e:
                logger.debug(f"Ошибка очистки памяти: {e}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Получить статистику использования памяти"""
        if not self.memory_samples:
            return {
                'peak_memory_gb': 0,
                'avg_memory_percent': 0,
                'current_memory_percent': 0,
                'samples_count': 0
            }
        
        # Рассчитываем средние значения
        with self.cleanup_lock:
            samples = list(self.memory_samples)  # Копируем для безопасности
        
        if not samples:
            return {
                'peak_memory_gb': self.peak_memory,
                'avg_memory_percent': 0,
                'current_memory_percent': 0,
                'samples_count': 0
            }
        
        avg_percent = sum(s['percent'] for s in samples) / len(samples)
        current_percent = samples[-1]['percent'] if samples else 0
        
        return {
            'peak_memory_gb': self.peak_memory,
            'avg_memory_percent': avg_percent,
            'current_memory_percent': current_percent,
            'samples_count': len(samples),
            'last_cleanup': self.last_cleanup
        }


class FaceRecognitionProcessor:
    """Главный процессор обработки данных с контролем памяти"""
    
    def __init__(self, formats: List[str], resume: bool = False):
        self.metrics = ProcessingMetrics()
        self.records: List[FaceRecord] = []
        self.image_processor = None
        self.formats = formats
        self.output_dir = ""
        self.report_generator = None
        self.checkpoint_manager = None
        self.resume = resume
        
        # Динамические настройки
        self.batch_size = Config.INITIAL_BATCH_SIZE
        self.max_batch_size = 20000
        self.min_batch_size = 500
        
        # Оптимизированные компоненты
        self.memory_manager = OptimizedMemoryManager()
        self.memory_monitor = MemoryMonitor()
        self.batch_processor = None
        
        # Состояние обработки
        self.processed_hashes: Set[str] = set()
        self.last_checkpoint_save = 0
        self.processed_since_checkpoint = 0
        self.progress_tracker = None
        self.is_running = True
        
        # Статистика
        self.total_batches_processed = 0
        self.avg_batch_processing_time = 0
        
        # Мониторинг производительности
        self.performance_stats = {
            'file_read_speed': 0,
            'parsing_speed': 0,
            'image_processing_speed': 0,
            'total_records_processed': 0
        }
        
        # Обработка сигналов для Windows
        self._setup_signal_handlers()
        
        logger.info(f"Инициализирован FaceRecognitionProcessor с batch_size={self.batch_size}")
    
    def _setup_signal_handlers(self):
        """Настройка обработчиков сигналов для корректного завершения"""
        if platform.system() == "Windows":
            try:
                # Для Windows используем альтернативный метод
                import win32api
                
                def windows_signal_handler(signal_type):
                    if signal_type in [2, 15]:  # CTRL_C_EVENT, CTRL_BREAK_EVENT
                        print("\n⚠️  Получен сигнал прерывания, завершаем обработку...")
                        self.is_running = False
                        return True  # Обработано
                    return False
                
                # Устанавливаем обработчик
                win32api.SetConsoleCtrlHandler(windows_signal_handler, True)
            except ImportError:
                logger.warning("Модуль win32api не установлен, обработка сигналов может работать некорректно")
        else:
            # Для Unix-систем
            import signal
            
            def unix_signal_handler(signum, frame):
                print(f"\n⚠️  Получен сигнал {signum}, завершаем обработку...")
                self.is_running = False
            
            signal.signal(signal.SIGINT, unix_signal_handler)
            signal.signal(signal.SIGTERM, unix_signal_handler)
    
    async def process_file(self, input_file: str) -> bool:
        """Обработка файла с поддержкой возобновления"""
        logger.info(f"🎯 Начало обработки файла: {os.path.basename(input_file)}")
        
        # Включаем поддержку длинных путей для Windows
        if platform.system() == "Windows":
            enable_windows_long_paths()
        
        # Запуск мониторинга памяти
        self.memory_monitor.start()
        
        # Начало трассировки памяти
        tracemalloc.start()
        
        try:
            # Подсчет строк
            print("🔍 Подсчет записей в файле...")
            total_lines = await self._count_lines_optimized(input_file)
            if total_lines == 0:
                logger.error("Файл пуст")
                return False
            
            print(f"✅ Найдено записей: {total_lines:,}")
            print(f"📁 Форматы отчетов: {', '.join(self.formats)}")
            
            system_info = Config.get_system_info()
            print(f"💾 Доступно памяти: {system_info['memory_available_gb']:.1f} GB")
            print(f"💿 Свободно на диске: {system_info['disk_free_gb']:.1f} GB")
            
            # Создание папки для результатов с безопасными путями
            self.output_dir = Config.setup_directories()
            print(f"📂 Результаты будут сохранены в: {self.output_dir}")
            
            # Инициализация менеджера чекпоинтов
            self.checkpoint_manager = CheckpointManager(self.output_dir)
            
            # Загрузка чекпоинта если нужно
            start_position, checkpoint_data = await self._load_checkpoint_state(input_file, total_lines)
            
            # Инициализация трекера прогресса
            self.progress_tracker = OptimizedProgressTracker(total_lines)
            
            # Инициализация процессора изображений
            print("🚀 Инициализация обработчика изображений...")
            self.image_processor = ImageProcessorWithEmbedding(self.output_dir)
            
            # Инициализация батч-процессора
            self.batch_processor = BatchProcessor(self.image_processor, self.metrics)
            
            print("\n" + "="*80)
            print("🚀 НАЧАЛО ОБРАБОТКИ")
            print("="*80)
            
            # Запуск обработки с таймаутом
            processing_task = asyncio.create_task(
                self._process_file_optimized(input_file, total_lines, start_position)
            )
            
            # Запуск отображения прогресса
            progress_task = asyncio.create_task(self._display_optimized_progress())
            
            # Мониторинг производительности
            monitor_task = asyncio.create_task(self._monitor_performance())
            
            # Ожидание завершения обработки
            try:
                # Используем wait с таймаутом для обработки прерываний
                done, pending = await asyncio.wait(
                    [processing_task],
                    timeout=3600 * 24,  # 24 часа максимум
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                if processing_task in done:
                    success = processing_task.result()
                else:
                    # Таймаут или прерывание
                    processing_task.cancel()
                    try:
                        await processing_task
                    except asyncio.CancelledError:
                        pass
                    success = False
                    print("\n⚠️  Обработка прервана по таймауту")
                
            except KeyboardInterrupt:
                print("\n\n⚠️  Обработка прервана пользователем")
                self.is_running = False
                success = False
            finally:
                # Остановка задач
                progress_task.cancel()
                monitor_task.cancel()
                try:
                    await asyncio.wait_for(progress_task, timeout=2.0)
                    await asyncio.wait_for(monitor_task, timeout=2.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            
            if success:
                # Финальное сохранение состояния
                current_position = await self._get_file_position_async(input_file, start_position, self.metrics.total_records)
                self._save_checkpoint(input_file, total_lines, current_position)
                
                # Генерация отчетов
                await self._generate_reports()
                
                return True
            else:
                # Сохраняем чекпоинт для возможности возобновления
                if self.metrics.total_records > 0:
                    current_position = await self._get_file_position_async(input_file, start_position, self.metrics.total_records)
                    self._save_checkpoint(input_file, total_lines, current_position)
                    print("💾 Прогресс сохранен для возможности возобновления")
                return False
            
        except KeyboardInterrupt:
            print("\n\n⚠️  Обработка прервана пользователем")
            return False
        except Exception as e:
            logger.error(f"Ошибка обработки: {e}", exc_info=True)
            traceback.print_exc()
            return False
        finally:
            self.is_running = False
            self.memory_monitor.stop()
            
            # Остановка трассировки памяти
            if tracemalloc.is_tracing():
                try:
                    snapshot = tracemalloc.take_snapshot()
                    top_stats = snapshot.statistics('lineno')[:10]
                    
                    logger.info("Топ-10 строк по использованию памяти:")
                    for stat in top_stats:
                        logger.info(f"{stat}")
                except Exception as e:
                    logger.error(f"Ошибка при трассировке памяти: {e}")
                
                tracemalloc.stop()
            
            # Финальная очистка памяти
            await self._final_cleanup()
    
    async def _load_checkpoint_state(self, input_file: str, total_lines: int) -> Tuple[int, Optional[Dict]]:
        """Загрузить состояние чекпоинта"""
        start_position = 0
        
        if self.resume:
            checkpoint = self.checkpoint_manager.load_checkpoint()
            if checkpoint and self.checkpoint_manager.validate_checkpoint(input_file)[0]:
                start_position = checkpoint.last_position
                total_lines = checkpoint.total_lines
                self.metrics.total_records = checkpoint.processed_lines
                self.metrics.valid_images = checkpoint.valid_images
                self.metrics.failed_images = checkpoint.failed_images
                self.metrics.json_errors = checkpoint.json_errors
                self.metrics.cached_images = checkpoint.cached_images
                self.metrics.network_errors = checkpoint.network_errors
                self.metrics.timeout_errors = checkpoint.timeout_errors
                self.metrics.duplicate_records = checkpoint.duplicate_records
                self.batch_size = checkpoint.batch_size
                
                # Восстанавливаем хэши и уникальные данные
                self.processed_hashes = set(checkpoint.records_processed)
                self.metrics.unique_users = set(checkpoint.unique_users)
                self.metrics.unique_devices = set(checkpoint.unique_devices)
                self.metrics.unique_companies = set(checkpoint.unique_companies)
                self.metrics.unique_ips = set(checkpoint.unique_ips)
                
                print(f"🔄 Продолжаем с позиции: {start_position:,} байт")
                print(f"🔄 Уже обработано: {checkpoint.processed_lines:,} записей")
                print(f"🔄 Размер батча: {self.batch_size:,}")
                
                return start_position, checkpoint
            else:
                if checkpoint:
                    print("⚠️  Чекпоинт невалиден, начинаем с начала")
                else:
                    print("🔄 Чекпоинт не найден, начинаем с начала")
                self.checkpoint_manager.clear_checkpoint()
        
        return start_position, None
    
    async def _process_file_optimized(self, input_file: str, total_lines: int, start_position: int) -> bool:
        """Оптимизированная обработка файла"""
        try:
            async with self.image_processor:
                # Адаптивный размер буфера для разных ОС
                if platform.system() == "Windows":
                    buffer_size = 1024 * 1024 * 2  # 2MB для Windows
                else:
                    buffer_size = 1024 * 1024 * 10  # 10MB для других ОС
                
                with open(input_file, 'r', encoding='utf-8', buffering=buffer_size, errors='ignore') as f:
                    # Перемещаемся к позиции возобновления
                    if start_position > 0:
                        f.seek(start_position)
                    
                    batch_data = []
                    batch_count = 0
                    lines_processed = 0
                    current_byte_position = start_position
                    
                    batch_start_time = time.time()
                    
                    # Чтение файла по строкам
                    for line in f:
                        if not self.is_running:
                            break
                        
                        line = line.strip()
                        if not line:
                            continue
                        
                        # Обновляем позицию в файле
                        current_byte_position += len(line.encode('utf-8', errors='replace')) + 1  # +1 для символа новой строки
                        
                        # Генерируем хэш строки
                        line_hash = hashlib.md5(line.encode('utf-8', errors='replace')).hexdigest()[:16]
                        
                        # Проверка на дубликат
                        if line_hash in self.processed_hashes:
                            self.metrics.total_records += 1
                            self.metrics.duplicate_records += 1
                            continue
                        
                        batch_data.append((line, line_hash))
                        lines_processed += 1
                        self.processed_hashes.add(line_hash)
                        
                        # Обрабатываем батч когда накопится достаточно данных
                        if len(batch_data) >= self.batch_size:
                            await self._process_and_update_batch(batch_data, current_byte_position, batch_count, input_file, total_lines)
                            
                            batch_data = []
                            batch_count += 1
                            
                            # Динамическая настройка размера батча
                            self._adjust_batch_size_dynamically(batch_count)
                            
                            # Измерение времени батча
                            batch_time = time.time() - batch_start_time
                            self.avg_batch_processing_time = (
                                self.avg_batch_processing_time * 0.9 + batch_time * 0.1
                            )
                            batch_start_time = time.time()
                    
                    # Обработка остатка
                    if batch_data:
                        await self._process_and_update_batch(batch_data, current_byte_position, batch_count, input_file, total_lines)
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка в процессе обработки: {e}")
            traceback.print_exc()
            return False
    
    async def _process_and_update_batch(self, batch_data: List[Tuple[str, str]], 
                                      current_position: int, batch_count: int,
                                      input_file: str, total_lines: int):
        """Обработать батч и обновить состояние"""
        # Обработка батча
        batch_records = await self.batch_processor.process_batch_optimized(
            batch_data, current_position
        )
        
        # Добавление записей в общий список
        self.records.extend(batch_records)
        
        # Обновление счетчиков
        processed_in_batch = len(batch_data)
        self.metrics.total_records += processed_in_batch
        self.metrics.processed_records += len(batch_records)
        self.total_batches_processed += 1
        
        # Обновление прогресса (только каждые 1000 записей для уменьшения оверхеда)
        if self.metrics.total_records % 1000 == 0:
            memory_usage_mb = 0
            try:
                memory_usage_mb = psutil.virtual_memory().used / (1024**2)
            except:
                pass
            
            self.progress_tracker.update(
                self.metrics.total_records, 
                processed_in_batch,
                memory_usage_mb
            )
        
        # Сохранение чекпоинта
        self.processed_since_checkpoint += processed_in_batch
        if self.processed_since_checkpoint >= Config.CHECKPOINT_INTERVAL:
            self._save_checkpoint(
                os.path.basename(input_file),
                total_lines,
                current_position
            )
            self.processed_since_checkpoint = 0
            
            # Промежуточное сохранение записей
            await self._save_records_intermediate()
        
        # Оптимизация памяти каждые 10 батчей
        if batch_count % 10 == 0:
            await self._optimize_memory_usage()
    
    async def _optimize_memory_usage(self):
        """Оптимизация использования памяти"""
        try:
            # Очистка кэша парсера если он слишком большой
            parser = get_global_parser()
            if parser and hasattr(parser, '_cache'):
                cache_size = len(parser._cache) if hasattr(parser._cache, '__len__') else 0
                if cache_size > 15000:
                    parser.clear_cache()
                    logger.debug(f"Очищен кэш парсера (было {cache_size} записей)")
            
            # Принудительный сбор мусора
            collected = gc.collect()
            logger.debug(f"Собрано мусора: {collected} объектов")
            
            # Проверка памяти и приостановка если нужно
            try:
                memory_percent = psutil.virtual_memory().percent
                if memory_percent > 85:
                    logger.warning(f"Высокое использование памяти ({memory_percent}%), пауза 2 секунды")
                    await asyncio.sleep(2)
            except:
                pass
                
        except Exception as e:
            logger.debug(f"Ошибка оптимизации памяти: {e}")
    
    async def _save_records_intermediate(self):
        """Промежуточное сохранение записей для экономии памяти"""
        if len(self.records) < 10000:
            return
        
        try:
            # Сохраняем часть записей во временный файл
            save_count = len(self.records) // 2
            records_to_save = self.records[:save_count]
            
            # Создаем безопасный путь для Windows
            temp_dir = get_windows_safe_path(self.output_dir, Config.TEMP_FOLDER)
            os.makedirs(temp_dir, exist_ok=True)
            
            temp_file = get_windows_safe_path(
                temp_dir, 
                f"records_temp_{int(time.time())}.jsonl"
            )
            
            # Используем быструю сериализацию
            with open(temp_file, 'w', encoding='utf-8', errors='ignore') as f:
                for record in records_to_save:
                    try:
                        record_dict = record.to_dict()
                        f.write(json.dumps(record_dict, ensure_ascii=False) + '\n')
                    except Exception as e:
                        logger.debug(f"Ошибка сериализации записи: {e}")
                        continue
            
            # Удаляем сохраненные записи из памяти
            del self.records[:save_count]
            
            logger.debug(f"Сохранено {save_count} записей во временный файл")
            
            # Очистка памяти
            gc.collect()
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении записей: {e}")
    
    async def _load_saved_records(self):
        """Загрузить сохраненные записи из временных файлов"""
        temp_dir = get_windows_safe_path(self.output_dir, Config.TEMP_FOLDER)
        if not os.path.exists(temp_dir):
            return
        
        try:
            temp_files = []
            for filename in os.listdir(temp_dir):
                if filename.startswith('records_temp_') and filename.endswith('.jsonl'):
                    filepath = os.path.join(temp_dir, filename)
                    temp_files.append((os.path.getmtime(filepath), filepath))
            
            # Сортируем по времени создания
            temp_files.sort()
            
            loaded_count = 0
            for _, filepath in temp_files:
                try:
                    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                        for line in f:
                            if line.strip():
                                try:
                                    data = json.loads(line.strip())
                                    record = FaceRecord(**data)
                                    self.records.append(record)
                                    loaded_count += 1
                                except json.JSONDecodeError as e:
                                    logger.debug(f"Ошибка парсинга JSON: {e}")
                                    continue
                    
                    # Удаляем временный файл
                    os.remove(filepath)
                    
                except Exception as e:
                    logger.error(f"Ошибка загрузки файла {filepath}: {e}")
            
            if loaded_count > 0:
                logger.info(f"Загружено {loaded_count} записей из временных файлов")
                
        except Exception as e:
            logger.error(f"Ошибка загрузки сохраненных записей: {e}")
    
    def _adjust_batch_size_dynamically(self, batch_count: int):
        """Динамическая настройка размера батча на основе производительности"""
        try:
            # Получаем текущие метрики
            memory_percent = psutil.virtual_memory().percent
            available_gb = psutil.virtual_memory().available / (1024**3)
            
            # Рассчитываем целевую скорость обработки
            target_records_per_second = 1000  # Целевая скорость
            
            # Адаптируем размер батча
            if memory_percent > 85 or available_gb < 0.5:
                # Критическая ситуация - резко уменьшаем
                new_size = max(self.min_batch_size, self.batch_size // 2)
                if new_size != self.batch_size:
                    logger.warning(f"Критическая память: уменьшаем batch_size до {new_size}")
            elif memory_percent > 75:
                # Высокая нагрузка - немного уменьшаем
                new_size = max(self.min_batch_size, int(self.batch_size * 0.7))
                if new_size != self.batch_size and batch_count % 5 == 0:
                    logger.info(f"Высокая нагрузка памяти: уменьшаем batch_size до {new_size}")
            elif self.avg_batch_processing_time > 10:
                # Медленная обработка - уменьшаем
                new_size = max(self.min_batch_size, int(self.batch_size * 0.8))
                if new_size != self.batch_size:
                    logger.info(f"Медленная обработка: уменьшаем batch_size до {new_size}")
            elif (memory_percent < 60 and available_gb > 2 and 
                  self.avg_batch_processing_time < 5 and 
                  self.batch_size < self.max_batch_size):
                # Хорошие условия - увеличиваем
                new_size = min(self.max_batch_size, int(self.batch_size * 1.5))
                if new_size != self.batch_size and batch_count % 10 == 0:
                    logger.info(f"Хорошие условия: увеличиваем batch_size до {new_size}")
            else:
                new_size = self.batch_size
            
            # Применяем новый размер
            self.batch_size = new_size
            
        except Exception as e:
            logger.debug(f"Ошибка настройки размера батча: {e}")
    
    async def _display_optimized_progress(self):
        """Оптимизированное отображение прогресса"""
        last_update = 0
        
        while self.is_running:
            try:
                current_time = time.time()
                
                # Обновляем каждые 2 секунды для уменьшения оверхеда
                if current_time - last_update >= 2.0 and self.progress_tracker:
                    # Выводим прогресс (обновление трекера происходит в основном потоке)
                    
                    # Выводим прогресс
                    progress_str = self.progress_tracker.get_progress_string(self.metrics)
                    sys.stdout.write('\r' + progress_str + ' ' * 10)
                    sys.stdout.flush()
                    
                    last_update = current_time
                
                await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Ошибка в отображении прогресса: {e}")
                await asyncio.sleep(1)
        
        # Финальный прогресс
        if self.progress_tracker:
            try:
                progress_str = self.progress_tracker.get_progress_string(self.metrics)
                sys.stdout.write('\r' + progress_str + ' ' * 10 + '\n')
                sys.stdout.flush()
            except:
                pass
    
    async def _monitor_performance(self):
        """Мониторинг производительности"""
        last_check = time.time()
        
        while self.is_running:
            try:
                current_time = time.time()
                
                # Проверяем каждые 5 секунд
                if current_time - last_check >= 5:
                    # Получаем статистику парсера
                    parser = get_global_parser()
                    parser_stats = parser.get_statistics() if hasattr(parser, 'get_statistics') else {}
                    
                    # Обновляем статистику производительности
                    if self.progress_tracker:
                        self.performance_stats['records_per_second'] = (
                            self.progress_tracker.records_per_second
                        )
                    
                    self.performance_stats['total_records_processed'] = self.metrics.total_records
                    self.performance_stats['parser_cache_hit_rate'] = parser_stats.get('cache_hit_rate', 'N/A')
                    
                    last_check = current_time
                
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Ошибка мониторинга производительности: {e}")
                await asyncio.sleep(5)
    
    def _save_checkpoint(self, input_file: str, total_lines: int, position: int):
        """Сохранить чекпоинт"""
        if self.checkpoint_manager:
            self.checkpoint_manager.save_checkpoint(
                file_name=os.path.basename(input_file),
                total_lines=total_lines,
                processed_lines=self.metrics.total_records,
                valid_images=self.metrics.valid_images,
                failed_images=self.metrics.failed_images,
                json_errors=self.metrics.json_errors,
                cached_images=self.metrics.cached_images,
                network_errors=self.metrics.network_errors,
                timeout_errors=self.metrics.timeout_errors,
                duplicate_records=self.metrics.duplicate_records,
                last_position=position,
                batch_size=self.batch_size,
                records_processed=list(self.processed_hashes),
                unique_users=list(self.metrics.unique_users),
                unique_devices=list(self.metrics.unique_devices),
                unique_companies=list(self.metrics.unique_companies),
                unique_ips=list(self.metrics.unique_ips)
            )
    
    async def _generate_reports(self):
        """Генерация выбранных отчетов"""
        logger.info("📄 Генерация отчетов...")
        
        # Загружаем сохраненные записи если есть
        await self._load_saved_records()
        
        print("\n" + "="*80)
        print("📊 ИТОГОВАЯ СТАТИСТИКА ОБРАБОТКИ")
        print("="*80)
        
        # Основная статистика
        stats = StatisticsAnalyzer.analyze(self.records)
        
        print(f"✅ Всего записей обработано: {self.metrics.total_records:,}")
        print(f"✅ Успешных фото: {self.metrics.valid_images:,}")
        print(f"⚠️  Неудачных загрузок: {self.metrics.failed_images:,}")
        print(f"❌ Ошибок JSON: {self.metrics.json_errors:,}")
        print(f"💾 Кэшированных фото: {self.metrics.cached_images:,}")
        print(f"⏱️  Время обработки: {self.metrics.elapsed_time:.1f} сек")
        if self.metrics.elapsed_time > 0:
            print(f"⚡ Средняя скорость: {self.metrics.total_records / self.metrics.elapsed_time:.0f} записей/сек")
        print("─" * 80)
        
        # Расширенная статистика
        print(f"🏢 Уникальных компаний: {len(self.metrics.unique_companies)}")
        print(f"👤 Уникальных пользователей: {len(self.metrics.unique_users)}")
        print(f"📱 Уникальных устройств: {len(self.metrics.unique_devices)}")
        print(f"🌐 Уникальных IP: {len(self.metrics.unique_ips)}")
        print(f"📸 Записей с фото: {stats['with_images']:,}")
        print(f"🎯 Распознаваний (тип 1): {stats['by_event_type'].get('1', 0):,}")
        print(f"📅 Событий (тип 2): {stats['by_event_type'].get('2', 0):,}")
        print("─" * 80)
        
        # Использование памяти
        memory_stats = self.memory_monitor.get_statistics()
        print(f"🧠 Пиковое использование памяти: {memory_stats['peak_memory_mb']:.1f} MB")
        print(f"💾 Среднее использование памяти: {memory_stats['avg_memory_mb']:.1f} MB")
        
        # Статистика парсера
        parser = get_global_parser()
        if hasattr(parser, 'get_statistics'):
            parser_stats = parser.get_statistics()
            print(f"📊 Кэш парсера: {parser_stats.get('cache_hit_rate', 'N/A')}")
        
        print("="*80)
        
        # Генерация отчетов
        reports_created = []
        
        if "HTML" in self.formats:
            print("🔄 Создание HTML отчета...")
            # Используем отложенный импорт для избежания циклической зависимости
            from src.processing.report_generator import ReportGenerator
            report_generator = ReportGenerator(self.output_dir)
            html_report = report_generator.generate_html_report(self.records, self.metrics)
            reports_created.append(("🌐 HTML отчет", html_report))
            print("✅ HTML отчет создан")
        
        # Создание README файла
        self._create_readme(reports_created)
        
        # Вывод результатов
        print("\n🎉 ОТЧЕТЫ УСПЕШНО СОЗДАНЫ")
        print("="*80)
        print(f"📁 Папка с результатами: {self.output_dir}")
        
        for report_name, report_path in reports_created:
            if report_path:
                print(f"   • {report_name}: {os.path.basename(report_path)}")
        
        print(f"🖼️  Фотографии сохранены в: {os.path.join(self.output_dir, Config.IMAGE_FOLDER)}")
        print("="*80)
    
    def _create_readme(self, reports_created: List[Tuple[str, str]]):
        """Создание README файла"""
        memory_stats = self.memory_monitor.get_statistics()
        
        readme_content = f"""
# ОТЧЕТ ПО РАСПОЗНАВАНИЮ ЛИЦ

## 📊 Статистика обработки
- Дата создания: {datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")}
- Всего записей: {self.metrics.total_records:,}
- Успешных фото: {self.metrics.valid_images:,}
- Ошибок загрузки: {self.metrics.failed_images:,}
- Время обработки: {self.metrics.elapsed_time:.1f} сек
- Средняя скорость: {self.metrics.total_records / self.metrics.elapsed_time:.0f} записей/сек
- Пиковое использование памяти: {memory_stats['peak_memory_mb']:.1f} MB

## 🚀 Особенности этой версии:
✅ Поддержка файлов более 2 ГБ
✅ Возобновление обработки после прерывания
✅ Контроль использования памяти (до 85% ОЗУ)
✅ Автоматическое сохранение прогресса каждые 100,000 записей
✅ Динамическая настройка размера батча
✅ Отказоустойчивая загрузка изображений
✅ Мониторинг сети и памяти в реальном времени
✅ Оптимизированный парсинг JSON с кэшированием

## 📄 Созданные отчеты:
"""
        
        for report_name, report_path in reports_created:
            if report_path:
                readme_content += f"- {report_name}: {os.path.basename(report_path)}\n"
        
        readme_content += f"""
## 📁 Структура папок
{self.output_dir}/
├── {Config.IMAGE_FOLDER}/     # Оригинальные изображения
├── {Config.CACHE_FOLDER}/     # Кэш изображений
├── {Config.TEMP_FOLDER}/      # Временные файлы
└── {Config.REPORTS_FOLDER}/   # Все созданные отчеты

## 🔧 Как использовать
1. Откройте HTML отчет в браузере
2. Все фотографии уже встроены в таблицу
3. Используйте фильтры для поиска по компании/типу
4. Нажимайте на фото для увеличения
5. Экспортируйте данные в нужном формате

## ⚠️ Восстановление обработки
Если обработка прервалась, запустите программу с ключом --resume
Программа автоматически продолжит с последнего чекпоинта.

## 💡 Советы
- Для файлов >1 ГБ рекомендуется использовать только HTML отчет
- SSD ускоряет работу с кэшем изображений
- При проблемах с памятью программа автоматически уменьшит batch_size
- Используйте промежуточное сохранение для очень больших файлов
"""
        
        readme_path = os.path.join(self.output_dir, "README.txt")
        try:
            with open(readme_path, 'w', encoding='utf-8', errors='ignore') as f:
                f.write(readme_content)
        except Exception as e:
            logger.error(f"Ошибка создания README файла: {e}")
    
    async def _count_lines_optimized(self, file_path: str) -> int:
        """Оптимизированный подсчет строк в файле"""
        loop = asyncio.get_event_loop()
        
        def count_lines_sync():
            count = 0
            # Адаптивный размер буфера
            if platform.system() == "Windows":
                buffer_size = 1024 * 1024 * 4  # 4MB для Windows
            else:
                buffer_size = 1024 * 1024 * 8  # 8MB для других ОС
            
            with open(file_path, 'r', encoding='utf-8', errors='ignore', buffering=buffer_size) as f:
                while True:
                    buffer = f.read(buffer_size)
                    if not buffer:
                        break
                    count += buffer.count('\n')
            
            return count
        
        with ThreadPoolExecutor(max_workers=1) as executor:
            result = await loop.run_in_executor(executor, count_lines_sync)
        
        return result
    
    async def _get_file_position_async(self, file_path: str, start_position: int, processed_lines: int) -> int:
        """Асинхронное получение позиции в файле"""
        try:
            if processed_lines == 0:
                return start_position
            
            # Получаем размер файла
            file_size = os.path.getsize(file_path)
            
            # Если мы в начале файла, используем start_position
            if start_position == 0:
                # Оцениваем средний размер строки
                if processed_lines > 1000:
                    # Читаем первые 1000 строк для оценки
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        total_bytes = 0
                        lines_read = 0
                        for _ in range(min(1000, processed_lines)):
                            line = f.readline()
                            if not line:
                                break
                            total_bytes += len(line.encode('utf-8', errors='replace'))
                            lines_read += 1
                        
                        if lines_read > 0:
                            avg_line_size = total_bytes / lines_read
                            estimated_position = int(start_position + (processed_lines * avg_line_size))
                            return min(estimated_position, file_size)
            
            return start_position
            
        except:
            return start_position
    
    async def _final_cleanup(self):
        """Финальная очистка ресурсов"""
        try:
            # Очистка кэша парсера
            parser = get_global_parser()
            if hasattr(parser, 'clear_cache'):
                parser.clear_cache()
            
            # Очистка списков
            self.records.clear()
            self.processed_hashes.clear()
            
            # Принудительный сбор мусора
            for _ in range(2):  # Уменьшаем количество циклов
                gc.collect()
            
            logger.info("Выполнена финальная очистка ресурсов")
            
        except Exception as e:
            logger.error(f"Ошибка при финальной очистке: {e}")
    
    def get_performance_report(self) -> Dict[str, Any]:
        """Получить отчет о производительности"""
        memory_stats = self.memory_monitor.get_statistics()
        
        report = {
            'processing': {
                'total_records': self.metrics.total_records,
                'processing_time_seconds': self.metrics.elapsed_time,
                'records_per_second': self.metrics.total_records / self.metrics.elapsed_time if self.metrics.elapsed_time > 0 else 0,
                'success_rate': self.metrics.success_rate,
                'batches_processed': self.total_batches_processed,
                'avg_batch_processing_time': self.avg_batch_processing_time,
                'final_batch_size': self.batch_size
            },
            'memory': memory_stats,
            'images': {
                'valid': self.metrics.valid_images,
                'failed': self.metrics.failed_images,
                'cached': self.metrics.cached_images,
                'success_rate': self.metrics.success_rate
            },
            'uniques': {
                'users': len(self.metrics.unique_users),
                'devices': len(self.metrics.unique_devices),
                'companies': len(self.metrics.unique_companies),
                'ips': len(self.metrics.unique_ips)
            }
        }
        
        # Добавляем статистику парсера
        parser = get_global_parser()
        if hasattr(parser, 'get_statistics'):
            report['parser'] = parser.get_statistics()
        
        # Добавляем статистику трекера
        if self.progress_tracker:
            report['progress_tracker'] = self.progress_tracker.get_statistics()
        
        return report