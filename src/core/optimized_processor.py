"""
Оптимизированный процессор обработки данных с минимальным оверхедом
"""
import os
import sys
import gc
import hashlib
import datetime
import asyncio
import time
import traceback
import json
from typing import List, Tuple, Set, Dict, Any, Optional, Deque
from collections import deque
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor

import psutil
import tracemalloc

# Локальные импорты - исправлены на относительные
from .config import Config
from .models import ProcessingMetrics, FaceRecord
from .data_parser import parse_batch_records, get_global_parser
from .checkpoint_manager import CheckpointManager
from processing.image_processor import ImageProcessorWithEmbedding, process_images_batch
from processing.report_generator import ReportGenerator
from src.utils.logger import setup_logger
from src.utils.memory_monitor import MemoryMonitor

logger = setup_logger()


@dataclass
class ProgressStats:
    """Статистика прогресса обработки"""
    total_records: int = 0
    processed_records: int = 0
    start_time: float = 0.0
    last_update_time: float = 0.0
    last_update_count: int = 0
    speeds: Deque[float] = None
    batch_times: Deque[Tuple[int, float]] = None
    memory_samples: Deque[float] = None
    eta_history: Deque[float] = None
    
    def __post_init__(self):
        if self.speeds is None:
            self.speeds = deque(maxlen=10)
        if self.batch_times is None:
            self.batch_times = deque(maxlen=5)
        if self.memory_samples is None:
            self.memory_samples = deque(maxlen=20)
        if self.eta_history is None:
            self.eta_history = deque(maxlen=3)
        if self.start_time == 0:
            self.start_time = time.time()
        self.last_update_time = self.start_time
        self.last_update_count = 0
    
    def update(self, processed: int, batch_size: int = 0, memory_usage_mb: float = 0):
        """Обновить статистику"""
        self.processed_records = processed
        
        current_time = time.time()
        time_since_last = current_time - self.last_update_time
        records_since_last = processed - self.last_update_count
        
        # Обновляем статистику скорости
        if time_since_last >= 5.0 and records_since_last > 0:
            speed = records_since_last / time_since_last
            self.speeds.append(speed)
            self.last_update_time = current_time
            self.last_update_count = processed
        
        # Обновляем статистику батчей
        if batch_size > 0:
            self.batch_times.append((batch_size, time_since_last))
        
        # Мониторинг памяти
        if memory_usage_mb > 0:
            self.memory_samples.append(memory_usage_mb)
    
    @property
    def progress_percent(self) -> float:
        if self.total_records == 0:
            return 0.0
        return (self.processed_records / self.total_records) * 100
    
    @property
    def records_per_second(self) -> float:
        if not self.speeds:
            return 0.0
        return sum(self.speeds) / len(self.speeds)
    
    @property
    def avg_batch_size(self) -> float:
        if not self.batch_times:
            return 0.0
        return sum(b[0] for b in self.batch_times) / len(self.batch_times)
    
    @property
    def max_memory_usage(self) -> float:
        if not self.memory_samples:
            return 0.0
        return max(self.memory_samples)
    
    @property
    def avg_memory_usage(self) -> float:
        if not self.memory_samples:
            return 0.0
        return sum(self.memory_samples) / len(self.memory_samples)
    
    def get_eta_seconds(self) -> float:
        """Получить оставшееся время в секундах"""
        if self.records_per_second == 0:
            return 0.0
        
        remaining = self.total_records - self.processed_records
        eta = remaining / self.records_per_second
        
        # Сглаживание ETA
        self.eta_history.append(eta)
        if len(self.eta_history) > 1:
            return sum(self.eta_history) / len(self.eta_history)
        return eta
    
    def get_progress_string(self, metrics: ProcessingMetrics) -> str:
        """Получить строку прогресса"""
        if self.total_records == 0:
            return "Ожидание..."
        
        # Время
        elapsed = time.time() - self.start_time
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)
        
        # ETA
        eta_seconds = self.get_eta_seconds()
        eta_hours = int(eta_seconds // 3600)
        eta_minutes = int((eta_seconds % 3600) // 60)
        eta_seconds = int(eta_seconds % 60)
        
        # Использование памяти
        try:
            memory_usage = psutil.virtual_memory().percent
            memory_status = "🟢" if memory_usage < 60 else "🟡" if memory_usage < 80 else "🔴"
        except:
            memory_usage = 0
            memory_status = "⚪"
        
        lines = [
            f"📊 {self.progress_percent:6.2f}% | 📈 {self.processed_records:,}/{self.total_records:,}",
            f"⚡ {self.records_per_second:.0f}/сек | ⏱️ {hours:02d}:{minutes:02d}:{seconds:02d}",
            f"⏳ ETA: {eta_hours:02d}:{eta_minutes:02d}:{eta_seconds:02d} | 🧠 {memory_status} {memory_usage:5.1f}%",
            f"🖼️ {metrics.valid_images:,}✅ {metrics.failed_images:,}❌ | 💾 {self.max_memory_usage/1024:.1f}GB"
        ]
        return " | ".join(lines)


class BatchProcessor:
    """Обработчик батчей с контролем памяти"""
    
    def __init__(self, image_processor: ImageProcessorWithEmbedding, metrics: ProcessingMetrics):
        self.image_processor = image_processor
        self.metrics = metrics
        self.last_memory_check = time.time()
        self.memory_lock = asyncio.Lock()
        
    async def process_batch(self, batch_data: List[Tuple[str, str]]) -> List[FaceRecord]:
        """Обработка батча записей"""
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
                logger.debug(f"Пустой батч после парсинга: {batch_size} записей")
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
                    logger.error(f"Ошибка при обработке изображений: {e}")
                    # Продолжаем с записями без изображений
            
            # Шаг 4: Создание объектов FaceRecord
            face_records = self._create_face_records(
                parsed_records, 
                record_indices, 
                image_results
            )
            
            # Шаг 5: Обновление статистики
            batch_time = time.time() - batch_start_time
            self.metrics.add_batch_time(batch_time)
            
            logger.debug(f"Обработан батч из {batch_size} записей за {batch_time:.2f} сек, создано {len(face_records)} объектов")
            return face_records
            
        except Exception as e:
            logger.error(f"Ошибка при обработке батча: {e}", exc_info=True)
            return self._create_fallback_records(batch_data)
    
    def _create_face_records(self, parsed_records: List[Dict], 
                           record_indices: List[int], 
                           image_results: List) -> List[FaceRecord]:
        """Создание объектов FaceRecord из распарсенных данных"""
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
                    
                    if img_result and isinstance(img_result, tuple) and len(img_result) >= 3:
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
        
        return face_records
    
    async def _check_memory_and_adjust(self):
        """Проверить память и при необходимости приостановить обработку"""
        async with self.memory_lock:
            current_time = time.time()
            
            # Проверяем не чаще чем раз в 5 секунд
            if current_time - self.last_memory_check < 5:
                return
            
            self.last_memory_check = current_time
            
            try:
                memory_percent = psutil.virtual_memory().percent
                available_gb = psutil.virtual_memory().available / (1024**3)
                
                # Адаптивная пауза в зависимости от использования памяти
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


class MemoryManager:
    """Менеджер памяти с улучшенным контролем"""
    
    def __init__(self):
        self.peak_memory = 0
        self.memory_samples = deque(maxlen=200)
        self.last_cleanup = time.time()
        self.cleanup_interval = 60  # секунд
        
    def is_memory_safe(self, additional_mb: float = 0) -> bool:
        """Проверить, безопасно ли выделять дополнительную память"""
        try:
            memory = psutil.virtual_memory()
            current_usage = memory.percent
            available_mb = memory.available / (1024**2)
            
            # Обновляем пиковое значение
            self.peak_memory = max(self.peak_memory, memory.used / (1024**3))
            
            # Сохраняем сэмпл
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
                current_time = time.time()
                if current_time - self.last_cleanup > self.cleanup_interval:
                    self.perform_cleanup()
                    self.last_cleanup = current_time
            
            return safe_percent and safe_available
            
        except Exception:
            return True  # При ошибке продолжаем работу
    
    def perform_cleanup(self):
        """Выполнить очистку памяти"""
        logger.debug("Выполняю очистку памяти...")
        try:
            # Очищаем кэш парсера
            parser = get_global_parser()
            if hasattr(parser, 'clear_cache'):
                parser.clear_cache()
                logger.debug("Очищен кэш парсера")
            
            # Принудительный сбор мусора
            for _ in range(2):
                gc.collect()
            
            logger.info("Выполнена очистка памяти")
            
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
        avg_percent = sum(s['percent'] for s in self.memory_samples) / len(self.memory_samples)
        current_percent = self.memory_samples[-1]['percent'] if self.memory_samples else 0
        
        return {
            'peak_memory_gb': self.peak_memory,
            'avg_memory_percent': avg_percent,
            'current_memory_percent': current_percent,
            'samples_count': len(self.memory_samples),
            'last_cleanup': self.last_cleanup
        }


class OptimizedFaceRecognitionProcessor:
    """
    Оптимизированный процессор обработки данных
    
    Особенности:
    - Минимальный оверхед на блокировки
    - Динамическая настройка размера батча
    - Эффективное управление памятью
    - Поддержка возобновления обработки
    """
    
    def __init__(self, formats: List[str], resume: bool = False):
        self.metrics = ProcessingMetrics()
        self.records: List[FaceRecord] = []
        self.image_processor = None
        self.formats = formats
        self.output_dir = ""
        self.resume = resume
        
        # Динамические настройки
        self.batch_size = Config.INITIAL_BATCH_SIZE
        self.max_batch_size = 20000
        self.min_batch_size = 500
        
        # Компоненты
        self.memory_manager = MemoryManager()
        self.batch_processor = None
        self.checkpoint_manager = None
        self.report_generator = None
        
        # Состояние обработки
        self.processed_hashes: Set[str] = set()
        self.progress_stats = ProgressStats()
        self.is_running = True
        
        # Блокировки
        self.metrics_lock = asyncio.Lock()
        self.records_lock = asyncio.Lock()
        
        logger.info(f"Инициализирован OptimizedFaceRecognitionProcessor с batch_size={self.batch_size}")
    
    async def process_file(self, input_file: str) -> bool:
        """Обработка файла с поддержкой возобновления"""
        logger.info(f"🎯 Начало обработки файла: {os.path.basename(input_file)}")
        
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
            
            # Создание папки для результатов
            self.output_dir = Config.setup_directories()
            print(f"📂 Результаты будут сохранены в: {self.output_dir}")
            
            # Инициализация компонентов
            await self._initialize_components(input_file, total_lines)
            
            # Запуск обработки
            return await self._run_processing_pipeline(input_file, total_lines)
            
        except KeyboardInterrupt:
            print("\n⚠️  Обработка прервана пользователем")
            await self._save_checkpoint_before_exit(input_file)
            return False
        except Exception as e:
            logger.error(f"Ошибка обработки: {e}", exc_info=True)
            return False
        finally:
            await self._final_cleanup()
    
    async def _initialize_components(self, input_file: str, total_lines: int):
        """Инициализация компонентов обработки"""
        # Инициализация менеджера чекпоинтов
        self.checkpoint_manager = CheckpointManager(self.output_dir)
        
        # Загрузка состояния чекпоинта
        start_position, _ = await self._load_checkpoint_state(input_file, total_lines)
        
        # Инициализация трекера прогресса
        self.progress_stats = ProgressStats(total_records=total_lines)
        
        # Инициализация процессора изображений
        print("🚀 Инициализация обработчика изображений...")
        self.image_processor = ImageProcessorWithEmbedding(self.output_dir)
        
        # Инициализация батч-процессора
        self.batch_processor = BatchProcessor(self.image_processor, self.metrics)
        
        # Инициализация генератора отчетов
        self.report_generator = ReportGenerator(self.output_dir)
    
    async def _run_processing_pipeline(self, input_file: str, total_lines: int) -> bool:
        """Запуск конвейера обработки"""
        print("\n" + "="*80)
        print("🚀 НАЧАЛО ОБРАБОТКИ")
        print("="*80)
        
        async with self.image_processor:
            # Создание задач
            processing_task = asyncio.create_task(
                self._process_file_stream(input_file, total_lines)
            )
            progress_task = asyncio.create_task(self._display_progress())
            
            try:
                # Ожидание завершения обработки
                success = await processing_task
                
                if success:
                    # Финальное сохранение и генерация отчетов
                    await self._finalize_processing()
                    return True
                else:
                    return False
                    
            except KeyboardInterrupt:
                print("\n⚠️  Обработка прервана пользователем")
                self.is_running = False
                success = False
                
                # Ждем завершения задач
                try:
                    await asyncio.wait_for(processing_task, timeout=5.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
                
                await self._save_checkpoint_before_exit(input_file)
                return False
            finally:
                # Остановка задач
                self.is_running = False
                progress_task.cancel()
                try:
                    await progress_task
                except asyncio.CancelledError:
                    pass
    
    async def _process_file_stream(self, input_file: str, total_lines: int) -> bool:
        """Потоковая обработка файла"""
        try:
            # Чтение файла с буферизацией
            buffer_size = 1024 * 1024 * 20  # 20MB буфер
            
            with open(input_file, 'r', encoding='utf-8', buffering=buffer_size) as f:
                # Определяем начальную позицию
                start_position = await self._get_start_position()
                if start_position > 0:
                    f.seek(start_position)
                    logger.info(f"Продолжаем с позиции: {start_position:,} байт")
                
                batch_data = []
                batch_count = 0
                current_position = start_position
                batch_start_time = time.time()
                
                # Чтение и обработка файла построчно
                for line in f:
                    if not self.is_running:
                        break
                    
                    line = line.strip()
                    if not line:
                        continue
                    
                    # Обновляем позицию в файле
                    current_position += len(line.encode('utf-8')) + 1  # +1 для символа новой строки
                    
                    # Генерируем хэш строки для проверки дубликатов
                    line_hash = hashlib.md5(line.encode()).hexdigest()[:16]
                    
                    # Проверка на дубликат
                    if line_hash in self.processed_hashes:
                        async with self.metrics_lock:
                            self.metrics.duplicate_records += 1
                        continue
                    
                    batch_data.append((line, line_hash))
                    self.processed_hashes.add(line_hash)
                    
                    # Обработка батча при достижении размера
                    if len(batch_data) >= self.batch_size:
                        await self._process_batch(
                            batch_data, 
                            current_position, 
                            batch_count, 
                            batch_start_time,
                            input_file,
                            total_lines
                        )
                        
                        batch_data = []
                        batch_count += 1
                        
                        # Динамическая настройка размера батча
                        self._adjust_batch_size_dynamically(batch_count)
                        
                        # Сброс времени старта батча
                        batch_start_time = time.time()
                
                # Обработка остатка
                if batch_data:
                    await self._process_batch(
                        batch_data, 
                        current_position, 
                        batch_count, 
                        batch_start_time,
                        input_file,
                        total_lines
                    )
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка в процессе обработки: {e}", exc_info=True)
            return False
    
    async def _process_batch(self, batch_data: List[Tuple[str, str]], 
                           current_position: int, 
                           batch_count: int,
                           batch_start_time: float,
                           input_file: str = None,
                           total_lines: int = 0):
        """Обработать батч и обновить состояние"""
        # Обработка батча
        batch_records = await self.batch_processor.process_batch(batch_data)
        
        # Безопасное обновление метрик
        async with self.metrics_lock:
            processed_in_batch = len(batch_data)
            self.metrics.total_records += processed_in_batch
            self.metrics.processed_records += len(batch_records)
        
        # Добавление записей в общий список
        async with self.records_lock:
            self.records.extend(batch_records)
        
        # Обновление прогресса
        if self.metrics.processed_records % 1000 == 0:
            memory_usage_mb = psutil.virtual_memory().used / (1024**2)
            self.progress_stats.update(
                self.metrics.processed_records,
                processed_in_batch,
                memory_usage_mb
            )
        
        # Сохранение чекпоинта
        if self.metrics.processed_records % Config.CHECKPOINT_INTERVAL == 0:
            await self._save_checkpoint_with_state(input_file, total_lines, current_position)
        
        # Оптимизация памяти
        if batch_count % 20 == 0:
            await self._optimize_memory_usage()
    
    async def _optimize_memory_usage(self):
        """Оптимизация использования памяти"""
        try:
            # Очистка кэша парсера
            parser = get_global_parser()
            if hasattr(parser, '_cache') and len(parser._cache) > 25000:
                parser.clear_cache()
                logger.debug(f"Очищен кэш парсера")
            
            # Промежуточное сохранение записей если их много
            if len(self.records) > 10000:
                await self._save_records_intermediate()
            
            # Принудительный сбор мусора
            gc.collect()
            
        except Exception as e:
            logger.debug(f"Ошибка оптимизации памяти: {e}")
    
    async def _save_records_intermediate(self):
        """Промежуточное сохранение записей для экономии памяти"""
        try:
            if len(self.records) < 5000:
                return
            
            # Сохраняем половину записей
            save_count = len(self.records) // 2
            records_to_save = self.records[:save_count]
            
            temp_file = os.path.join(
                self.output_dir,
                Config.TEMP_FOLDER,
                f"records_temp_{int(time.time())}.jsonl"
            )
            
            os.makedirs(os.path.dirname(temp_file), exist_ok=True)
            
            # Быстрая сериализация
            with open(temp_file, 'w', encoding='utf-8') as f:
                for record in records_to_save:
                    f.write(json.dumps(record.to_dict()) + '\n')
            
            # Удаляем сохраненные записи
            async with self.records_lock:
                del self.records[:save_count]
            
            logger.debug(f"Сохранено {save_count} записей во временный файл")
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении записей: {e}")
    
    async def _display_progress(self):
        """Отображение прогресса обработки"""
        last_update = 0
        
        while self.is_running:
            try:
                current_time = time.time()
                
                # Обновляем каждые 5 секунд
                if current_time - last_update >= 5.0:
                    progress_str = self.progress_stats.get_progress_string(self.metrics)
                    sys.stdout.write('\r' + progress_str + ' ' * 10)
                    sys.stdout.flush()
                    last_update = current_time
                
                await asyncio.sleep(2)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Ошибка в отображении прогресса: {e}")
                await asyncio.sleep(2)
        
        # Финальный вывод прогресса
        if hasattr(self, 'progress_stats'):
            progress_str = self.progress_stats.get_progress_string(self.metrics)
            sys.stdout.write('\r' + progress_str + ' ' * 10 + '\n')
            sys.stdout.flush()
    
    async def _finalize_processing(self):
        """Завершение обработки и генерация отчетов"""
        print("\n" + "="*80)
        print("📊 ИТОГОВАЯ СТАТИСТИКА ОБРАБОТКИ")
        print("="*80)
        
        # Основная статистика
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
        print("="*80)
        
        # Генерация отчетов
        if self.formats:
            print("\n📄 Генерация отчетов...")
            await self._generate_reports()
        
        print("\n" + "="*80)
        print("✨ ОБРАБОТКА ЗАВЕРШЕНА!")
        print("="*80)
    
    async def _generate_reports(self):
        """Генерация выбранных отчетов"""
        reports_created = []
        
        # Загрузка сохраненных записей если есть
        await self._load_saved_records()
        
        # Генерация HTML отчета
        if "HTML" in self.formats:
            print("🔄 Создание HTML отчета...")
            try:
                html_report = self.report_generator.generate_html_report(self.records, self.metrics)
                if html_report:
                    reports_created.append(("🌐 HTML отчет", html_report))
                    print("✅ HTML отчет создан")
            except Exception as e:
                print(f"❌ Ошибка создания HTML отчета: {e}")
        
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
        memory_stats = self.memory_manager.get_statistics()
        
        readme_content = f"""
# ОТЧЕТ ПО РАСПОЗНАВАНИЮ ЛИЦ

## 📊 Статистика обработки
- Дата создания: {datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")}
- Всего записей: {self.metrics.total_records:,}
- Успешных фото: {self.metrics.valid_images:,}
- Ошибок загрузки: {self.metrics.failed_images:,}
- Время обработки: {self.metrics.elapsed_time:.1f} сек
- Пиковое использование памяти: {memory_stats.get('peak_memory_gb', 0):.1f} GB

## 🚀 Особенности оптимизированной версии:
✅ Поддержка файлов более 2 ГБ
✅ Возобновление обработки после прерывания
✅ Контроль использования памяти (до 85% ОЗУ)
✅ Динамическая настройка размера батча
✅ Минимальный оверхед на блокировки
✅ Эффективное управление памятью
✅ Промежуточное сохранение результатов
"""
        
        for report_name, report_path in reports_created:
            if report_path:
                readme_content += f"- {report_name}: {os.path.basename(report_path)}\n"
        
        readme_path = os.path.join(self.output_dir, "README.txt")
        try:
            with open(readme_path, 'w', encoding='utf-8') as f:
                f.write(readme_content)
        except Exception as e:
            logger.error(f"Ошибка создания README файла: {e}")
    
    async def _load_saved_records(self):
        """Загрузка сохраненных записей из временных файлов"""
        temp_dir = os.path.join(self.output_dir, Config.TEMP_FOLDER)
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
                    with open(filepath, 'r', encoding='utf-8') as f:
                        for line in f:
                            if line.strip():
                                data = json.loads(line.strip())
                                record = FaceRecord(**data)
                                async with self.records_lock:
                                    self.records.append(record)
                                loaded_count += 1
                    
                    # Удаляем временный файл
                    os.remove(filepath)
                    
                except Exception as e:
                    logger.error(f"Ошибка загрузки файла {filepath}: {e}")
            
            if loaded_count > 0:
                logger.info(f"Загружено {loaded_count} записей из временных файлов")
                
        except Exception as e:
            logger.error(f"Ошибка загрузки сохраненных записей: {e}")
    
    async def _load_checkpoint_state(self, input_file: str, total_lines: int) -> Tuple[int, Dict[str, Any]]:
        """Загрузить состояние чекпоинта"""
        if not self.resume:
            print("🔄 Режим: НОВАЯ ОБРАБОТКА")
            return 0, {}
        
        if not self.checkpoint_manager:
            print("🔄 Режим: НОВАЯ ОБРАБОТКА (чекпоинт не настроен)")
            return 0, {}
        
        checkpoint_state = self.checkpoint_manager.load_checkpoint()
        if not checkpoint_state:
            print("🔄 Режим: НОВАЯ ОБРАБОТКА (чекпоинт не найден)")
            return 0, {}
        
        # Используем валидацию чекпоинта
        is_valid, message = self.checkpoint_manager.validate_checkpoint(input_file)
        if not is_valid:
            print(f"🔄 Режим: НОВАЯ ОБРАБОТКА (чекпоинт невалиден: {message})")
            self.checkpoint_manager.clear_checkpoint()
            return 0, {}
        
        # Восстанавливаем состояние
        print(f"🔄 Режим: ВОЗОБНОВЛЕНИЕ ПРОЦЕССА")
        print(f"📊 Найден валидный чекпоинт: {checkpoint_state.processed_lines:,}/{checkpoint_state.total_lines:,}")
        
        # Восстанавливаем метрики
        self.metrics.valid_images = checkpoint_state.valid_images
        self.metrics.failed_images = checkpoint_state.failed_images
        self.metrics.json_errors = checkpoint_state.json_errors
        self.metrics.cached_images = checkpoint_state.cached_images
        self.metrics.network_errors = checkpoint_state.network_errors
        self.metrics.timeout_errors = checkpoint_state.timeout_errors
        self.metrics.duplicate_records = checkpoint_state.duplicate_records
        
        # Восстанавливаем уникальные значения
        self.metrics.unique_users = set(checkpoint_state.unique_users)
        self.metrics.unique_devices = set(checkpoint_state.unique_devices)
        self.metrics.unique_companies = set(checkpoint_state.unique_companies)
        self.metrics.unique_ips = set(checkpoint_state.unique_ips)
        
        # Восстанавливаем хэши обработанных записей
        self.processed_hashes = set(checkpoint_state.records_processed)
        
        # Восстанавливаем размер батча
        self.batch_size = checkpoint_state.batch_size
        
        return checkpoint_state.last_position, checkpoint_state.__dict__
    
    async def _get_start_position(self) -> int:
        """Получить начальную позицию для обработки"""
        if self.checkpoint_manager and self.checkpoint_manager.state:
            return self.checkpoint_manager.state.last_position
        return 0
    
    async def _save_checkpoint_with_state(self, input_file: str, total_lines: int, position: int):
        """Сохранить чекпоинт с текущим состоянием"""
        if not self.checkpoint_manager:
            return
        
        try:
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
            
            logger.debug(f"Чекпоинт сохранен: {self.metrics.total_records:,} записей")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения чекпоинта: {e}")
    
    async def _save_checkpoint_before_exit(self, input_file: str):
        """Сохранить чекпоинт перед выходом"""
        try:
            # Пытаемся получить текущую позицию
            if os.path.exists(input_file):
                file_size = os.path.getsize(input_file)
                estimated_position = int(file_size * (self.progress_stats.progress_percent / 100))
                
                await self._save_checkpoint_with_state(
                    input_file,
                    self.progress_stats.total_records,
                    estimated_position
                )
                
                print("💾 Чекпоинт сохранен для восстановления")
        except Exception as e:
            logger.error(f"Ошибка сохранения чекпоинта при выходе: {e}")
    
    def _adjust_batch_size_dynamically(self, batch_count: int):
        """Динамическая настройка размера батча"""
        try:
            memory_percent = psutil.virtual_memory().percent
            available_gb = psutil.virtual_memory().available / (1024**3)
            cpu_percent = psutil.cpu_percent(interval=0.1)
            
            new_batch_size = self.batch_size
            
            # Регулировка на основе нагрузки
            if memory_percent > 85 or cpu_percent > 80 or available_gb < 0.5:
                # Высокая нагрузка - уменьшаем
                new_batch_size = max(self.min_batch_size, int(self.batch_size * 0.5))
            elif memory_percent > 70 or cpu_percent > 60:
                # Средняя нагрузка - немного уменьшаем
                new_batch_size = max(self.min_batch_size, int(self.batch_size * 0.7))
            elif memory_percent < 40 and cpu_percent < 40 and self.batch_size < self.max_batch_size:
                # Низкая нагрузка - увеличиваем
                new_batch_size = min(self.max_batch_size, int(self.batch_size * 1.5))
            
            # Применяем новое значение если изменилось
            if new_batch_size != self.batch_size and batch_count % 10 == 0:
                logger.info(f"Изменен размер батча: {self.batch_size:,} → {new_batch_size:,}")
                self.batch_size = new_batch_size
                
        except Exception as e:
            logger.debug(f"Ошибка настройки размера батча: {e}")
    
    async def _count_lines_optimized(self, file_path: str) -> int:
        """Оптимизированный подсчет строк в файле"""
        loop = asyncio.get_event_loop()
        
        def count_lines_sync():
            count = 0
            buffer_size = 1024 * 1024 * 16  # 16MB буфер
            
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
    
    async def _final_cleanup(self):
        """Финальная очистка ресурсов"""
        try:
            # Остановка трассировки памяти
            if tracemalloc.is_tracing():
                snapshot = tracemalloc.take_snapshot()
                top_stats = snapshot.statistics('lineno')[:5]
                
                logger.info("Топ-5 строк по использованию памяти:")
                for stat in top_stats:
                    logger.info(f"{stat}")
                
                tracemalloc.stop()
            
            # Очистка кэша парсера
            parser = get_global_parser()
            if hasattr(parser, 'clear_cache'):
                parser.clear_cache()
            
            # Очистка списков
            async with self.records_lock:
                self.records.clear()
            
            self.processed_hashes.clear()
            
            # Принудительный сбор мусора
            for _ in range(2):
                gc.collect()
            
            logger.info("Выполнена финальная очистка ресурсов")
            
        except Exception as e:
            logger.error(f"Ошибка при финальной очистке: {e}")

    def get_performance_report(self) -> Dict[str, Any]:
        """Получить отчет о производительности"""
        memory_stats = self.memory_manager.get_statistics()
        
        return {
            'total_processed': self.metrics.processed_records,
            'total_records': self.metrics.total_records,
            'valid_images': self.metrics.valid_images,
            'failed_images': self.metrics.failed_images,
            'duplicate_records': self.metrics.duplicate_records,
            'peak_memory_gb': memory_stats['peak_memory_gb'],
            'avg_memory_percent': memory_stats['avg_memory_percent'],
            'current_memory_percent': memory_stats['current_memory_percent'],
            'processing_speed_avg': self.progress_stats.records_per_second,
            'total_time_seconds': time.time() - self.progress_stats.start_time,
            'memory_samples_count': memory_stats['samples_count']
        }


def get_optimized_processor(formats: List[str], resume: bool = False) -> OptimizedFaceRecognitionProcessor:
    """Фабрика для создания оптимизированного процессора"""
    return OptimizedFaceRecognitionProcessor(formats, resume)