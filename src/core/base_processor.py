"""
Базовый класс процессора для устранения дублирования кода
"""

import os
import sys
import gc
import asyncio
import time
import psutil
import tracemalloc
from typing import List, Tuple, Set, Dict, Any, Optional
from abc import ABC, abstractmethod

from core.config import Config
from core.models import ProcessingMetrics, FaceRecord
from core.data_parser import get_global_parser
from core.checkpoint_manager import CheckpointManager
from processing.image_processor import ImageProcessorWithEmbedding
from utils.memory_monitor import MemoryMonitor


class BaseFaceRecognitionProcessor(ABC):
    """Абстрактный базовый класс для всех процессоров"""
    
    def __init__(self, formats: List[str], resume: bool = False):
        self.metrics = ProcessingMetrics()
        self.records: List[FaceRecord] = []
        self.image_processor = None
        self.formats = formats
        self.output_dir = ""
        self.checkpoint_manager = None
        self.resume = resume
        
        # Динамические настройки
        self.batch_size = Config.INITIAL_BATCH_SIZE
        self.max_batch_size = 20000
        self.min_batch_size = 500
        
        # Компоненты
        self.memory_monitor = MemoryMonitor()
        
        # Состояние обработки
        self.processed_hashes: Set[str] = set()
        self.processed_since_checkpoint = 0
        self.is_running = True
        
        # Статистика
        self.total_batches_processed = 0
        self.avg_batch_processing_time = 0
        
        # Блокировки
        self.memory_lock = asyncio.Lock()
    
    @abstractmethod
    async def process_file(self, input_file: str):
        """Абстрактный метод обработки файла"""
        pass
    
    @abstractmethod
    async def _process_file_optimized(self, input_file: str, total_lines: int, start_position: int):
        """Абстрактный метод оптимизированной обработки"""
        pass
    
    # Общие методы, которые могут использоваться в наследниках
    
    async def _count_lines_optimized(self, file_path: str) -> int:
        """Оптимизированный подсчет строк в файле (общая реализация)"""
        import concurrent.futures
        
        def count_lines_sync():
            count = 0
            buffer_size = 1024 * 1024 * 16
            with open(file_path, 'r', encoding='utf-8', errors='ignore', 
                     buffering=buffer_size) as f:
                while True:
                    buffer = f.read(buffer_size)
                    if not buffer:
                        break
                    count += buffer.count('\n')
            return count
        
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            result = await loop.run_in_executor(executor, count_lines_sync)
        return result
    
    async def _optimize_memory_usage_safe(self):
        """Безопасная оптимизация использования памяти с блокировкой"""
        async with self.memory_lock:
            await self._optimize_memory_usage_impl()
    
    async def _optimize_memory_usage_impl(self):
        """Реализация оптимизации памяти (может переопределяться)"""
        try:
            # Очистка кэша парсера если он слишком большой
            parser = get_global_parser()
            if parser and hasattr(parser, '_cache'):
                cache_size = len(parser._cache)
                if cache_size > 15000:
                    parser.clear_cache()
            
            # Принудительный сбор мусора
            gc.collect()
            
            # Проверка памяти и приостановка если нужно
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > 85:
                await asyncio.sleep(2)
                
        except Exception as e:
            import logging
            logging.getLogger(__name__).debug(f"Memory optimization error: {e}")
    
    def _save_checkpoint(self, input_file: str, total_lines: int, position: int):
        """Сохранение чекпоинта (общая реализация)"""
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
    
    async def _final_cleanup(self):
        """Финальная очистка ресурсов (общая реализация)"""
        try:
            # Очистка кэша парсера
            parser = get_global_parser()
            if hasattr(parser, 'clear_cache'):
                parser.clear_cache()
            
            # Очистка списков
            self.records.clear()
            self.processed_hashes.clear()
            
            # Принудительный сбор мусора
            for _ in range(3):
                gc.collect()
            
        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f"Final cleanup error: {e}")