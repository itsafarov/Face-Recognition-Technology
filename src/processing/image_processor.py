"""
Оптимизированный обработчик изображений с встраиванием в HTML
"""

import os
import asyncio
import time
import hashlib
import base64
import gc
import random
import traceback
import ssl
import json
from typing import Optional, Tuple, Dict, List, Any, Deque, NamedTuple
from collections import deque, OrderedDict
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
from dataclasses import dataclass
from enum import Enum
from io import BytesIO
from contextlib import asynccontextmanager

import aiohttp
import aiofiles
import numpy as np
import cv2
from PIL import Image, ImageFile
import psutil

from core.config import Config
from core.models import ProcessingMetrics, FaceRecord, ImageMetrics
from utils.logger import setup_logger

logger = setup_logger()

# Разрешаем обработку усеченных изображений
ImageFile.LOAD_TRUNCATED_IMAGES = True


class ImageProcessingResult(NamedTuple):
    """Результат обработки изображения"""
    filepath: str
    base64_str: str
    image_info: Dict[str, Any]


class ImageCache:
    """Умный кэш для изображений с контролем памяти"""
    
    def __init__(self, max_size_mb: int = 200):
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.current_size_bytes = 0
        self.cache = OrderedDict()  # Для сохранения порядка использования
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        
    def get(self, key: str) -> Optional[bytes]:
        """Получить данные из кэша"""
        if key not in self.cache:
            self.misses += 1
            return None
        
        # Проверяем TTL (в этом простом кэше TTL не реализован,
        # но можно добавить при необходимости)
        data = self.cache[key]
        # Перемещаем в конец (сделали недавно использованным)
        self.cache.move_to_end(key)
        
        self.hits += 1
        return data
    
    def put(self, key: str, data: bytes) -> bool:
        """Добавить данные в кэш"""
        data_size = len(data)
        
        # Не кэшируем слишком большие файлы (>10% от максимального размера)
        if data_size > self.max_size_bytes * 0.1:
            return False
        
        # Если ключ уже существует, удаляем старое значение
        if key in self.cache:
            old_data = self.cache[key]
            self.current_size_bytes -= len(old_data)
            self.cache.pop(key)
        
        # Освобождаем место если нужно
        while (self.current_size_bytes + data_size > self.max_size_bytes 
               and self.cache):
            self._evict_oldest()
        
        # Добавляем только если есть место
        if self.current_size_bytes + data_size <= self.max_size_bytes:
            self.cache[key] = data
            self.current_size_bytes += data_size
            return True
        
        return False
    
    def _evict_oldest(self):
        """Удалить самую старую запись"""
        if not self.cache:
            return
        
        oldest_key = next(iter(self.cache))
        oldest_data = self.cache[oldest_key]
        del self.cache[oldest_key]
        self.current_size_bytes -= len(oldest_data)
        self.evictions += 1
    
    def clear(self):
        """Очистить кэш"""
        self.cache.clear()
        self.current_size_bytes = 0
        self.hits = 0
        self.misses = 0
        self.evictions = 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Получить статистику кэша"""
        hit_rate = 0
        if self.hits + self.misses > 0:
            hit_rate = (self.hits / (self.hits + self.misses)) * 100
        
        return {
            'size_bytes': self.current_size_bytes,
            'size_mb': self.current_size_bytes / (1024 * 1024),
            'items_count': len(self.cache),
            'hits': self.hits,
            'misses': self.misses,
            'evictions': self.evictions,
            'hit_rate_percent': hit_rate,
            'max_size_mb': self.max_size_bytes / (1024 * 1024)
        }


class DownloadDiagnostics:
    """Диагностика загрузки изображений"""
    
    def __init__(self, url: str):
        self.url = url
        self.attempts = 0
        self.errors: List[str] = []
        self.status_code: Optional[int] = None
        self.response_time_ms: float = 0.0
        self.size_bytes: int = 0
        self.success: bool = False
        self.file_type: str = "unknown"
        self.cached: bool = False
        
    def add_error(self, error: str):
        """Добавить ошибку"""
        self.errors.append(error[:100])  # Ограничиваем длину
    
    def to_dict(self) -> Dict[str, Any]:
        """Конвертировать в словарь"""
        return {
            'url': self.url[:100] + "..." if len(self.url) > 100 else self.url,
            'attempts': self.attempts,
            'errors': self.errors,
            'status_code': self.status_code,
            'response_time_ms': self.response_time_ms,
            'size_bytes': self.size_bytes,
            'success': self.success,
            'file_type': self.file_type,
            'cached': self.cached
        }


class RetryStrategy:
    """Стратегия повторных попыток с экспоненциальной задержкой"""
    
    @staticmethod
    async def execute_with_retry(
        func,
        max_attempts: int = 3,
        base_delay: float = 0.3,
        max_delay: float = 5.0
    ):
        """Выполнить функцию с повторными попытками"""
        last_exception = None
        
        for attempt in range(max_attempts):
            try:
                return await func()
            except Exception as e:
                last_exception = e
                
                if attempt == max_attempts - 1:
                    break
                
                # Экспоненциальная задержка с джиттером
                delay = min(
                    base_delay * (2 ** attempt) + random.uniform(0, 0.1),
                    max_delay
                )
                await asyncio.sleep(delay)
        
        raise last_exception


@dataclass
class ProcessingConfig:
    """Конфигурация обработки изображений"""
    max_workers: int = 15
    max_connections: int = 20
    timeout_seconds: int = 30
    max_retries: int = 3
    max_image_size_mb: int = 5
    memory_cache_size_mb: int = 200
    compression_quality: int = 85
    thumbnail_size: Tuple[int, int] = (120, 120)


class ImageProcessorWithEmbedding:
    """
    Оптимизированный обработчик изображений с встраиванием в HTML
    
    Особенности:
    - Умное кэширование с контролем памяти
    - Асинхронная загрузка с ограничением соединений
    - Пакетная обработка с пулом процессов
    - Автоматическая ретрай-стратегия
    - Безопасное SSL соединение
    """
    
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.config = ProcessingConfig()
        
        # Настройка на основе конфигурации системы
        self._adapt_to_system_resources()
        
        # Безопасный SSL контекст
        self.ssl_context = self._create_ssl_context()
        
        # HTTP клиент
        self.session: Optional[aiohttp.ClientSession] = None
        self.connector: Optional[aiohttp.TCPConnector] = None
        
        # Кэши
        self.memory_cache = ImageCache(self.config.memory_cache_size_mb)
        self.disk_cache_dir = os.path.join(base_dir, Config.CACHE_FOLDER)
        self.images_dir = os.path.join(base_dir, Config.IMAGE_FOLDER)
        
        # Пул процессов для CPU-bound операций (оптимально для Windows)
        max_processes = min(multiprocessing.cpu_count() - 1, 4)  # Ограничение для Windows
        self.process_pool = ProcessPoolExecutor(
            max_workers=max_processes,
            mp_context=multiprocessing.get_context('spawn')  # Для Windows
        )
        
        # Семафор для ограничения одновременных загрузок
        self.download_semaphore = asyncio.Semaphore(self.config.max_connections)
        
        # Статистика
        self.metrics: List[ImageMetrics] = []
        self.processing_times: Deque[float] = deque(maxlen=1000)
        self.total_processed = 0
        self.total_download_time = 0.0
        
        # Создание необходимых директорий
        self._create_directories()
        
        # Компрессионные параметры
        self.compression_params = [
            cv2.IMWRITE_JPEG_QUALITY, self.config.compression_quality,
            cv2.IMWRITE_JPEG_PROGRESSIVE, 1,
            cv2.IMWRITE_JPEG_OPTIMIZE, 1
        ]
        
        logger.info(f"Инициализирован ImageProcessor с кэшем {self.config.memory_cache_size_mb}MB")
    
    def _create_ssl_context(self) -> ssl.SSLContext:
        """Создать безопасный SSL контекст"""
        try:
            ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            ssl_context.check_hostname = True
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            
            # Для Windows добавляем дополнительные корневые сертификаты
            import platform
            if platform.system() == "Windows":
                # Windows хранит сертификаты в системном хранилище
                ssl_context.load_default_certs()
            
            return ssl_context
        except Exception as e:
            logger.warning(f"Не удалось создать SSL контекст: {e}. Использую системный по умолчанию")
            return ssl.create_default_context()
    
    def _adapt_to_system_resources(self):
        """Адаптировать конфигурацию к системным ресурсам"""
        try:
            # Адаптация на основе доступной памяти
            memory_gb = psutil.virtual_memory().total / (1024**3)
            cpu_cores = psutil.cpu_count(logical=False) or 4
            
            if memory_gb < 4:
                self.config.max_workers = 6
                self.config.max_connections = 8
                self.config.memory_cache_size_mb = 100
            elif memory_gb < 8:
                self.config.max_workers = 10
                self.config.max_connections = 15
                self.config.memory_cache_size_mb = 150
            else:
                self.config.max_workers = min(cpu_cores * 2, 15)
                self.config.max_connections = min(cpu_cores * 3, 25)
                self.config.memory_cache_size_mb = 200
            
            logger.debug(f"Адаптированная конфигурация: workers={self.config.max_workers}, "
                        f"connections={self.config.max_connections}, "
                        f"cache={self.config.memory_cache_size_mb}MB")
                        
        except Exception as e:
            logger.warning(f"Ошибка адаптации конфигурации: {e}")
    
    def _create_directories(self):
        """Создание необходимых директорий"""
        os.makedirs(self.images_dir, exist_ok=True)
        os.makedirs(self.disk_cache_dir, exist_ok=True)
        os.makedirs(os.path.join(self.base_dir, Config.TEMP_FOLDER), exist_ok=True)
    
    async def __aenter__(self):
        """Контекстный менеджер"""
        await self._initialize_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Закрытие ресурсов"""
        await self._close_resources()
        
        # Сохранение статистики
        await self._save_metrics()
    
    async def _initialize_session(self):
        """Инициализация HTTP сессии с безопасным SSL"""
        try:
            timeout = aiohttp.ClientTimeout(
                total=self.config.timeout_seconds,
                connect=5,
                sock_read=10,
                sock_connect=5
            )
            
            self.connector = aiohttp.TCPConnector(
                limit=self.config.max_connections,
                limit_per_host=8,
                ttl_dns_cache=300,
                force_close=True,
                enable_cleanup_closed=True,
                ssl=self.ssl_context  # Используем безопасный SSL контекст
            )
            
            self.session = aiohttp.ClientSession(
                connector=self.connector,
                timeout=timeout,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'DNT': '1'
                },
                raise_for_status=False
            )
            
            logger.debug("HTTP сессия инициализирована с безопасным SSL")
            
        except Exception as e:
            logger.error(f"Ошибка инициализации HTTP сессии: {e}")
            raise
    
    async def _close_resources(self):
        """Закрытие всех ресурсов"""
        try:
            # Закрытие HTTP сессии
            if self.session:
                await self.session.close()
                self.session = None
                logger.debug("HTTP сессия закрыта")
            
            # Завершение пула процессов
            if hasattr(self, 'process_pool'):
                self.process_pool.shutdown(wait=True)
                logger.debug("Пул процессов завершен")
            
            # Очистка кэша памяти
            self.memory_cache.clear()
            
            # Очистка памяти
            gc.collect()
            
            # Вывод статистики
            stats = self.memory_cache.get_stats()
            logger.info(f"ImageProcessor закрыт. Кэш хиты: {self.memory_cache.hits}, "
                       f"промахи: {self.memory_cache.misses}, "
                       f"хит-рейт: {stats['hit_rate_percent']:.1f}%")
            
        except Exception as e:
            logger.error(f"Ошибка при закрытии ресурсов: {e}")
    
    async def _save_metrics(self):
        """Сохранение метрик обработки изображений"""
        if not self.metrics:
            return
        
        try:
            metrics_file = os.path.join(self.disk_cache_dir, "image_metrics.json")
            metrics_data = [metric.to_dict() for metric in self.metrics]
            
            async with aiofiles.open(metrics_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(metrics_data, indent=2, ensure_ascii=False))
            
            # Сохранение сводной статистики
            await self._save_summary_statistics()
            
        except Exception as e:
            logger.error(f"Ошибка сохранения метрик: {e}")
    
    async def _save_summary_statistics(self):
        """Сохранение сводной статистики"""
        try:
            successful = [m for m in self.metrics if m.success]
            failed = [m for m in self.metrics if not m.success]
            
            if self.processing_times:
                avg_processing_time = sum(self.processing_times) / len(self.processing_times)
            else:
                avg_processing_time = 0
            
            summary = {
                "total_images": len(self.metrics),
                "successful": len(successful),
                "failed": len(failed),
                "success_rate": (len(successful) / len(self.metrics) * 100) if self.metrics else 0,
                "total_download_time_seconds": self.total_download_time,
                "avg_download_time_ms": (sum(m.download_time_ms for m in successful) / len(successful)) if successful else 0,
                "avg_processing_time_ms": avg_processing_time * 1000,
                "cached_images": sum(1 for m in self.metrics if m.is_cached),
                "avg_image_size_kb": (sum(m.size_kb for m in successful) / len(successful)) if successful else 0,
                "memory_cache_stats": self.memory_cache.get_stats(),
                "timestamp": time.time()
            }
            
            summary_file = os.path.join(self.disk_cache_dir, "image_summary.json")
            async with aiofiles.open(summary_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(summary, indent=2, ensure_ascii=False))
                
        except Exception as e:
            logger.error(f"Ошибка сохранения сводной статистики: {e}")
    
    def _generate_image_name(self, url: str) -> Tuple[str, str, str]:
        """Генерация уникальных имен файлов"""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        timestamp = int(time.time() * 1000)
        filename = f"photo_{url_hash}_{timestamp}.jpg"
        cache_filename = f"cache_{url_hash}.jpg"
        return url_hash, filename, cache_filename
    
    @staticmethod
    def _validate_image_data(data: bytes) -> Tuple[bool, str]:
        """Валидация данных изображения"""
        if len(data) < 100:
            return False, "File too small (<100 bytes)"
        
        # Проверка сигнатур изображений
        signatures = [
            (b'\xff\xd8\xff', 'JPEG'),
            (b'\x89PNG\r\n\x1a\n', 'PNG'),
            (b'GIF87a', 'GIF'),
            (b'GIF89a', 'GIF'),
            (b'BM', 'BMP'),
            (b'RIFF', 'WEBP'),
        ]
        
        for sig, file_type in signatures:
            if data.startswith(sig):
                return True, file_type
        
        # Попробуем определить по заголовкам
        if b'JFIF' in data[:100] or b'Exif' in data[:100]:
            return True, "JPEG"
        
        return False, "Invalid image format"
    
    async def _download_image_with_retry(self, url: str) -> Tuple[Optional[bytes], DownloadDiagnostics]:
        """Загрузка изображения с повторными попытками и безопасным SSL"""
        diagnostics = DownloadDiagnostics(url)
        
        async def attempt_download():
            diagnostics.attempts += 1
            start_time = time.time()
            
            try:
                async with self.download_semaphore:
                    async with self.session.get(
                        url, 
                        allow_redirects=True,
                        ssl=self.ssl_context,  # Используем безопасный SSL
                        compress=True
                    ) as response:
                        
                        diagnostics.status_code = response.status
                        
                        if response.status != 200:
                            diagnostics.add_error(f"HTTP {response.status}")
                            return None
                        
                        # Читаем данные с ограничением по размеру
                        max_size = self.config.max_image_size_mb * 1024 * 1024
                        data = bytearray()
                        
                        async for chunk in response.content.iter_chunked(8192):
                            data.extend(chunk)
                            if len(data) > max_size:
                                diagnostics.add_error(f"File too large (> {self.config.max_image_size_mb}MB)")
                                return None
                        
                        diagnostics.size_bytes = len(data)
                        diagnostics.response_time_ms = (time.time() - start_time) * 1000
                        
                        # Валидация изображения
                        is_valid, file_type = self._validate_image_data(bytes(data))
                        if not is_valid:
                            diagnostics.add_error(f"Invalid image format ({file_type})")
                            return None
                        
                        diagnostics.file_type = file_type
                        diagnostics.success = True
                        
                        return bytes(data)
                        
            except asyncio.TimeoutError:
                diagnostics.add_error("Timeout")
            except aiohttp.ClientError as e:
                diagnostics.add_error(f"Client error: {str(e)[:50]}")
            except ssl.SSLError as e:
                diagnostics.add_error(f"SSL error: {str(e)[:50]}")
            except Exception as e:
                diagnostics.add_error(f"Unexpected error: {str(e)[:50]}")
            
            return None
        
        # Выполнение с повторными попытками
        try:
            return await RetryStrategy.execute_with_retry(
                attempt_download,
                max_attempts=self.config.max_retries,
                base_delay=0.3,
                max_delay=5.0
            ), diagnostics
            
        except Exception as e:
            diagnostics.add_error(f"All download attempts failed: {str(e)[:50]}")
            return None, diagnostics
    
    
    async def process_image(self, url: str, metrics: ProcessingMetrics) -> ImageProcessingResult:
        """
        Полная обработка изображения
        
        Returns:
            Tuple: (filepath, base64_string, image_info) или (None, None, error_info)
        """
        if not url or 'http' not in url.lower():
            metrics.network_errors += 1
            return ImageProcessingResult("", "", {"failed_reason": "Invalid URL"})
        
        start_time = time.time()
        url_hash, filename, cache_filename = self._generate_image_name(url)
        
        # Создание метрики для отслеживания
        image_metric = ImageMetrics(
            url=url,
            hash=url_hash,
            download_time_ms=0,
            processing_time_ms=0,
            size_kb=0,
            width=0,
            height=0,
            is_cached=False,
            success=False
        )
        
        # Шаг 1: Проверка кэша на диске
        cache_path = os.path.join(self.disk_cache_dir, cache_filename)
        if os.path.exists(cache_path):
            try:
                result = await self._load_from_cache(cache_path, url_hash)
                if result:
                    metrics.cached_images += 1
                    self._update_image_metric(image_metric, True, result[2], 0)
                    self.metrics.append(image_metric)
                    return result
            except Exception as e:
                logger.debug(f"Cache read error: {e}")
        
        # Шаг 2: Проверка памяти кэша
        cached_data = self.memory_cache.get(url)
        if cached_data:
            try:
                result = await self._process_cached_data(cached_data, url_hash)
                if result:
                    metrics.cached_images += 1
                    self._update_image_metric(image_metric, True, result[2], 0)
                    self.metrics.append(image_metric)
                    return result
            except Exception as e:
                logger.debug(f"Memory cache processing error: {e}")
        
        # Шаг 3: Загрузка нового изображения
        download_start = time.time()
        image_data, diagnostics = await self._download_image_with_retry(url)
        
        if image_data:
            # Сохранение в кэш памяти
            self.memory_cache.put(url, image_data)
            
            # Обработка изображения
            process_start = time.time()
            result = await self._process_image_data(image_data, url_hash)
            processing_time = time.time() - process_start
            
            if result and result[0] and result[1]:  # filepath и base64_str не пустые
                metrics.valid_images += 1
                download_time = (time.time() - download_start) * 1000
                self.total_download_time += time.time() - download_start
                
                self._update_image_metric(
                    image_metric, 
                    True, 
                    result[2], 
                    download_time,
                    processing_time * 1000
                )
                
                self.metrics.append(image_metric)
                gc.collect()
                
                return result
            else:
                # Изображение загружено, но не обработано
                error_msg = "Failed to process image data"
                self._update_image_metric(image_metric, False, {"failed_reason": error_msg}, 0)
                self.metrics.append(image_metric)
                metrics.failed_images += 1
                
                return ImageProcessingResult("", "", {
                    "failed_reason": error_msg,
                    "attempts": diagnostics.attempts,
                    "diagnostics": diagnostics.to_dict()
                })
        else:
            # Ошибка загрузки
            error_msg = diagnostics.errors[0] if diagnostics.errors else "Download failed"
            if diagnostics.status_code:
                error_msg = f"HTTP {diagnostics.status_code}: {error_msg}"
            
            self._update_image_metric(image_metric, False, {"failed_reason": error_msg}, 0)
            self.metrics.append(image_metric)
            metrics.failed_images += 1
            
            if diagnostics.status_code in [404, 403, 500]:
                metrics.network_errors += 1
            
            return ImageProcessingResult("", "", {
                "failed_reason": error_msg,
                "attempts": diagnostics.attempts,
                "diagnostics": diagnostics.to_dict()
            })
    
    async def _load_from_cache(self, cache_path: str, url_hash: str) -> Optional[ImageProcessingResult]:
        """Загрузка изображения из кэша на диске"""
        loop = asyncio.get_event_loop()
        
        def read_and_process():
            try:
                with open(cache_path, 'rb') as f:
                    img_data = f.read()
                
                # Обработка для создания thumbnail
                nparr = np.frombuffer(img_data, np.uint8)
                img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                
                if img is not None:
                    height, width = img.shape[:2]
                    
                    # Создание миниатюры
                    thumbnail_size = self.config.thumbnail_size
                    if width > thumbnail_size[0] or height > thumbnail_size[1]:
                        scale = min(thumbnail_size[0] / width, thumbnail_size[1] / height)
                        new_width = int(width * scale)
                        new_height = int(height * scale)
                        img_resized = cv2.resize(img, (new_width, new_height), cv2.INTER_AREA)
                    else:
                        img_resized = img
                    
                    success, buffer = cv2.imencode('.jpg', img_resized, [
                        cv2.IMWRITE_JPEG_QUALITY, 85,
                        cv2.IMWRITE_JPEG_OPTIMIZE, 1
                    ])
                    
                    if success:
                        base64_str = base64.b64encode(buffer.tobytes()).decode('utf-8')
                        
                        # Поиск оригинального файла
                        import glob
                        check_pattern = f"photo_{url_hash}_*.jpg"
                        existing = glob.glob(os.path.join(self.images_dir, check_pattern))
                        filepath = existing[0] if existing else ""
                        
                        return ImageProcessingResult(
                            filepath=filepath,
                            base64_str=base64_str,
                            image_info={
                                "width": width,
                                "height": height,
                                "file_size_kb": len(img_data) / 1024,
                                "from_cache": True
                            }
                        )
            except Exception as e:
                logger.debug(f"Error reading from disk cache: {e}")
            
            return None
        
        return await loop.run_in_executor(self.process_pool, read_and_process)
    
    async def _process_cached_data(self, image_data: bytes, url_hash: str) -> Optional[ImageProcessingResult]:
        """Обработка данных из кэша памяти"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.process_pool,
            _process_image_sync_static,
            image_data,
            url_hash,
            self.images_dir,
            self.compression_params
        )
    
    async def _process_image_data(self, image_data: bytes, url_hash: str) -> Optional[ImageProcessingResult]:
        """Асинхронная обработка данных изображения"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.process_pool,
            _process_image_sync_static,
            image_data,
            url_hash,
            self.images_dir,
            self.compression_params
        )
    
    def _update_image_metric(self, metric: ImageMetrics, success: bool, 
                           info: Dict[str, Any], download_time_ms: float,
                           processing_time_ms: float = 0):
        """Обновление метрики изображения"""
        metric.download_time_ms = int(download_time_ms)
        metric.processing_time_ms = int(processing_time_ms)
        metric.success = success
        
        if success and info:
            metric.size_kb = info.get("file_size_kb", 0)
            metric.width = info.get("width", 0)
            metric.height = info.get("height", 0)
            metric.is_cached = info.get("from_cache", False) or info.get("is_cached", False)
        
        if not success and info:
            metric.error_message = info.get("failed_reason", "Unknown error")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Получить статистику обработчика изображений"""
        successful = [m for m in self.metrics if m.success]
        
        if self.processing_times:
            avg_processing_time = sum(self.processing_times) / len(self.processing_times)
        else:
            avg_processing_time = 0
        
        cache_stats = self.memory_cache.get_stats()
        
        return {
            "total_processed": len(self.metrics),
            "successful": len(successful),
            "failed": len(self.metrics) - len(successful),
            "success_rate": (len(successful) / len(self.metrics) * 100) if self.metrics else 0,
            "cached_count": sum(1 for m in self.metrics if m.is_cached),
            "total_download_time_seconds": self.total_download_time,
            "avg_download_time_ms": (sum(m.download_time_ms for m in successful) / len(successful)) if successful else 0,
            "avg_processing_time_ms": avg_processing_time * 1000,
            "avg_image_size_kb": (sum(m.size_kb for m in successful) / len(successful)) if successful else 0,
            "memory_cache_stats": cache_stats
        }


def _process_image_sync_static(image_data: bytes, url_hash: str, images_dir: str, compression_params: list) -> Optional[ImageProcessingResult]:
    """Синхронная обработка изображения (выполняется в отдельном процессе)"""
    start_time = time.time()

    try:
        # Декодирование через OpenCV
        nparr = np.frombuffer(image_data, np.uint8)
        img_np = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if img_np is None:
            # Попытка через PIL как запасной вариант
            try:
                with Image.open(BytesIO(image_data)) as img_pil:
                    # Конвертация в RGB
                    if img_pil.mode in ('RGBA', 'LA', 'P'):
                        if img_pil.mode == 'RGBA':
                            background = Image.new('RGB', img_pil.size, (255, 255, 255))
                            background.paste(img_pil, mask=img_pil.split()[3])
                            img_pil = background
                        else:
                            img_pil = img_pil.convert('RGB')
                    elif img_pil.mode != 'RGB':
                        img_pil = img_pil.convert('RGB')

                    img_np = np.array(img_pil)
                    img_np = cv2.cvtColor(img_np, cv2.COLOR_RGB2BGR)
            except Exception as e:
                logger.debug(f"PIL decode failed: {e}")
                return None

        # Проверка валидности
        if img_np.size == 0 or img_np.shape[0] == 0 or img_np.shape[1] == 0:
            return None

        # Получаем размеры
        height, width = img_np.shape[:2]

        # Масштабирование очень больших изображений
        if width > 5000 or height > 5000:
            scale = min(5000 / width, 5000 / height)
            new_width = int(width * scale)
            new_height = int(height * scale)
            img_np = cv2.resize(img_np, (new_width, new_height), cv2.INTER_AREA)
            height, width = img_np.shape[:2]

        # Создание миниатюры
        thumbnail_size = (120, 120)  # Default size
        scale = min(thumbnail_size[0] / width, thumbnail_size[1] / height, 1.0)
        new_width = int(width * scale)
        new_height = int(height * scale)

        # Выбор интерполяции
        interpolation = cv2.INTER_AREA if scale < 0.5 else cv2.INTER_LINEAR
        img_resized = cv2.resize(img_np, (new_width, new_height), interpolation=interpolation)

        # Кодирование в base64
        success, buffer = cv2.imencode('.jpg', img_resized, [
            cv2.IMWRITE_JPEG_QUALITY, 85,
            cv2.IMWRITE_JPEG_OPTIMIZE, 1
        ])

        if not success:
            return None

        base64_str = base64.b64encode(buffer.tobytes()).decode('utf-8')

        # Сохранение оригинального изображения
        timestamp = int(time.time() * 1000)
        filename = f"photo_{url_hash}_{timestamp}.jpg"
        filepath = os.path.join(images_dir, filename)

        cv2.imwrite(filepath, img_np, compression_params)

        # Получение размера файла
        file_size_kb = os.path.getsize(filepath) / 1024
        processing_time = time.time() - start_time

        return ImageProcessingResult(
            filepath=filepath,
            base64_str=base64_str,
            image_info={
                "width": width,
                "height": height,
                "file_size_kb": file_size_kb,
                "original_size": len(image_data),
                "processing_time": processing_time,
                "thumbnail_size": (new_width, new_height)
            }
        )

    except Exception as e:
        logger.debug(f"Error processing image {url_hash[:8]}: {e}")
        return None


async def process_images_batch(processor: ImageProcessorWithEmbedding, 
                             urls: List[str], 
                             metrics: ProcessingMetrics) -> List[ImageProcessingResult]:
    """
    Пакетная обработка изображений с ограничением параллелизма
    
    Args:
        processor: Обработчик изображений
        urls: Список URL для обработки
        metrics: Метрики обработки
        
    Returns:
        List[Tuple]: Результаты обработки
    """
    if not urls:
        return []
    
    # Динамическая настройка параллелизма
    try:
        available_memory_gb = psutil.virtual_memory().available / (1024**3)
        
        if available_memory_gb < 1:
            max_concurrent = 4
        elif available_memory_gb < 2:
            max_concurrent = 8
        elif available_memory_gb < 4:
            max_concurrent = 12
        else:
            max_concurrent = min(len(urls), 20)
    except:
        max_concurrent = min(len(urls), 12)
    
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def process_single(url: str):
        async with semaphore:
            try:
                return await processor.process_image(url, metrics)
            except Exception as e:
                logger.error(f"Error processing image {url[:50]}: {e}")
                return ImageProcessingResult("", "", {"failed_reason": str(e)})
    
    # Создание задач с ограничением
    tasks = []
    for url in urls:
        task = asyncio.create_task(process_single(url))
        tasks.append(task)
    
    try:
        # Обработка с таймаутом
        timeout_duration = processor.config.timeout_seconds * 2
        
        results = await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=timeout_duration
        )
        
        # Обработка результатов
        processed_results = []
        for result in results:
            if isinstance(result, Exception):
                processed_results.append(ImageProcessingResult("", "", {"failed_reason": str(result)}))
            else:
                processed_results.append(result)
        
        return processed_results
        
    except asyncio.TimeoutError:
        logger.warning(f"Batch processing timeout for {len(urls)} images")
        
        # Возвращаем частичные результаты
        processed_results = []
        for task in tasks:
            if task.done():
                try:
                    result = task.result()
                    processed_results.append(result)
                except Exception as e:
                    processed_results.append(ImageProcessingResult("", "", {"failed_reason": str(e)}))
            else:
                processed_results.append(ImageProcessingResult("", "", {"failed_reason": "Timeout"}))
        
        return processed_results
        
    except Exception as e:
        logger.error(f"Batch processing error: {e}")
        return [ImageProcessingResult("", "", {"failed_reason": str(e)})] * len(urls)


def create_thumbnail_from_file(file_path: str, thumbnail_size: tuple = (120, 120)) -> Optional[str]:
    """
    Создание base64 thumbnail из файла (синхронная версия)
    
    Args:
        file_path: Путь к файлу изображения
        thumbnail_size: Размер миниатюры
        
    Returns:
        str: base64 строка или None при ошибке
    """
    try:
        with open(file_path, 'rb') as f:
            data = f.read()
        
        nparr = np.frombuffer(data, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if img is None:
            return None
        
        height, width = img.shape[:2]
        scale = min(thumbnail_size[0] / width, thumbnail_size[1] / height, 1.0)
        
        if scale < 1:
            new_width = int(width * scale)
            new_height = int(height * scale)
            img = cv2.resize(img, (new_width, new_height), cv2.INTER_AREA)
        
        success, buffer = cv2.imencode('.jpg', img, [
            cv2.IMWRITE_JPEG_QUALITY, 85,
            cv2.IMWRITE_JPEG_OPTIMIZE, 1
        ])
        
        if success:
            return base64.b64encode(buffer).decode('utf-8')
        
        return None
        
    except Exception as e:
        logger.debug(f"Error creating thumbnail: {e}")
        return None


async def cleanup_old_cache(cache_dir: str, max_age_hours: int = 24):
    """
    Очистка старых файлов кэша
    
    Args:
        cache_dir: Директория кэша
        max_age_hours: Максимальный возраст файлов в часах
    """
    try:
        current_time = time.time()
        files_removed = 0
        bytes_freed = 0
        
        if not os.path.exists(cache_dir):
            return
        
        for filename in os.listdir(cache_dir):
            if filename.startswith('cache_'):
                filepath = os.path.join(cache_dir, filename)
                
                if os.path.isfile(filepath):
                    file_age = current_time - os.path.getmtime(filepath)
                    
                    if file_age > max_age_hours * 3600:
                        try:
                            file_size = os.path.getsize(filepath)
                            os.remove(filepath)
                            files_removed += 1
                            bytes_freed += file_size
                        except Exception as e:
                            logger.debug(f"Error removing cache file {filename}: {e}")
        
        if files_removed > 0:
            logger.info(f"Removed {files_removed} old cache files, freed {bytes_freed/1024/1024:.1f} MB")
            
    except Exception as e:
        logger.error(f"Error cleaning up cache: {e}")


class BatchImageProcessor:
    """Обработчик пакетной обработки изображений с улучшенным управлением памятью"""
    
    def __init__(self, processor: ImageProcessorWithEmbedding):
        self.processor = processor
        self.batch_size = 50
        self.max_retries = 2
        
    async def process_batch(self, urls: List[str], metrics: ProcessingMetrics) -> List[ImageProcessingResult]:
        """Обработка пакета URL с контролем памяти"""
        results = []
        
        for i in range(0, len(urls), self.batch_size):
            batch = urls[i:i + self.batch_size]
            batch_results = await process_images_batch(self.processor, batch, metrics)
            results.extend(batch_results)
            
            # Очистка памяти между пакетами
            if i % (self.batch_size * 5) == 0:
                gc.collect()
        
        return results