"""
Обработчик изображений с встраиванием в HTML
"""
import os
import glob
import time
import hashlib
import base64
import asyncio
import concurrent.futures
from typing import Optional, Tuple, Dict, List, Any
from collections import OrderedDict
from io import BytesIO
import traceback
import gc
import ujson as json
import aiohttp
import aiofiles
import numpy as np
import cv2
from PIL import Image, ImageFile, ImageOps
import psutil
from core.config import Config
from core.models import ProcessingMetrics, FaceRecord, ImageMetrics
from utils.logger import setup_logging
logger = setup_logging()

# Разрешаем обработку усеченных изображений
ImageFile.LOAD_TRUNCATED_IMAGES = True

# Создаем пул потоков для CPU-bound операций
THREAD_POOL = concurrent.futures.ThreadPoolExecutor(
    max_workers=min(Config.MAX_WORKERS, 8),
    thread_name_prefix="ImageProcessor"
)

# Глобальный кэш для повторно используемых объектов
COMPRESSION_PARAMS = [
    cv2.IMWRITE_JPEG_QUALITY, Config.IMAGE_QUALITY,
    cv2.IMWRITE_JPEG_PROGRESSIVE, 1,
    cv2.IMWRITE_JPEG_OPTIMIZE, 1
]
THUMBNAIL_COMPRESSION_PARAMS = [
    cv2.IMWRITE_JPEG_QUALITY, 85,
    cv2.IMWRITE_JPEG_OPTIMIZE, 1
]


class ImageProcessorWithEmbedding:
    """Обработчик изображений с встраиванием в HTML"""
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.session: Optional[aiohttp.ClientSession] = None
        # Динамический семафор на основе доступной памяти
        self._update_semaphore()
        self.cache = OrderedDict()
        self.cache_size = 1000
        self.cache_max_bytes = 200 * 1024 * 1024  # 200 MB максимум кэша в памяти
        self.cache_current_bytes = 0
        # Статистика
        self.image_metrics: List[ImageMetrics] = []
        self.total_download_time = 0.0
        self.total_images_processed = 0
        # Мониторинг производительности
        self.processing_times = []
        self.cache_hits = 0
        self.cache_misses = 0
        # Создаем папки
        Config.setup_directories(base_dir)
        # Папка для изображений
        self.images_dir = os.path.join(base_dir, Config.IMAGE_FOLDER)
        self.cache_dir = os.path.join(base_dir, Config.CACHE_FOLDER)
        # Создаем папку кэша если нет
        os.makedirs(self.cache_dir, exist_ok=True)
        # Подготовка пула соединений
        self._prepare_connection_pool()
        logger.info(f"Инициализирован ImageProcessor с кэшем {self.cache_max_bytes/1024/1024:.0f}MB")

    def _update_semaphore(self):
        """Обновить семафор на основе доступной памяти"""
        try:
            available_memory_gb = psutil.virtual_memory().available / (1024**3)
            if available_memory_gb < 1:
                max_workers = 4
            elif available_memory_gb < 2:
                max_workers = 8
            elif available_memory_gb < 4:
                max_workers = 12
            else:
                max_workers = min(Config.MAX_WORKERS, 15)
            self.semaphore = asyncio.Semaphore(max_workers)
            logger.debug(f"Семафор обновлен: {max_workers} рабочих потоков")
        except:
            self.semaphore = asyncio.Semaphore(min(Config.MAX_WORKERS, 10))

    def _prepare_connection_pool(self):
        """Подготовить настройки пула соединений"""
        self.connector_settings = {
            'limit': min(Config.MAX_WORKERS, 20),
            'limit_per_host': 8,
            'ttl_dns_cache': 300,
            'force_close': True,
            'enable_cleanup_closed': True,
            'ssl': False
        }

    async def __aenter__(self):
        """Контекстный менеджер"""
        try:
            await self._create_session()
        except Exception as e:
            logger.error(f"Ошибка создания HTTP сессии: {e}")
            raise
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Закрытие сессии"""
        try:
            if self.session:
                await self.session.close()
            # Очищаем кэш
            self.cache.clear()
            self.cache_current_bytes = 0
            # Очистка памяти
            gc.collect()
            # Сохраняем метрики изображений
            await self._save_image_metrics()
            logger.info(f"ImageProcessor закрыт. Кэш хиты: {self.cache_hits}, промахи: {self.cache_misses}")
        except Exception as e:
            logger.error(f"Ошибка при закрытии процессора изображений: {e}")

    async def _create_session(self):
        """Создание HTTP сессии с улучшенными настройками"""
        timeout = aiohttp.ClientTimeout(
            total=Config.REQUEST_TIMEOUT,
            connect=5,
            sock_read=10,
            sock_connect=5
        )
        connector = aiohttp.TCPConnector(**self.connector_settings)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'DNT': '1',
                'Sec-Fetch-Dest': 'image',
                'Sec-Fetch-Mode': 'no-cors',
                'Sec-Fetch-Site': 'cross-site',
            },
            raise_for_status=False
        )

    def _generate_image_name(self, url: str) -> tuple:
        """Генерация уникальных имен файлов"""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        timestamp = int(time.time() * 1000)
        filename = f"photo_{url_hash}_{timestamp}.jpg"
        cache_filename = f"cache_{url_hash}.jpg"
        return url_hash, filename, cache_filename

    async def download_image(self, url: str) -> Tuple[Optional[bytes], Dict[str, Any]]:
        """Загрузка изображения с расширенной диагностикой"""
        if not url or 'http' not in url.lower():
            return None, {"error": "Invalid URL", "reason": "No HTTP URL"}

        # Проверка кэша в памяти
        if url in self.cache:
            data = self.cache[url]
            # Перемещаем в конец (сделали недавно использованным)
            self.cache.move_to_end(url)
            self.cache_hits += 1
            return data, {"cached": True, "source": "memory_cache"}

        self.cache_misses += 1

        diagnostics = {
            "url": url[:100] + "..." if len(url) > 100 else url,
            "attempts": 0,
            "errors": [],
            "status_code": None,
            "response_time": 0,
            "size_bytes": 0,
            "success": False
        }

        for attempt in range(Config.REQUEST_RETRIES + 1):
            diagnostics["attempts"] = attempt + 1
            start_time = time.time()
            try:
                async with self.semaphore:
                    async with self.session.get(
                        url, 
                        allow_redirects=True, 
                        ssl=False,
                        compress=True
                    ) as response:
                        diagnostics["status_code"] = response.status
                        diagnostics["response_time"] = time.time() - start_time
                        if response.status != 200:
                            diagnostics["errors"].append(f"HTTP {response.status}")
                            if attempt == Config.REQUEST_RETRIES:
                                return None, diagnostics
                            await asyncio.sleep(0.3 * (attempt + 1))
                            continue

                        # Читаем данные с ограничением по размеру
                        max_size = 5 * 1024 * 1024  # 5MB максимум
                        data = bytearray()
                        async for chunk in response.content.iter_chunked(8192):
                            data.extend(chunk)
                            if len(data) > max_size:
                                diagnostics["errors"].append(f"File too large (>5MB)")
                                break

                        diagnostics["size_bytes"] = len(data)

                        # Проверка размера
                        if len(data) < 100:
                            diagnostics["errors"].append("File too small (<100 bytes)")
                            if attempt == Config.REQUEST_RETRIES:
                                return None, diagnostics
                            continue

                        # Проверка сигнатур изображений
                        signatures = [
                            (b'\xff\xd8\xff', 'JPEG'),  # JPEG
                            (b'\x89PNG\r\n\x1a\n', 'PNG'),  # PNG
                            (b'GIF87a', 'GIF'),  # GIF87a
                            (b'GIF89a', 'GIF'),  # GIF89a
                            (b'BM', 'BMP'),  # BMP
                            (b'RIFF', 'WEBP'),  # WEBP (RIFF header)
                        ]
                        valid = False
                        file_type = "Unknown"
                        img_data = bytes(data)
                        for sig, ftype in signatures:
                            if img_data.startswith(sig):
                                valid = True
                                file_type = ftype
                                break

                        if not valid:
                            # Попробуем определить по заголовкам
                            if b'JFIF' in img_data[:100] or b'Exif' in img_data[:100]:
                                valid = True
                                file_type = "JPEG"

                        if not valid:
                            diagnostics["errors"].append(f"Invalid image format ({file_type})")
                            if attempt == Config.REQUEST_RETRIES:
                                return None, diagnostics
                            continue

                        diagnostics["file_type"] = file_type
                        diagnostics["success"] = True
                        # Сохраняем в кэш памяти с контролем размера
                        self._add_to_cache(url, img_data)
                        return img_data, diagnostics

            except asyncio.TimeoutError:
                diagnostics["errors"].append("Timeout")
                if attempt == Config.REQUEST_RETRIES:
                    return None, diagnostics
                await asyncio.sleep(0.5 * (attempt + 1))
            except aiohttp.ClientError as e:
                diagnostics["errors"].append(f"Client error: {str(e)[:50]}")
                if attempt == Config.REQUEST_RETRIES:
                    return None, diagnostics
                await asyncio.sleep(0.3 * (attempt + 1))
            except Exception as e:
                diagnostics["errors"].append(f"Unexpected error: {str(e)[:50]}")
                if attempt == Config.REQUEST_RETRIES:
                    return None, diagnostics
                await asyncio.sleep(0.3 * (attempt + 1))

        return None, diagnostics

    def _add_to_cache(self, url: str, data: bytes):
        """Добавить данные в кэш с контролем памяти"""
        data_size = len(data)
        
        # Не кэшируем слишком большие файлы
        if data_size > self.cache_max_bytes * 0.1:
            return
            
        # Проверяем, поместится ли в текущий кэш
        if self.cache_current_bytes + data_size > self.cache_max_bytes:
            # Освобождаем место пока не хватит
            while (self.cache_current_bytes + data_size > self.cache_max_bytes 
                   and self.cache):
                oldest_url = next(iter(self.cache))
                oldest_data = self.cache[oldest_url]
                self.cache_current_bytes -= len(oldest_data)
                self.cache.pop(oldest_url)
        
        # Добавляем только если после очистки есть место
        if self.cache_current_bytes + data_size <= self.cache_max_bytes:
            self.cache[url] = data
            self.cache_current_bytes += data_size

    def _process_and_embed_image_sync(self, image_data: bytes, url_hash: str) -> Tuple[Optional[str], Optional[str], Optional[Dict]]:
        """Синхронная обработка изображения и создание base64 (оптимизированная)"""
        start_time = time.time()
        try:
            # Оптимизация: сразу пробуем OpenCV
            nparr = np.frombuffer(image_data, np.uint8)
            img_np = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            if img_np is None:
                # Пробуем PIL как запасной вариант
                try:
                    with Image.open(BytesIO(image_data)) as img_pil:
                        # Быстрое преобразование в RGB
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
                    return None, None, None

            # Быстрая проверка валидности
            if img_np.size == 0 or img_np.shape[0] == 0 or img_np.shape[1] == 0:
                return None, None, None

            # Получаем размеры
            height, width = img_np.shape[:2]

            # Оптимизация: пропускаем слишком большие изображения
            if width > 5000 or height > 5000:
                # Масштабируем очень большие изображения
                scale = min(5000 / width, 5000 / height)
                if scale < 1:
                    new_width = int(width * scale)
                    new_height = int(height * scale)
                    img_np = cv2.resize(img_np, (new_width, new_height), cv2.INTER_AREA)
                    height, width = img_np.shape[:2]

            # Создаем миниатюру для HTML
            thumbnail_size = Config.THUMBNAIL_SIZE
            scale = min(thumbnail_size[0] / width, thumbnail_size[1] / height, 1.0)
            new_width = int(width * scale)
            new_height = int(height * scale)

            # Оптимизация: используем быструю интерполяцию для маленьких изображений
            if scale < 0.5:
                interpolation = cv2.INTER_AREA
            else:
                interpolation = cv2.INTER_LINEAR

            img_resized = cv2.resize(img_np, (new_width, new_height), interpolation=interpolation)

            # Быстрое кодирование в base64
            success, buffer = cv2.imencode('.jpg', img_resized, THUMBNAIL_COMPRESSION_PARAMS)
            if not success:
                return None, None, None

            base64_str = base64.b64encode(buffer.tobytes()).decode('utf-8')

            # Сохраняем оригинальное изображение
            timestamp = int(time.time() * 1000)
            filename = f"photo_{url_hash}_{timestamp}.jpg"
            filepath = os.path.join(self.images_dir, filename)

            # Сохраняем с оптимальными параметрами
            cv2.imwrite(filepath, img_np, COMPRESSION_PARAMS)

            # Также сохраняем в кэш для будущего использования (только если не слишком большое)
            if width <= 2000 and height <= 2000:
                cache_filename = f"cache_{url_hash}.jpg"
                cache_path = os.path.join(self.cache_dir, cache_filename)
                cv2.imwrite(cache_path, img_np, COMPRESSION_PARAMS)

            # Получаем размер файла
            file_size_kb = os.path.getsize(filepath) / 1024
            processing_time = time.time() - start_time
            self.processing_times.append(processing_time)

            return filepath, base64_str, {
                "width": width,
                "height": height,
                "file_size_kb": file_size_kb,
                "original_size": len(image_data),
                "processing_time": processing_time,
                "thumbnail_size": (new_width, new_height)
            }

        except Exception as e:
            logger.debug(f"Error processing image {url_hash[:8]}: {e}")
            return None, None, None
        finally:
            # Освобождаем память
            gc.collect()

    async def process_and_embed_image(self, image_data: bytes, url_hash: str) -> Tuple[Optional[str], Optional[str], Optional[Dict]]:
        """Асинхронная обработка изображения и создание base64"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            THREAD_POOL,
            self._process_and_embed_image_sync,
            image_data,
            url_hash
        )

    def _create_thumbnail_from_cache_sync(self, cache_path: str) -> Tuple[Optional[str], Optional[Dict]]:
        """Синхронное создание thumbnail из кэшированного файла (оптимизированное)"""
        try:
            # Быстрое чтение файла
            with open(cache_path, 'rb') as f:
                img_data = f.read()
            # Прямая декодирование через OpenCV
            nparr = np.frombuffer(img_data, np.uint8)
            img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            if img is not None:
                height, width = img.shape[:2]
                # Быстрое создание миниатюры
                thumbnail_size = Config.THUMBNAIL_SIZE
                if width > thumbnail_size[0] or height > thumbnail_size[1]:
                    scale = min(thumbnail_size[0] / width, thumbnail_size[1] / height)
                    new_width = int(width * scale)
                    new_height = int(height * scale)
                    img_resized = cv2.resize(img, (new_width, new_height), cv2.INTER_AREA)
                else:
                    img_resized = img
                success, buffer = cv2.imencode('.jpg', img_resized, THUMBNAIL_COMPRESSION_PARAMS)
                if success:
                    base64_str = base64.b64encode(buffer.tobytes()).decode('utf-8')
                    return base64_str, {
                        "width": width,
                        "height": height,
                        "file_size_kb": len(img_data) / 1024,
                        "from_cache": True
                    }
        except Exception as e:
            logger.debug(f"Error reading cache: {e}")
        return None, None

    async def _create_thumbnail_from_cache(self, cache_path: str) -> Tuple[Optional[str], Optional[Dict]]:
        """Асинхронное создание thumbnail из кэшированного файла"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            THREAD_POOL,
            self._create_thumbnail_from_cache_sync,
            cache_path
        )

    async def process_image(self, url: str, metrics: ProcessingMetrics) -> Tuple[Any, Any, Dict]:
        """Полная обработка изображения с улучшенной обработкой ошибок"""
        if not url or 'http' not in url:
            metrics.network_errors += 1
            return None, None, {"failed_reason": "Invalid URL"}

        start_time = time.time()
        download_start = time.time()
        url_hash, filename, cache_filename = self._generate_image_name(url)

        # Статистика для метрик
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

        # Шаг 1: Проверяем кэш на диске (быстрая проверка)
        cache_path = os.path.join(self.cache_dir, cache_filename)
        if os.path.exists(cache_path):
            try:
                # 🔥 ИСПРАВЛЕНО: вызываем async-метод с await
                base64_str, cache_info = await self._create_thumbnail_from_cache(cache_path)
                if base64_str and cache_info:
                    metrics.cached_images += 1
                    self.total_images_processed += 1
                    # Находим оригинальный файл
                    check_pattern = f"photo_{url_hash}_*.jpg"
                    existing = glob.glob(os.path.join(self.images_dir, check_pattern))
                    filepath = existing[0] if existing else ""
                    processing_time = time.time() - start_time
                    image_metric.download_time_ms = 0
                    image_metric.processing_time_ms = int(processing_time * 1000)
                    image_metric.size_kb = cache_info.get("file_size_kb", 0)
                    image_metric.width = cache_info.get("width", 0)
                    image_metric.height = cache_info.get("height", 0)
                    image_metric.is_cached = True
                    image_metric.success = True
                    self.image_metrics.append(image_metric)
                    return filepath, base64_str, {
                        "width": cache_info.get("width", 0),
                        "height": cache_info.get("height", 0),
                        "file_size_kb": cache_info.get("file_size_kb", 0),
                        "download_time_ms": 0,
                        "is_cached": True,
                        "failed_reason": ""
                    }
            except Exception as e:
                logger.debug(f"Cache read error: {e}")

        # Шаг 2: Проверяем уже загруженные изображения (быстрая проверка)
        check_pattern = f"photo_{url_hash}_*.jpg"
        existing = glob.glob(os.path.join(self.images_dir, check_pattern))
        if existing:
            try:
                async with aiofiles.open(existing[0], 'rb') as f:
                    img_data = await f.read()
                # Обрабатываем для создания thumbnail
                filepath, base64_str, img_info = await self.process_and_embed_image(img_data, url_hash)
                if filepath and base64_str and img_info:
                    metrics.cached_images += 1
                    self.total_images_processed += 1
                    processing_time = time.time() - start_time
                    image_metric.download_time_ms = 0
                    image_metric.processing_time_ms = int(processing_time * 1000)
                    image_metric.size_kb = img_info.get("file_size_kb", 0) if img_info else 0
                    image_metric.width = img_info.get("width", 0) if img_info else 0
                    image_metric.height = img_info.get("height", 0) if img_info else 0
                    image_metric.is_cached = True
                    image_metric.success = True
                    self.image_metrics.append(image_metric)
                    return filepath, base64_str, {
                        "width": img_info.get("width", 0) if img_info else 0,
                        "height": img_info.get("height", 0) if img_info else 0,
                        "file_size_kb": img_info.get("file_size_kb", 0) if img_info else 0,
                        "download_time_ms": 0,
                        "is_cached": True,
                        "failed_reason": ""
                    }
            except Exception as e:
                logger.debug(f"Existing image read error: {e}")

        # Шаг 3: Загрузка нового изображения
        download_attempts = 0
        for attempt in range(Config.REQUEST_RETRIES + 1):
            download_attempts += 1
            try:
                # Проверка памяти перед загрузкой
                if not self._check_memory_safe():
                    await asyncio.sleep(0.5)
                    continue

                image_data, diagnostics = await self.download_image(url)
                download_time = time.time() - download_start
                if image_data:
                    # Проверка памяти перед обработкой
                    if not self._check_memory_safe(additional_mb=len(image_data)/1024/1024):
                        await asyncio.sleep(0.5)
                        continue

                    # Обработка изображения
                    process_start = time.time()
                    filepath, base64_str, img_info = await self.process_and_embed_image(image_data, url_hash)
                    processing_time = time.time() - process_start
                    if filepath and base64_str and img_info:
                        metrics.valid_images += 1
                        self.total_images_processed += 1
                        self.total_download_time += download_time
                        # Записываем метрики
                        image_metric.download_time_ms = int(download_time * 1000)
                        image_metric.processing_time_ms = int(processing_time * 1000)
                        image_metric.size_kb = img_info.get("file_size_kb", 0)
                        image_metric.width = img_info.get("width", 0)
                        image_metric.height = img_info.get("height", 0)
                        image_metric.is_cached = False
                        image_metric.success = True
                        self.image_metrics.append(image_metric)
                        # Очистка памяти
                        gc.collect()
                        return filepath, base64_str, {
                            "width": img_info.get("width", 0),
                            "height": img_info.get("height", 0),
                            "file_size_kb": img_info.get("file_size_kb", 0),
                            "download_time_ms": int(download_time * 1000),
                            "is_cached": False,
                            "failed_reason": "",
                            "attempts": download_attempts
                        }
                    else:
                        # Изображение загружено, но не обработано
                        error_msg = "Failed to process image data"
                        image_metric.error_message = error_msg
                        self.image_metrics.append(image_metric)
                        metrics.failed_images += 1
                        return None, None, {
                            "failed_reason": error_msg,
                            "attempts": download_attempts,
                            "diagnostics": diagnostics
                        }

                else:
                    # Ошибка загрузки
                    error_msg = diagnostics.get("errors", ["Unknown error"])[0] if diagnostics.get("errors") else "Download failed"
                    if diagnostics.get("status_code"):
                        error_msg = f"HTTP {diagnostics['status_code']}: {error_msg}"
                    image_metric.error_message = error_msg
                    self.image_metrics.append(image_metric)
                    if attempt == Config.REQUEST_RETRIES:
                        metrics.failed_images += 1
                        if diagnostics.get("status_code") in [404, 403, 500]:
                            metrics.network_errors += 1
                        return None, None, {
                            "failed_reason": error_msg,
                            "attempts": download_attempts,
                            "diagnostics": diagnostics
                        }
                    # Ждем перед следующей попыткой
                    await asyncio.sleep(0.3 * (attempt + 1))

            except asyncio.TimeoutError:
                error_msg = f"Timeout (attempt {attempt + 1})"
                image_metric.error_message = error_msg
                self.image_metrics.append(image_metric)
                if attempt == Config.REQUEST_RETRIES:
                    metrics.timeout_errors += 1
                    metrics.failed_images += 1
                    return None, None, {
                        "failed_reason": error_msg,
                        "attempts": download_attempts
                    }
                await asyncio.sleep(0.5 * (attempt + 1))
            except Exception as e:
                error_msg = f"Unexpected error: {str(e)[:100]}"
                logger.error(f"Image processing error: {e}")
                image_metric.error_message = error_msg
                self.image_metrics.append(image_metric)
                if attempt == Config.REQUEST_RETRIES:
                    metrics.failed_images += 1
                    return None, None, {
                        "failed_reason": error_msg,
                        "attempts": download_attempts
                    }
                await asyncio.sleep(0.3 * (attempt + 1))

        # Все попытки исчерпаны
        metrics.failed_images += 1
        return None, None, {
            "failed_reason": "All download attempts failed",
            "attempts": download_attempts
        }

    def _check_memory_safe(self, additional_mb: float = 0) -> bool:
        """Проверить, безопасно ли продолжать обработку"""
        try:
            current_usage = psutil.virtual_memory().percent
            available_memory_mb = psutil.virtual_memory().available / (1024**2)
            # Проверяем два условия:
            # 1. Общее использование памяти < 85%
            # 2. Доступно минимум 200 MB памяти с учетом дополнительной
            if current_usage > Config.MAX_MEMORY_PERCENT:
                return False
            if available_memory_mb - additional_mb < 200:
                return False
            return True
        except:
            return True  # Если не можем проверить, продолжаем

    async def _save_image_metrics(self):
        """Сохранение метрик обработки изображений"""
        if not self.image_metrics:
            return
        try:
            metrics_file = os.path.join(self.base_dir, Config.CACHE_FOLDER, "image_metrics.json")
            metrics_data = [metric.to_dict() for metric in self.image_metrics]
            async with aiofiles.open(metrics_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(metrics_data, indent=2, ensure_ascii=False))

            # Также сохраняем сводную статистику
            successful = [m for m in self.image_metrics if m.success]
            failed = [m for m in self.image_metrics if not m.success]
            if self.processing_times:
                avg_processing_time = sum(self.processing_times) / len(self.processing_times)
            else:
                avg_processing_time = 0

            summary = {
                "total_images": len(self.image_metrics),
                "successful": len(successful),
                "failed": len(failed),
                "success_rate": (len(successful) / len(self.image_metrics) * 100) if self.image_metrics else 0,
                "total_download_time_seconds": self.total_download_time,
                "avg_download_time_ms": (sum(m.download_time_ms for m in successful) / len(successful)) if successful else 0,
                "avg_processing_time_ms": avg_processing_time * 1000,
                "cached_images": sum(1 for m in self.image_metrics if m.is_cached),
                "avg_image_size_kb": (sum(m.size_kb for m in successful) / len(successful)) if successful else 0,
                "cache_hits": self.cache_hits,
                "cache_misses": self.cache_misses,
                "cache_hit_rate": (self.cache_hits / (self.cache_hits + self.cache_misses) * 100) if (self.cache_hits + self.cache_misses) > 0 else 0,
                "memory_cache_size": len(self.cache),
                "memory_cache_bytes": self.cache_current_bytes,
            }
            summary_file = os.path.join(self.base_dir, Config.CACHE_FOLDER, "image_summary.json")
            async with aiofiles.open(summary_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(summary, indent=2, ensure_ascii=False))
        except Exception as e:
            logger.error(f"Failed to save image metrics: {e}")

    def get_statistics(self) -> Dict:
        """Получить статистику обработчика изображений"""
        successful = [m for m in self.image_metrics if m.success]
        if self.processing_times:
            avg_processing_time = sum(self.processing_times) / len(self.processing_times)
        else:
            avg_processing_time = 0

        return {
            "total_processed": len(self.image_metrics),
            "successful": len(successful),
            "failed": len(self.image_metrics) - len(successful),
            "success_rate": (len(successful) / len(self.image_metrics) * 100) if self.image_metrics else 0,
            "cached_count": sum(1 for m in self.image_metrics if m.is_cached),
            "total_download_time_seconds": self.total_download_time,
            "avg_download_time_ms": (sum(m.download_time_ms for m in successful) / len(successful)) if successful else 0,
            "avg_processing_time_ms": avg_processing_time * 1000,
            "avg_image_size_kb": (sum(m.size_kb for m in successful) / len(successful)) if successful else 0,
            "memory_cache_hits": self.cache_hits,
            "memory_cache_misses": self.cache_misses,
            "memory_cache_size": len(self.cache),
            "memory_cache_bytes": self.cache_current_bytes,
            "cache_hit_rate": (self.cache_hits / (self.cache_hits + self.cache_misses) * 100) if (self.cache_hits + self.cache_misses) > 0 else 0,
        }


# Оптимизированные вспомогательные функции

async def process_images_batch(processor: 'ImageProcessorWithEmbedding', urls: List[str], metrics: ProcessingMetrics):
    """Пакетная обработка изображений (оптимизированная версия)"""
    if not urls:
        return []

    # Увеличиваем параллелизм для лучшей производительности
    try:
        available_memory_gb = psutil.virtual_memory().available / (1024**3)
        if available_memory_gb < 1:
            max_concurrent = 6
        elif available_memory_gb < 2:
            max_concurrent = 10
        else:
            max_concurrent = min(len(urls), 20)  # Увеличили максимальное количество
    except:
        max_concurrent = min(len(urls), 12)

    semaphore = asyncio.Semaphore(max_concurrent)

    async def process_single(url: str):
        async with semaphore:
            return await processor.process_image(url, metrics)

    # Обработка с таймаутом для избегания зависания
    tasks = [asyncio.create_task(process_single(url)) for url in urls]

    try:
        # Увеличиваем таймаут для пакетной обработки
        timeout_duration = Config.REQUEST_TIMEOUT * 8  # Увеличили таймаут до 8x для лучшей стабильности
        results = await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=timeout_duration
        )
        return results
    except asyncio.TimeoutError:
        logger.warning(f"Batch processing timeout for {len(urls)} images, returning partial results")
        results = []
        completed_count = 0
        for i, task in enumerate(tasks):
            if not task.done():
                try:
                    result = await asyncio.wait_for(task, timeout=5.0)
                    results.append(result)
                    completed_count += 1
                except asyncio.TimeoutError:
                    results.append(None)
                    logger.debug(f"Task {i} did not complete due to timeout")
            else:
                results.append(task.result())
                completed_count += 1
        logger.info(f"Completed {completed_count}/{len(tasks)} tasks after timeout")
        return results
    except Exception as e:
        logger.error(f"Batch processing error: {e}")
        return [None] * len(urls)


def create_thumbnail_from_file(file_path: str, thumbnail_size: tuple = Config.THUMBNAIL_SIZE) -> Optional[str]:
    """Создание base64 thumbnail из файла (синхронная версия)"""
    try:
        # Быстрое чтение и декодирование
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
        success, buffer = cv2.imencode('.jpg', img, THUMBNAIL_COMPRESSION_PARAMS)
        if success:
            return base64.b64encode(buffer).decode('utf-8')
        return None
    except Exception as e:
        logger.debug(f"Error creating thumbnail: {e}")
        return None


def validate_image_file(file_path: str) -> bool:
    """Проверка валидности файла изображения"""
    try:
        with open(file_path, 'rb') as f:
            data = f.read(100)  # Читаем первые 100 байт для проверки сигнатуры
        signatures = [
            b'\xff\xd8\xff',  # JPEG
            b'\x89PNG\r\n\x1a\n',  # PNG
            b'GIF87a', b'GIF89a',  # GIF
            b'BM',  # BMP
            b'RIFF'  # WEBP
        ]
        return any(data.startswith(sig) for sig in signatures)
    except:
        return False


async def cleanup_old_cache(cache_dir: str, max_age_hours: int = 24):
    """Очистка старых файлов кэша"""
    try:
        current_time = time.time()
        files_removed = 0
        bytes_freed = 0
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


# Оптимизированный класс для пакетной обработки

class BatchImageProcessor:
    """Обработчик пакетной обработки изображений"""
    def __init__(self, processor: ImageProcessorWithEmbedding):
        self.processor = processor
        self.batch_size = 50
        self.max_retries = 2

    async def process_batch(self, urls: List[str], metrics: ProcessingMetrics):
        """Обработка пакета URL"""
        results = []
        for i in range(0, len(urls), self.batch_size):
            batch = urls[i:i + self.batch_size]
            batch_results = await process_images_batch(self.processor, batch, metrics)
            results.extend(batch_results)
            # Очистка памяти между пакетами
            if i % (self.batch_size * 5) == 0:
                gc.collect()
        return results


async def process_images_batch_with_progress(processor: 'ImageProcessorWithEmbedding', urls: List[str], metrics: ProcessingMetrics, progress_callback=None):
    """Пакетная обработка изображений с отслеживанием прогресса"""
    if not urls:
        return []

    # Разбиваем на подбатчи для лучшего отслеживания прогресса
    batch_size = min(500, len(urls))  # Используем меньший размер батча для прогресса
    all_results = []
    for i in range(0, len(urls), batch_size):
        batch_urls = urls[i:i + batch_size]
        # Обработка подбатча
        batch_results = await process_images_batch(processor, batch_urls, metrics)
        all_results.extend(batch_results)
        # Обновляем прогресс, если передан callback
        if progress_callback:
            processed = min(i + batch_size, len(urls))
            progress_callback(processed, len(urls))
    return all_results