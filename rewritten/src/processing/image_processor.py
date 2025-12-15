"""
Clean and optimized image processor with embedded HTML support
"""
import os
import asyncio
import time
import hashlib
import base64
import gc
import random
import ssl
import json
from typing import Optional, Tuple, Dict, List, Any
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
from dataclasses import dataclass
from enum import Enum
from io import BytesIO

import aiohttp
import aiofiles
import numpy as np
import cv2
from PIL import Image, ImageFile
import psutil

# Import configuration
from ..core.config import config as app_config
from ..core.data_parser import ParserMetrics

# Setup logging
import logging
logger = logging.getLogger(__name__)

# Allow truncated images
ImageFile.LOAD_TRUNCATED_IMAGES = True


class ImageProcessingResult:
    """Result of image processing"""
    def __init__(self, filepath: str, base64_str: str, image_info: Dict[str, Any]):
        self.filepath = filepath
        self.base64_str = base64_str
        self.image_info = image_info


class ImageCache:
    """Smart image cache with memory control"""
    
    def __init__(self, max_size_mb: int = 200):
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.current_size_bytes = 0
        self.cache = OrderedDict()  # For LRU
        self.hits = 0
        self.misses = 0
        self.evictions = 0
    
    def get(self, key: str) -> Optional[bytes]:
        """Get data from cache"""
        if key not in self.cache:
            self.misses += 1
            return None
        
        # Move to end (most recently used)
        data = self.cache.pop(key)
        self.cache[key] = data
        
        self.hits += 1
        return data
    
    def put(self, key: str, data: bytes) -> bool:
        """Add data to cache"""
        data_size = len(data)
        
        # Don't cache files larger than 10% of max size
        if data_size > self.max_size_bytes * 0.1:
            return False
        
        # Remove old key if exists
        if key in self.cache:
            old_data = self.cache[key]
            self.current_size_bytes -= len(old_data)
            del self.cache[key]
        
        # Evict oldest items if needed
        while (self.current_size_bytes + data_size > self.max_size_bytes 
               and self.cache):
            self._evict_oldest()
        
        # Add if there's space
        if self.current_size_bytes + data_size <= self.max_size_bytes:
            self.cache[key] = data
            self.current_size_bytes += data_size
            return True
        
        return False
    
    def _evict_oldest(self):
        """Remove oldest entry"""
        if not self.cache:
            return
        
        oldest_key = next(iter(self.cache))
        oldest_data = self.cache.pop(oldest_key)
        self.current_size_bytes -= len(oldest_data)
        self.evictions += 1
    
    def clear(self):
        """Clear cache"""
        self.cache.clear()
        self.current_size_bytes = 0
        self.hits = 0
        self.misses = 0
        self.evictions = 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
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
    """Image download diagnostics"""
    
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
        """Add error message"""
        self.errors.append(error[:100])  # Limit length
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
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
    """Exponential backoff retry strategy"""
    
    @staticmethod
    async def execute_with_retry(func, max_attempts: int = 3, 
                                base_delay: float = 0.3, max_delay: float = 5.0):
        """Execute function with retries"""
        last_exception = None
        
        for attempt in range(max_attempts):
            try:
                return await func()
            except Exception as e:
                last_exception = e
                
                if attempt == max_attempts - 1:
                    break
                
                # Exponential backoff with jitter
                delay = min(
                    base_delay * (2 ** attempt) + random.uniform(0, 0.1),
                    max_delay
                )
                await asyncio.sleep(delay)
        
        raise last_exception


@dataclass
class ProcessingConfig:
    """Image processing configuration"""
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
    Optimized image processor with HTML embedding support
    
    Features:
    - Smart caching with memory control
    - Async downloads with connection limits
    - Batch processing with process pools
    - Retry strategy
    - SSL security
    """
    
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.config = ProcessingConfig()
        
        # Adapt to system resources
        self._adapt_to_system_resources()
        
        # Safe SSL context
        self.ssl_context = self._create_ssl_context()
        
        # HTTP session
        self.session: Optional[aiohttp.ClientSession] = None
        self.connector: Optional[aiohttp.TCPConnector] = None
        
        # Caches
        self.memory_cache = ImageCache(self.config.memory_cache_size_mb)
        self.disk_cache_dir = os.path.join(base_dir, app_config.cache_folder)
        self.images_dir = os.path.join(base_dir, app_config.image_folder)
        
        # Process pool for CPU-intensive operations
        max_processes = min(multiprocessing.cpu_count() - 1, 4)  # Limit for Windows
        self.process_pool = ThreadPoolExecutor(max_workers=max_processes)
        
        # Semaphore for download limits
        self.download_semaphore = asyncio.Semaphore(self.config.max_connections)
        
        # Metrics
        self.metrics: List[Dict[str, Any]] = []
        self.total_processed = 0
        self.total_download_time = 0.0
        
        # Create directories
        self._create_directories()
        
        # Compression parameters
        self.compression_params = [
            cv2.IMWRITE_JPEG_QUALITY, self.config.compression_quality,
            cv2.IMWRITE_JPEG_PROGRESSIVE, 1,
            cv2.IMWRITE_JPEG_OPTIMIZE, 1
        ]
        
        logger.info(f"Initialized ImageProcessor with cache {self.config.memory_cache_size_mb}MB")
    
    def _create_ssl_context(self) -> ssl.SSLContext:
        """Create secure SSL context"""
        try:
            ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            ssl_context.check_hostname = True
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            
            # For Windows, load default certificates
            import platform
            if platform.system() == "Windows":
                ssl_context.load_default_certs()
            
            return ssl_context
        except Exception as e:
            logger.warning(f"Could not create SSL context: {e}. Using default")
            return ssl.create_default_context()
    
    def _adapt_to_system_resources(self):
        """Adapt configuration to system resources"""
        try:
            # Adapt based on available memory
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
            
            logger.debug(f"Adapted config: workers={self.config.max_workers}, "
                        f"connections={self.config.max_connections}, "
                        f"cache={self.config.memory_cache_size_mb}MB")
                        
        except Exception as e:
            logger.warning(f"Config adaptation error: {e}")
    
    def _create_directories(self):
        """Create required directories"""
        os.makedirs(self.images_dir, exist_ok=True)
        os.makedirs(self.disk_cache_dir, exist_ok=True)
        os.makedirs(os.path.join(self.base_dir, app_config.temp_folder), exist_ok=True)
    
    async def __aenter__(self):
        """Context manager entry"""
        await self._initialize_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        await self._close_resources()
        
        # Save metrics
        await self._save_metrics()
    
    async def _initialize_session(self):
        """Initialize HTTP session with secure SSL"""
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
                ssl=self.ssl_context
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
            
            logger.debug("HTTP session initialized with secure SSL")
            
        except Exception as e:
            logger.error(f"Session initialization error: {e}")
            raise
    
    async def _close_resources(self):
        """Close all resources"""
        try:
            # Close HTTP session
            if self.session:
                await self.session.close()
                self.session = None
                logger.debug("HTTP session closed")
            
            # Shutdown process pool
            if hasattr(self, 'process_pool'):
                self.process_pool.shutdown(wait=True)
                logger.debug("Process pool shutdown")
            
            # Clear memory cache
            self.memory_cache.clear()
            
            # Cleanup
            gc.collect()
            
            # Log stats
            stats = self.memory_cache.get_stats()
            logger.info(f"ImageProcessor closed. Cache hits: {self.memory_cache.hits}, "
                       f"misses: {self.memory_cache.misses}, "
                       f"hit rate: {stats['hit_rate_percent']:.1f}%")
            
        except Exception as e:
            logger.error(f"Error closing resources: {e}")
    
    async def _save_metrics(self):
        """Save image processing metrics"""
        if not self.metrics:
            return
        
        try:
            metrics_file = os.path.join(self.disk_cache_dir, "image_metrics.json")
            async with aiofiles.open(metrics_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(self.metrics, indent=2, ensure_ascii=False))
            
            # Save summary
            await self._save_summary_statistics()
            
        except Exception as e:
            logger.error(f"Metrics save error: {e}")
    
    async def _save_summary_statistics(self):
        """Save summary statistics"""
        try:
            successful = [m for m in self.metrics if m.get('success', False)]
            failed = [m for m in self.metrics if not m.get('success', False)]
            
            summary = {
                "total_images": len(self.metrics),
                "successful": len(successful),
                "failed": len(failed),
                "success_rate": (len(successful) / len(self.metrics) * 100) if self.metrics else 0,
                "total_download_time_seconds": self.total_download_time,
                "avg_download_time_ms": (sum(m.get('download_time_ms', 0) for m in successful) / len(successful)) if successful else 0,
                "cached_images": sum(1 for m in self.metrics if m.get('is_cached', False)),
                "avg_image_size_kb": (sum(m.get('size_kb', 0) for m in successful) / len(successful)) if successful else 0,
                "memory_cache_stats": self.memory_cache.get_stats(),
                "timestamp": time.time()
            }
            
            summary_file = os.path.join(self.disk_cache_dir, "image_summary.json")
            async with aiofiles.open(summary_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(summary, indent=2, ensure_ascii=False))
        
        except Exception as e:
            logger.error(f"Summary stats save error: {e}")
    
    def _generate_image_name(self, url: str) -> Tuple[str, str, str]:
        """Generate unique filenames"""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        timestamp = int(time.time() * 1000)
        filename = f"photo_{url_hash}_{timestamp}.jpg"
        cache_filename = f"cache_{url_hash}.jpg"
        return url_hash, filename, cache_filename
    
    @staticmethod
    def _validate_image_data(data: bytes) -> Tuple[bool, str]:
        """Validate image data"""
        if len(data) < 100:
            return False, "File too small (<100 bytes)"
        
        # Check image signatures
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
        
        # Try to detect by headers
        if b'JFIF' in data[:100] or b'Exif' in data[:100]:
            return True, "JPEG"
        
        return False, "Invalid image format"
    
    async def _download_image_with_retry(self, url: str) -> Tuple[Optional[bytes], DownloadDiagnostics]:
        """Download image with retries and secure SSL"""
        diagnostics = DownloadDiagnostics(url)
        
        async def attempt_download():
            diagnostics.attempts += 1
            start_time = time.time()
            
            try:
                async with self.download_semaphore:
                    async with self.session.get(
                        url,
                        allow_redirects=True,
                        ssl=self.ssl_context,
                        compress=True
                    ) as response:
                        
                        diagnostics.status_code = response.status
                        
                        if response.status != 200:
                            diagnostics.add_error(f"HTTP {response.status}")
                            return None
                        
                        # Read with size limit
                        max_size = self.config.max_image_size_mb * 1024 * 1024
                        data = bytearray()
                        
                        async for chunk in response.content.iter_chunked(8192):
                            data.extend(chunk)
                            if len(data) > max_size:
                                diagnostics.add_error(f"File too large (> {self.config.max_image_size_mb}MB)")
                                return None
                        
                        diagnostics.size_bytes = len(data)
                        diagnostics.response_time_ms = (time.time() - start_time) * 1000
                        
                        # Validate image
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
        
        # Execute with retries
        try:
            result = await RetryStrategy.execute_with_retry(
                attempt_download,
                max_attempts=self.config.max_retries,
                base_delay=0.3,
                max_delay=5.0
            )
            return result, diagnostics
        
        except Exception as e:
            diagnostics.add_error(f"All download attempts failed: {str(e)[:50]}")
            return None, diagnostics
    
    def _process_image_sync(self, image_data: bytes, url_hash: str) -> Optional[ImageProcessingResult]:
        """Synchronous image processing (run in separate thread)"""
        start_time = time.time()
        
        try:
            # Decode with OpenCV
            nparr = np.frombuffer(image_data, np.uint8)
            img_np = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            
            if img_np is None:
                # Fallback to PIL
                try:
                    with Image.open(BytesIO(image_data)) as img_pil:
                        # Convert to RGB
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
            
            # Validate
            if img_np.size == 0 or img_np.shape[0] == 0 or img_np.shape[1] == 0:
                return None
            
            # Get dimensions
            height, width = img_np.shape[:2]
            
            # Scale very large images
            if width > 5000 or height > 5000:
                scale = min(5000 / width, 5000 / height)
                new_width = int(width * scale)
                new_height = int(height * scale)
                img_np = cv2.resize(img_np, (new_width, new_height), cv2.INTER_AREA)
                height, width = img_np.shape[:2]
            
            # Create thumbnail
            thumbnail_size = self.config.thumbnail_size
            scale = min(thumbnail_size[0] / width, thumbnail_size[1] / height, 1.0)
            new_width = int(width * scale)
            new_height = int(height * scale)
            
            # Choose interpolation
            interpolation = cv2.INTER_AREA if scale < 0.5 else cv2.INTER_LINEAR
            img_resized = cv2.resize(img_np, (new_width, new_height), interpolation=interpolation)
            
            # Encode to base64
            success, buffer = cv2.imencode('.jpg', img_resized, [
                cv2.IMWRITE_JPEG_QUALITY, 85,
                cv2.IMWRITE_JPEG_OPTIMIZE, 1
            ])
            
            if not success:
                return None
            
            base64_str = base64.b64encode(buffer.tobytes()).decode('utf-8')
            
            # Save original image
            timestamp = int(time.time() * 1000)
            filename = f"photo_{url_hash}_{timestamp}.jpg"
            filepath = os.path.join(self.images_dir, filename)
            
            cv2.imwrite(filepath, img_np, self.compression_params)
            
            # Save to disk cache (for smaller images)
            if width <= 2000 and height <= 2000:
                cache_filename = f"cache_{url_hash}.jpg"
                cache_path = os.path.join(self.disk_cache_dir, cache_filename)
                cv2.imwrite(cache_path, img_np, self.compression_params)
            
            # Get file size
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
    
    async def process_image(self, url: str, metrics: Optional[ParserMetrics] = None) -> ImageProcessingResult:
        """
        Full image processing
        
        Returns:
            ImageProcessingResult or error info
        """
        if not url or 'http' not in url.lower():
            if metrics:
                metrics.total_errors += 1
            return ImageProcessingResult("", "", {"failed_reason": "Invalid URL"})
        
        start_time = time.time()
        url_hash, filename, cache_filename = self._generate_image_name(url)
        
        # Check disk cache
        cache_path = os.path.join(self.disk_cache_dir, cache_filename)
        if os.path.exists(cache_path):
            try:
                result = await self._load_from_cache(cache_path, url_hash)
                if result:
                    if metrics:
                        metrics.cache_hits += 1
                    return result
            except Exception as e:
                logger.debug(f"Cache read error: {e}")
        
        # Check memory cache
        cached_data = self.memory_cache.get(url)
        if cached_data:
            try:
                result = await self._process_cached_data(cached_data, url_hash)
                if result:
                    if metrics:
                        metrics.cache_hits += 1
                    return result
            except Exception as e:
                logger.debug(f"Memory cache processing error: {e}")
        
        # Download new image
        download_start = time.time()
        image_data, diagnostics = await self._download_image_with_retry(url)
        
        if image_data:
            # Save to memory cache
            self.memory_cache.put(url, image_data)
            
            # Process image
            process_start = time.time()
            result = await self._process_image_data(image_data, url_hash)
            processing_time = time.time() - process_start
            
            if result and result.filepath and result.base64_str:
                if metrics:
                    metrics.total_parsed += 1
                download_time = (time.time() - download_start) * 1000
                self.total_download_time += time.time() - download_start
                
                # Add to metrics
                image_metric = {
                    'url': url,
                    'hash': url_hash,
                    'download_time_ms': download_time,
                    'processing_time_ms': processing_time * 1000,
                    'size_kb': result.image_info.get('file_size_kb', 0),
                    'width': result.image_info.get('width', 0),
                    'height': result.image_info.get('height', 0),
                    'is_cached': False,
                    'success': True
                }
                self.metrics.append(image_metric)
                
                gc.collect()
                
                return result
            else:
                # Image downloaded but not processed
                error_msg = "Failed to process image data"
                image_metric = {
                    'url': url,
                    'hash': url_hash,
                    'download_time_ms': 0,
                    'processing_time_ms': 0,
                    'size_kb': 0,
                    'width': 0,
                    'height': 0,
                    'is_cached': False,
                    'success': False,
                    'error_message': error_msg
                }
                self.metrics.append(image_metric)
                
                if metrics:
                    metrics.total_errors += 1
                
                return ImageProcessingResult("", "", {
                    "failed_reason": error_msg,
                    "attempts": diagnostics.attempts,
                    "diagnostics": diagnostics.to_dict()
                })
        else:
            # Download error
            error_msg = diagnostics.errors[0] if diagnostics.errors else "Download failed"
            if diagnostics.status_code:
                error_msg = f"HTTP {diagnostics.status_code}: {error_msg}"
            
            image_metric = {
                'url': url,
                'hash': url_hash,
                'download_time_ms': 0,
                'processing_time_ms': 0,
                'size_kb': 0,
                'width': 0,
                'height': 0,
                'is_cached': False,
                'success': False,
                'error_message': error_msg
            }
            self.metrics.append(image_metric)
            
            if metrics:
                metrics.total_errors += 1
            
            if diagnostics.status_code in [404, 403, 500]:
                if metrics:
                    metrics.total_errors += 1
            
            return ImageProcessingResult("", "", {
                "failed_reason": error_msg,
                "attempts": diagnostics.attempts,
                "diagnostics": diagnostics.to_dict()
            })
    
    async def _load_from_cache(self, cache_path: str, url_hash: str) -> Optional[ImageProcessingResult]:
        """Load image from disk cache"""
        loop = asyncio.get_event_loop()
        
        def read_and_process():
            try:
                with open(cache_path, 'rb') as f:
                    img_data = f.read()
                
                # Process to create thumbnail
                nparr = np.frombuffer(img_data, np.uint8)
                img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                
                if img is not None:
                    height, width = img.shape[:2]
                    
                    # Create thumbnail
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
                        
                        # Find original file
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
        """Process data from memory cache"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.process_pool,
            self._process_image_sync,
            image_data,
            url_hash
        )
    
    async def _process_image_data(self, image_data: bytes, url_hash: str) -> Optional[ImageProcessingResult]:
        """Async image data processing"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.process_pool,
            self._process_image_sync,
            image_data,
            url_hash
        )
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get image processor statistics"""
        successful = [m for m in self.metrics if m.get('success', False)]
        
        return {
            "total_processed": len(self.metrics),
            "successful": len(successful),
            "failed": len(self.metrics) - len(successful),
            "success_rate": (len(successful) / len(self.metrics) * 100) if self.metrics else 0,
            "cached_count": sum(1 for m in self.metrics if m.get('is_cached', False)),
            "total_download_time_seconds": self.total_download_time,
            "avg_download_time_ms": (sum(m.get('download_time_ms', 0) for m in successful) / len(successful)) if successful else 0,
            "avg_image_size_kb": (sum(m.get('size_kb', 0) for m in successful) / len(successful)) if successful else 0,
            "memory_cache_stats": self.memory_cache.get_stats()
        }


async def process_images_batch(processor: ImageProcessorWithEmbedding,
                             urls: List[str],
                             metrics: Optional[ParserMetrics] = None) -> List[ImageProcessingResult]:
    """
    Batch image processing with concurrency limits
    
    Args:
        processor: Image processor instance
        urls: List of URLs to process
        metrics: Optional metrics for tracking
    
    Returns:
        List of processing results
    """
    if not urls:
        return []
    
    # Adapt concurrency based on available memory
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
    
    # Create tasks with limits
    tasks = []
    for url in urls:
        task = asyncio.create_task(process_single(url))
        tasks.append(task)
    
    try:
        # Process with timeout
        timeout_duration = processor.config.timeout_seconds * 2
        
        results = await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=timeout_duration
        )
        
        # Process results
        processed_results = []
        for result in results:
            if isinstance(result, Exception):
                processed_results.append(ImageProcessingResult("", "", {"failed_reason": str(result)}))
            else:
                processed_results.append(result)
        
        return processed_results
    
    except asyncio.TimeoutError:
        logger.warning(f"Batch processing timeout for {len(urls)} images")
        
        # Return partial results
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
    Create base64 thumbnail from file (synchronous)
    
    Args:
        file_path: Path to image file
        thumbnail_size: Thumbnail size
    
    Returns:
        Base64 string or None on error
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
    Clean up old cache files
    
    Args:
        cache_dir: Cache directory
        max_age_hours: Max age in hours
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
        logger.error(f"Cache cleanup error: {e}")