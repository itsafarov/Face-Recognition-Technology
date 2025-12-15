"""
Clean and improved JSON data parser with optimized performance and proper error handling
"""
import json
import re
import hashlib
import time
import logging
from typing import Optional, Dict, Tuple, List, Any, Callable, Union
from dataclasses import dataclass, field
from collections import defaultdict, OrderedDict
from contextlib import contextmanager
import sys
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

# Try to import ujson for better performance, fallback to standard json
try:
    import ujson
    JSON = ujson
    JSON_DECODE_ERROR = ujson.JSONDecodeError
except ImportError:
    JSON = json
    JSON_DECODE_ERROR = json.JSONDecodeError

logger = logging.getLogger(__name__)


@dataclass
class ParserConfig:
    """Parser configuration with validation"""
    max_cache_size: int = 10000
    cache_ttl_seconds: int = 3600
    enable_cache: bool = True
    batch_size: int = 1000
    max_retries: int = 2
    validation_enabled: bool = True
    strict_mode: bool = False


@dataclass
class ParserMetrics:
    """Parser metrics with thread-safe operations"""
    total_parsed: int = 0
    total_errors: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    parse_time_total: float = 0.0
    last_reset_time: float = field(default_factory=time.time)
    
    @property
    def cache_hit_rate(self) -> float:
        total_accesses = self.cache_hits + self.cache_misses
        return (self.cache_hits / total_accesses * 100) if total_accesses > 0 else 0.0
    
    @property
    def avg_parse_time_ms(self) -> float:
        return (self.parse_time_total / self.total_parsed * 1000) if self.total_parsed > 0 else 0.0
    
    @property
    def error_rate(self) -> float:
        total_processed = self.total_parsed + self.total_errors
        return (self.total_errors / total_processed * 100) if total_processed > 0 else 0.0
    
    def reset(self):
        """Reset metrics"""
        self.total_parsed = 0
        self.total_errors = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.parse_time_total = 0.0
        self.last_reset_time = time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'total_parsed': self.total_parsed,
            'total_errors': self.total_errors,
            'cache_hits': self.cache_hits,
            'cache_misses': self.cache_misses,
            'cache_hit_rate': self.cache_hit_rate,
            'avg_parse_time_ms': self.avg_parse_time_ms,
            'error_rate': self.error_rate,
            'uptime_seconds': time.time() - self.last_reset_time
        }


class CacheManager:
    """Thread-safe cache manager with memory and TTL controls"""
    
    def __init__(self, max_size: int = 10000, ttl_seconds: int = 3600):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._cache = OrderedDict()
        self._timestamps = {}
        self._size_bytes = 0
        self._hits = 0
        self._misses = 0
        self._lock = Lock()
        
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache with TTL check"""
        with self._lock:
            if key not in self._cache:
                self._misses += 1
                return None
            
            # Check TTL
            timestamp = self._timestamps.get(key)
            if timestamp and (time.time() - timestamp) > self.ttl_seconds:
                self._remove(key)
                self._misses += 1
                return None
            
            # Move to end (recently used)
            value = self._cache.pop(key)
            self._cache[key] = value
            
            self._hits += 1
            return value
    
    def set(self, key: str, value: Any) -> None:
        """Set value in cache with size management"""
        with self._lock:
            # If key already exists, remove old value
            if key in self._cache:
                self._remove(key)
            
            # Clear space if needed
            while len(self._cache) >= self.max_size:
                self._remove_oldest()
            
            # Add new value
            self._cache[key] = value
            self._timestamps[key] = time.time()
            self._size_bytes += self._estimate_size(key, value)
    
    def _remove(self, key: str) -> None:
        """Remove key from cache"""
        if key in self._cache:
            value = self._cache.pop(key)
            self._timestamps.pop(key, None)
            self._size_bytes -= self._estimate_size(key, value)

    def _remove_oldest(self) -> None:
        """Remove oldest element"""
        if self._cache:
            key, value = self._cache.popitem(last=False)
            self._timestamps.pop(key, None)
            self._size_bytes -= self._estimate_size(key, value)

    def _estimate_size(self, key: str, value: Any) -> int:
        """Estimate size in bytes"""
        size = len(key.encode('utf-8'))
        
        if isinstance(value, dict):
            for k, v in value.items():
                size += len(str(k).encode('utf-8'))
                size += len(str(v).encode('utf-8'))
        elif isinstance(value, (list, tuple)):
            for item in value:
                size += len(str(item).encode('utf-8'))
        else:
            size += len(str(value).encode('utf-8'))
        
        return size

    def clear(self) -> None:
        """Clear cache"""
        with self._lock:
            self._cache.clear()
            self._timestamps.clear()
            self._size_bytes = 0
            self._hits = 0
            self._misses = 0

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        with self._lock:
            total_accesses = self._hits + self._misses
            hit_rate = (self._hits / total_accesses * 100) if total_accesses > 0 else 0.0
            return {
                'size': len(self._cache),
                'max_size': self.max_size,
                'size_bytes': self._size_bytes,
                'hits': self._hits,
                'misses': self._misses,
                'hit_rate': f"{hit_rate:.1f}%",
                'ttl_seconds': self.ttl_seconds
            }


class FieldExtractor:
    """Thread-safe field extraction with compiled regex patterns"""
    
    def __init__(self):
        # Pre-compiled regex patterns for fast field extraction
        self._field_patterns = {
            'timestamp': re.compile(r'"timestamp"\s*:\s*(?:{"\$date"\s*:\s*"([^"]+)"}|"([^\"]+)")'),
            'device_id': re.compile(r'"device_id"\s*:\s*"([^\"]+)"'),
            'user_name': re.compile(r'"user_name"\s*:\s*"([^\"]+)"'),
            'eva_sex': re.compile(r'"eva_sex"\s*:\s*(?:"([^\"]+)"|(\d+|null|true|false))'),
            'sex': re.compile(r'"sex"\s*:\s*(?:"([^\"]+)"|(\d+|null|true|false))'),
            'comp_score': re.compile(r'"comp_score"\s*:\s*(?:"([^\"]+)"|([\d\.]+|null|true|false))'),
            'eva_age': re.compile(r'"eva_age"\s*:\s*(?:"([^\"]+)"|([\d\.]+|null|true|false))'),
            'image': re.compile(r'"image"\s*:\s*"([^\"]+)"'),
            'face_id': re.compile(r'"face_id"\s*:\s*(?:"([^\"]+)"|([\d\.]+|null|true|false))'),
            'company_id': re.compile(r'"company_id"\s*:\s*(?:"([^\"]+)"|([\d\.]+|null|true|false))'),
            'event_type': re.compile(r'"event_type"\s*:\s*(?:"([^\"]+)"|(\d+|null|true|false))'),
            'user_list': re.compile(r'"user_list"\s*:\s*(?:"([^\"]+)"|(\d+|null|true|false))'),
            'ip_address': re.compile(r'"(?:IP|device_ip)"\s*:\s*"([^\"]+)"'),
        }
        self._lock = Lock()

    def extract_fields_fast(self, line: str) -> Optional[Dict[str, Any]]:
        """
        Fast field extraction without full JSON parsing.
        Returns only basic fields for caching and validation.
        """
        if not line or len(line) < 10 or not line.strip().startswith('{'):
            return None

        result = {}

        try:
            for field_name, pattern in self._field_patterns.items():
                match = pattern.search(line)
                if match:
                    # Take first non-null group
                    groups = match.groups()
                    value = next((g for g in groups if g is not None), None)
                    if value:
                        result[field_name] = value

            return result if result else None

        except Exception:
            return None

    def is_valid_json_line(self, line: str) -> bool:
        """Fast JSON line validation"""
        line = line.strip()

        # Quick check by first and last characters
        if len(line) < 2:
            return False

        if not (line.startswith('{') and line.endswith('}')):
            return False

        # Check for key fields
        if '"timestamp"' not in line and '"device_id"' not in line:
            return False

        return True


class ValueTransformer:
    """Thread-safe value transformation with instance-based caching"""
    
    def __init__(self, max_cache_size: int = 10000):
        self._max_cache_size = max_cache_size
        self._timestamp_cache = {}
        self._gender_cache = {}
        self._score_cache = {}
        self._age_cache = {}
        self._lock = Lock()

    def transform_timestamp(self, value: Optional[str]) -> str:
        """Transform timestamp with caching"""
        if value is None:
            return "Н/Д"
        
        with self._lock:
            if value in self._timestamp_cache:
                return self._timestamp_cache[value]
        
        if not value or value.lower() in ('null', 'none', 'nan', ''):
            result = "Н/Д"
        else:
            try:
                # Handle MongoDB format
                if '$date' in value:
                    try:
                        import json as json_module
                        if value.strip().startswith('{'):
                            data = json_module.loads(value)
                            value = data.get('$date', '')
                        else:
                            import re
                            match = re.search(r'\$date[\"\']?\s*:\s*[\"\']?([^\"\s}]+)', value)
                            if match:
                                value = match.group(1)
                    except:
                        pass

                # Simplified ISO format handling
                if 'T' in value:
                    if value.endswith('Z'):
                        value = value[:-1]
                    parts = value.split('T')
                    if len(parts) == 2:
                        date_part = parts[0]
                        time_part = parts[1].split('.')[0]  # Remove milliseconds
                        result = f"{date_part} {time_part}"
                    else:
                        result = value
                else:
                    result = value

            except Exception:
                result = "Н/Д"
        
        with self._lock:
            if len(self._timestamp_cache) >= self._max_cache_size:
                self._timestamp_cache.clear()
            self._timestamp_cache[value] = result
        
        return result

    def transform_gender(self, eva_sex: Optional[str], sex: Optional[str]) -> str:
        """Transform gender with caching"""
        key = (str(eva_sex or ''), str(sex or ''))
        
        with self._lock:
            if key in self._gender_cache:
                return self._gender_cache[key]
        
        # Convert to lowercase and strip
        eva_sex_str = str(eva_sex).lower().strip() if eva_sex else ''
        sex_str = str(sex).lower().strip() if sex else ''

        # Mapping dictionary
        gender_map = {
            # eva_sex values
            'female': "Женский",
            'f': "Женский",
            'жен': "Женский",
            '0': "Женский",
            'male': "Мужской",
            'm': "Мужской",
            'муж': "Мужской",
            '1': "Мужской",
            
            # sex values
            '0': "Женский",
            '1': "Мужской",
            
            # Undefined values
            'null': "Н/Д",
            'none': "Н/Д",
            'nan': "Н/Д",
            '': "Н/Д",
        }

        # First check eva_sex
        if eva_sex_str and eva_sex_str in gender_map:
            result = gender_map[eva_sex_str]
        # Then check sex
        elif sex_str and sex_str in gender_map:
            result = gender_map[sex_str]
        else:
            result = "Н/Д"
        
        with self._lock:
            if len(self._gender_cache) >= self._max_cache_size:
                self._gender_cache.clear()
            self._gender_cache[key] = result
        
        return result

    def transform_score(self, value: Optional[str]) -> str:
        """Transform score with caching"""
        if value is None:
            return "Н/Д"
        
        with self._lock:
            if value in self._score_cache:
                return self._score_cache[value]
        
        if not value or value.lower() in ('null', 'none', 'nan', ''):
            result = "Н/Д"
        else:
            try:
                clean_value = ''.join(c for c in str(value) if c.isdigit() or c == '.')
                if not clean_value:
                    result = "Н/Д"
                else:
                    score = float(clean_value)
                    result = f"{score:.1f}%"
            except Exception:
                result = "Н/Д"
        
        with self._lock:
            if len(self._score_cache) >= self._max_cache_size:
                self._score_cache.clear()
            self._score_cache[value] = result
        
        return result

    def transform_age(self, value: Optional[str]) -> str:
        """Transform age with caching"""
        if value is None:
            return "Н/Д"
        
        with self._lock:
            if value in self._age_cache:
                return self._age_cache[value]
        
        if not value or value.lower() in ('null', 'none', 'nan', ''):
            result = "Н/Д"
        else:
            try:
                age = int(float(str(value)))
                result = str(age)
            except Exception:
                result = "Н/Д"
        
        with self._lock:
            if len(self._age_cache) >= self._max_cache_size:
                self._age_cache.clear()
            self._age_cache[value] = result
        
        return result

    def safe_str(self, value: Any, default: str = '') -> str:
        """Safe string conversion"""
        if value is None:
            return default

        try:
            result = str(value).strip()
            return result if result else default
        except Exception:
            return default


class DataParser:
    """
    Optimized JSON data parser with caching and error handling
    
    Features:
    - Cache with memory control
    - Fast field extraction without full parsing
    - Batch processing
    - Performance monitoring
    - Support for different data formats
    """
    
    def __init__(self, config: Optional[ParserConfig] = None):
        self.config = config or ParserConfig()
        self.metrics = ParserMetrics()
        
        # Initialize caches
        self.cache_manager = CacheManager(
            max_size=self.config.max_cache_size,
            ttl_seconds=self.config.cache_ttl_seconds
        ) if self.config.enable_cache else None
        
        # Statistics
        self._batch_stats = defaultdict(int)
        self._error_stats = defaultdict(int)
        
        # Hash function for strings
        self._hash_func = hashlib.md5
        
        # Field extractor and value transformer
        self._field_extractor = FieldExtractor()
        self._value_transformer = ValueTransformer(max_cache_size=self.config.max_cache_size // 4)
        
        logger.info(f"DataParser initialized: cache={self.config.enable_cache}, "
                   f"batch_size={self.config.batch_size}")

    def _generate_line_hash(self, line: str) -> str:
        """Generate line hash"""
        return self._hash_func(line.encode('utf-8')).hexdigest()[:16]

    def parse_record(self, line: str, metrics: Optional[Any] = None) -> Optional[Dict[str, Any]]:
        """
        Parse single record with caching and error handling
        
        Args:
            line: JSON string to parse
            metrics: Optional metrics for updates
            
        Returns:
            Dict with parsed data or None on error
        """
        start_time = time.time()

        # Quick validation
        if not self._field_extractor.is_valid_json_line(line):
            self.metrics.total_errors += 1
            self._error_stats['invalid_format'] += 1
            if metrics and hasattr(metrics, 'json_errors'):
                metrics.json_errors += 1
            return None

        # Check cache
        line_hash = None
        cached_result = None

        if self.config.enable_cache and self.cache_manager:
            line_hash = self._generate_line_hash(line)
            cached_result = self.cache_manager.get(line_hash)

            if cached_result:
                self.metrics.cache_hits += 1
                self.metrics.total_parsed += 1
                self.metrics.parse_time_total += time.time() - start_time

                if metrics and hasattr(metrics, 'total_records'):
                    metrics.total_records += 1

                return cached_result

        self.metrics.cache_misses += 1

        try:
            # Parse JSON
            data = JSON.loads(line.strip())

            # Extract fields with safe defaults
            result = self._extract_fields(data)

            # Validate result
            if self.config.validation_enabled and not self._validate_record(result):
                self.metrics.total_errors += 1
                self._error_stats['validation_failed'] += 1
                if metrics and hasattr(metrics, 'json_errors'):
                    metrics.json_errors += 1
                return None

            # Save to cache
            if self.config.enable_cache and self.cache_manager and line_hash:
                self.cache_manager.set(line_hash, result)

            # Update metrics
            self.metrics.total_parsed += 1
            parse_time = time.time() - start_time
            self.metrics.parse_time_total += parse_time

            if metrics and hasattr(metrics, 'total_records'):
                metrics.total_records += 1

            return result

        except JSON_DECODE_ERROR as e:
            self.metrics.total_errors += 1
            self._error_stats['json_decode'] += 1

            if metrics and hasattr(metrics, 'json_errors'):
                metrics.json_errors += 1

            logger.debug(f"JSON decode error: {e} for line: {line[:100]}...")
            return None

        except Exception as e:
            self.metrics.total_errors += 1
            self._error_stats['unexpected'] += 1

            if metrics and hasattr(metrics, 'json_errors'):
                metrics.json_errors += 1

            logger.error(f"Unexpected parsing error: {e}")

            # In strict mode, re-raise exception
            if self.config.strict_mode and not isinstance(e, (MemoryError, KeyboardInterrupt)):
                raise

            return None

    def _extract_fields(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and transform fields from data"""
        # Extract timestamp
        timestamp_raw = ""
        timestamp_obj = data.get('timestamp', {})
        if isinstance(timestamp_obj, dict):
            timestamp_raw = timestamp_obj.get('$date', '')
        elif isinstance(timestamp_obj, str):
            timestamp_raw = timestamp_obj

        # Extract other fields
        eva_sex = data.get('eva_sex', '')
        sex = data.get('sex', '')
        comp_score = data.get('comp_score', '')
        eva_age = data.get('eva_age', '')

        # Extract IP address (may be in different fields)
        ip_address = data.get('IP', data.get('device_ip', ''))

        # Extract MongoDB _id
        mongo_id = ""
        mongo_id_obj = data.get('_id', {})
        if isinstance(mongo_id_obj, dict):
            mongo_id = mongo_id_obj.get('$oid', '')
        elif mongo_id_obj:
            mongo_id = str(mongo_id_obj)

        # Apply transformations
        timestamp = self._value_transformer.transform_timestamp(timestamp_raw)
        gender = self._value_transformer.transform_gender(eva_sex, sex)
        score = self._value_transformer.transform_score(comp_score)
        age = self._value_transformer.transform_age(eva_age)

        # Create result
        return {
            'timestamp': timestamp,
            'device_id': self._value_transformer.safe_str(data.get('device_id', ''), "Н/Д"),
            'user_name': self._value_transformer.safe_str(data.get('user_name', ''), "Н/Д"),
            'gender': gender,
            'age': age,
            'score': score,
            'face_id': self._value_transformer.safe_str(data.get('face_id', ''), "Н/Д"),
            'company_id': self._value_transformer.safe_str(data.get('company_id', ''), "Н/Д"),
            'image_url': self._value_transformer.safe_str(data.get('image', ''), ""),
            'event_type': self._value_transformer.safe_str(data.get('event_type', ''), ""),
            'user_list': self._value_transformer.safe_str(data.get('user_list', ''), ""),
            'ip_address': self._value_transformer.safe_str(ip_address, "Н/Д"),

            # Additional fields
            'user_id': self._value_transformer.safe_str(data.get('user_id', ''), ""),
            'frpic_name': self._value_transformer.safe_str(data.get('frpic_name', ''), ""),
            'request_type': self._value_transformer.safe_str(data.get('request_type', ''), ""),
            'group': self._value_transformer.safe_str(data.get('group', ''), ""),
            'mongo_id': mongo_id,
            'company_type': self._value_transformer.safe_str(data.get('company_type', ''), "")
        }

    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate parsed record"""
        # Check required fields
        required_fields = ['timestamp', 'device_id', 'user_name']

        for field in required_fields:
            if not record.get(field) or record[field] == 'Н/Д':
                return False

        # Additional checks
        if 'image_url' in record and record['image_url']:
            if not record['image_url'].startswith(('http://', 'https://')):
                return False

        return True

    def parse_batch(self, lines: List[str], metrics: Optional[Any] = None) -> List[Dict[str, Any]]:
        """
        Batch parsing of records
        
        Args:
            lines: List of JSON strings to parse
            metrics: Optional metrics for updates
            
        Returns:
            List of parsed records
        """
        start_time = time.time()
        batch_size = len(lines)
        results = []

        # Split into sub-batches for better memory control
        batch_size_actual = min(batch_size, self.config.batch_size)

        for i in range(0, batch_size, batch_size_actual):
            sub_batch = lines[i:i + batch_size_actual]

            # Process sub-batch
            for line in sub_batch:
                result = self.parse_record(line, metrics)
                if result:
                    results.append(result)

            # Periodic cache cleanup
            if i % (batch_size_actual * 10) == 0 and self.cache_manager:
                # Remove expired entries
                self._clean_expired_cache()

        # Update statistics
        parse_time = time.time() - start_time
        self._batch_stats['total_batches'] += 1
        self._batch_stats['total_records_in_batches'] += batch_size
        self._batch_stats['total_time_in_batches'] += parse_time

        logger.debug(f"Batch of {batch_size} records processed in {parse_time:.3f} sec, "
                    f"successful: {len(results)}")

        return results

    def _clean_expired_cache(self):
        """Clean expired cache entries"""
        if not self.cache_manager:
            return

        current_time = time.time()
        expired_keys = []

        # Find expired keys
        for key, timestamp in self.cache_manager._timestamps.items():
            if (current_time - timestamp) > self.cache_manager.ttl_seconds:
                expired_keys.append(key)

        # Remove expired entries
        for key in expired_keys:
            if key in self.cache_manager._cache:
                self.cache_manager._cache.pop(key, None)
                self.cache_manager._timestamps.pop(key, None)

        if expired_keys:
            logger.debug(f"Cleaned {len(expired_keys)} expired cache entries")

    def clear_cache(self):
        """Clear cache"""
        if self.cache_manager:
            self.cache_manager.clear()
            logger.info("Parser cache cleared")

    def reset_metrics(self):
        """Reset metrics"""
        self.metrics.reset()
        self._batch_stats.clear()
        self._error_stats.clear()
        logger.info("Parser metrics reset")

    def get_statistics(self) -> Dict[str, Any]:
        """Get full parser statistics"""
        stats = {
            'metrics': self.metrics.to_dict(),
            'cache': self.cache_manager.get_stats() if self.cache_manager else None,
            'batch_stats': dict(self._batch_stats),
            'error_stats': dict(self._error_stats),
            'config': {
                'max_cache_size': self.config.max_cache_size,
                'cache_ttl_seconds': self.config.cache_ttl_seconds,
                'enable_cache': self.config.enable_cache,
                'batch_size': self.config.batch_size,
                'validation_enabled': self.config.validation_enabled,
                'strict_mode': self.config.strict_mode
            }
        }

        # Calculate parsing speed
        uptime = time.time() - self.metrics.last_reset_time
        if uptime > 0:
            stats['parse_speed_records_per_second'] = self.metrics.total_parsed / uptime
            stats['parse_speed_records_per_hour'] = (self.metrics.total_parsed / uptime) * 3600

        return stats

    def get_performance_report(self) -> str:
        """Get performance report as text"""
        stats = self.get_statistics()

        report_lines = [
            "📊 PARSER PERFORMANCE REPORT",
            "=" * 50,
            f"Records processed: {stats['metrics']['total_parsed']:,}",
            f"Parsing errors: {stats['metrics']['total_errors']:,}",
            f"Error rate: {stats['metrics']['error_rate']:.1f}%",
            f"Average parsing time: {stats['metrics']['avg_parse_time_ms']:.2f} ms",
            "",
            "Cache:",
        ]

        if stats['cache']:
            report_lines.extend([
                f"  Hits: {stats['cache']['hits']:,}",
                f"  Misses: {stats['cache']['misses']:,}",
                f"  Hit rate: {stats['cache']['hit_rate']}",
                f"  Size: {stats['cache']['size']:,}/{stats['cache']['max_size']:,}"
            ])
        else:
            report_lines.append("  Cache disabled")

        report_lines.append("")
        report_lines.append("Error statistics:")

        for error_type, count in stats['error_stats'].items():
            report_lines.append(f"  {error_type}: {count:,}")

        if 'parse_speed_records_per_hour' in stats:
            report_lines.append(f"\nProcessing speed: {stats['parse_speed_records_per_hour']:,.0f} records/hour")

        return "\n".join(report_lines)


# Global instances for reuse
_global_parser: Optional[DataParser] = None


def get_global_parser(config: Optional[ParserConfig] = None) -> DataParser:
    """Get global parser instance"""
    global _global_parser

    if _global_parser is None:
        _global_parser = DataParser(config)
    elif config is not None:
        # Update configuration if provided
        _global_parser.config = config

    return _global_parser


def create_standard_parser() -> DataParser:
    """Create parser with standard configuration"""
    config = ParserConfig(
        max_cache_size=15000,
        cache_ttl_seconds=7200,
        enable_cache=True,
        batch_size=2000,
        validation_enabled=True,
        strict_mode=False
    )

    return DataParser(config)


def parse_single_record(line: str, metrics: Optional[Any] = None) -> Optional[Dict[str, Any]]:
    """Quick parsing of single record using global parser"""
    parser = get_global_parser()
    return parser.parse_record(line, metrics)


def parse_batch_records(lines: List[str], metrics: Optional[Any] = None) -> List[Dict[str, Any]]:
    """Batch parsing using global parser"""
    parser = get_global_parser()
    return parser.parse_batch(lines, metrics)


def extract_key_fields_fast(line: str) -> Optional[Dict[str, str]]:
    """Quick extraction of key fields without full parsing"""
    parser = get_global_parser()
    return parser._field_extractor.extract_fields_fast(line)


def is_valid_json_line(line: str) -> bool:
    """Quick JSON line validation"""
    parser = get_global_parser()
    return parser._field_extractor.is_valid_json_line(line)


@contextmanager
def parser_context(config: Optional[ParserConfig] = None):
    """
    Context manager for parser operations
    
    Example usage:
    ```python
    with parser_context() as parser:
        results = parser.parse_batch(lines)
    ```
    """
    parser = DataParser(config)

    try:
        yield parser
    finally:
        # Clean up resources
        parser.clear_cache()


# Initialize global parser on import
get_global_parser()