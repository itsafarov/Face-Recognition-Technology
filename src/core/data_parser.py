"""
Парсер JSON данных с оптимизациями производительности и обработкой ошибок
"""

import json
import re
import hashlib
import time
import functools
import logging
from typing import Optional, Dict, Tuple, List, Any, Callable, Union
from collections import defaultdict, OrderedDict
from dataclasses import dataclass, field
from contextlib import contextmanager
import sys
import os

# Добавляем путь для корректных импортов
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Попробуем импортировать ujson для скорости, иначе используем стандартный json
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
    """Конфигурация парсера"""
    max_cache_size: int = 10000
    cache_ttl_seconds: int = 3600
    enable_cache: bool = True
    batch_size: int = 1000
    max_retries: int = 2
    validation_enabled: bool = True
    strict_mode: bool = False


@dataclass
class ParserMetrics:
    """Метрики парсера"""
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
        """Сбросить метрики"""
        self.total_parsed = 0
        self.total_errors = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.parse_time_total = 0.0
        self.last_reset_time = time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразовать в словарь"""
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
    """Менеджер кэша с ограничением памяти и времени жизни"""
    
    def __init__(self, max_size: int = 10000, ttl_seconds: int = 3600):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._cache = OrderedDict()
        self._timestamps = {}
        self._size_bytes = 0
        self._hits = 0
        self._misses = 0
        
    def get(self, key: str) -> Optional[Any]:
        """Получить значение из кэша"""
        if key not in self._cache:
            self._misses += 1
            return None
        
        # Проверяем TTL
        timestamp = self._timestamps.get(key)
        if timestamp and (time.time() - timestamp) > self.ttl_seconds:
            self._remove(key)
            self._misses += 1
            return None
        
        # Перемещаем в конец (сделали недавно использованным)
        value = self._cache.pop(key)
        self._cache[key] = value
        
        self._hits += 1
        return value
    
    def set(self, key: str, value: Any) -> None:
        """Установить значение в кэш"""
        # Если ключ уже существует, удаляем старое значение
        if key in self._cache:
            self._remove(key)
        
        # Очищаем место если нужно
        while len(self._cache) >= self.max_size:
            self._remove_oldest()
        
        # Добавляем новое значение
        self._cache[key] = value
        self._timestamps[key] = time.time()
        self._size_bytes += self._estimate_size(key, value)
    
    def _remove(self, key: str) -> None:
        """Удалить ключ из кэша"""
        if key in self._cache:
            value = self._cache.pop(key)
            self._timestamps.pop(key, None)
            self._size_bytes -= self._estimate_size(key, value)
    
    def _remove_oldest(self) -> None:
        """Удалить самый старый элемент"""
        if self._cache:
            key, value = self._cache.popitem(last=False)
            self._timestamps.pop(key, None)
            self._size_bytes -= self._estimate_size(key, value)
    
    def _estimate_size(self, key: str, value: Any) -> int:
        """Оценить размер в байтах"""
        size = len(key.encode('utf-8'))  # UTF-8 для точности
        
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
        """Очистить кэш"""
        self._cache.clear()
        self._timestamps.clear()
        self._size_bytes = 0
        self._hits = 0
        self._misses = 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Получить статистику кэша"""
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
    """Извлечение полей из JSON с поддержкой вложенных структур"""
    
    # Предварительно скомпилированные регулярные выражения для быстрого извлечения
    _FIELD_PATTERNS = {
        'timestamp': re.compile(r'"timestamp"\s*:\s*(?:\{"\$date"\s*:\s*"([^"]+)"\}|"([^"]+)")'),
        'device_id': re.compile(r'"device_id"\s*:\s*"([^"]+)"'),
        'user_name': re.compile(r'"user_name"\s*:\s*"([^"]+)"'),
        'eva_sex': re.compile(r'"eva_sex"\s*:\s*(?:"([^"]+)"|(\d+|null|true|false))'),
        'sex': re.compile(r'"sex"\s*:\s*(?:"([^"]+)"|(\d+|null|true|false))'),
        'comp_score': re.compile(r'"comp_score"\s*:\s*(?:"([^"]+)"|([\d\.]+|null|true|false))'),
        'eva_age': re.compile(r'"eva_age"\s*:\s*(?:"([^"]+)"|([\d\.]+|null|true|false))'),
        'image': re.compile(r'"image"\s*:\s*"([^"]+)"'),
        'face_id': re.compile(r'"face_id"\s*:\s*(?:"([^"]+)"|([\d\.]+|null|true|false))'),
        'company_id': re.compile(r'"company_id"\s*:\s*(?:"([^"]+)"|([\d\.]+|null|true|false))'),
        'event_type': re.compile(r'"event_type"\s*:\s*(?:"([^"]+)"|(\d+|null|true|false))'),
        'user_list': re.compile(r'"user_list"\s*:\s*(?:"([^"]+)"|(\d+|null|true|false))'),
        'ip_address': re.compile(r'"(?:IP|device_ip)"\s*:\s*"([^"]+)"'),
    }
    
    @classmethod
    def extract_fields_fast(cls, line: str) -> Optional[Dict[str, Any]]:
        """
        Быстрое извлечение полей без полного парсинга JSON.
        Возвращает только основные поля для кэширования и валидации.
        """
        if not line or len(line) < 10 or not line.strip().startswith('{'):
            return None
        
        result = {}
        
        try:
            for field_name, pattern in cls._FIELD_PATTERNS.items():
                match = pattern.search(line)
                if match:
                    # Берем первую непустую группу
                    groups = match.groups()
                    value = next((g for g in groups if g is not None), None)
                    if value:
                        result[field_name] = value
            
            return result if result else None
            
        except Exception:
            return None
    
    @classmethod
    def is_valid_json_line(cls, line: str) -> bool:
        """Быстрая проверка валидности JSON строки"""
        line = line.strip()
        
        # Быстрая проверка по первым и последним символам
        if len(line) < 2:
            return False
        
        if not (line.startswith('{') and line.endswith('}')):
            return False
        
        # Проверяем наличие ключевых полей
        if '"timestamp"' not in line and '"device_id"' not in line:
            return False
        
        return True


class ValueTransformer:
    """Трансформация значений с кэшированием"""
    
    # Кэшированные функции преобразования
    _cache_size = 10000
    
    @staticmethod
    @functools.lru_cache(maxsize=_cache_size)
    def transform_timestamp(value: Optional[str]) -> str:
        """Преобразование timestamp"""
        if not value or value.lower() in ('null', 'none', 'nan', ''):
            return "Н/Д"
        
        try:
            # Обработка формата MongoDB
            if '$date' in value:
                try:
                    # Убираем экранирование
                    import json as json_module
                    # Пытаемся распарсить как JSON если это строка JSON
                    if value.strip().startswith('{'):
                        data = json_module.loads(value)
                        value = data.get('$date', '')
                    else:
                        # Ищем $date в строке
                        import re
                        match = re.search(r'\$date["\']?\s*:\s*["\']?([^"\'\s}]+)', value)
                        if match:
                            value = match.group(1)
                except:
                    # Если не получилось, оставляем как есть
                    pass
            
            # Упрощенная обработка ISO формата
            if 'T' in value:
                # Убираем Z и миллисекунды
                if value.endswith('Z'):
                    value = value[:-1]
                # Разделяем дату и время
                parts = value.split('T')
                if len(parts) == 2:
                    date_part = parts[0]
                    time_part = parts[1].split('.')[0]  # Убираем миллисекунды
                    return f"{date_part} {time_part}"
            
            return value
            
        except Exception:
            return "Н/Д"
    
    @staticmethod
    @functools.lru_cache(maxsize=_cache_size)
    def transform_gender(eva_sex: Optional[str], sex: Optional[str]) -> str:
        """Преобразование пола"""
        # Приводим к строке и нижнему регистру
        eva_sex_str = str(eva_sex).lower().strip() if eva_sex else ''
        sex_str = str(sex).lower().strip() if sex else ''
        
        # Словарь для быстрого поиска
        gender_map = {
            # eva_sex значения
            'female': "Женский",
            'f': "Женский",
            'жен': "Женский",
            '0': "Женский",
            'male': "Мужской",
            'm': "Мужской",
            'муж': "Мужской",
            '1': "Мужской",
            
            # sex значения
            '0': "Женский",
            '1': "Мужской",
            
            # Неопределенные значения
            'null': "Н/Д",
            'none': "Н/Д",
            'nan': "Н/Д",
            '': "Н/Д",
        }
        
        # Сначала проверяем eva_sex
        if eva_sex_str and eva_sex_str in gender_map:
            return gender_map[eva_sex_str]
        
        # Затем проверяем sex
        if sex_str and sex_str in gender_map:
            return gender_map[sex_str]
        
        return "Н/Д"
    
    @staticmethod
    @functools.lru_cache(maxsize=_cache_size)
    def transform_score(value: Optional[str]) -> str:
        """Преобразование оценки"""
        if not value or value.lower() in ('null', 'none', 'nan', ''):
            return "Н/Д"
        
        try:
            # Убираем символы процента и пробелы
            clean_value = ''.join(c for c in str(value) if c.isdigit() or c == '.')
            if not clean_value:
                return "Н/Д"
            
            score = float(clean_value)
            return f"{score:.1f}%"
            
        except Exception:
            return "Н/Д"
    
    @staticmethod
    @functools.lru_cache(maxsize=_cache_size)
    def transform_age(value: Optional[str]) -> str:
        """Преобразование возраста"""
        if not value or value.lower() in ('null', 'none', 'nan', ''):
            return "Н/Д"
        
        try:
            age = int(float(str(value)))
            return str(age)
        except Exception:
            return "Н/Д"
    
    @staticmethod
    def safe_str(value: Any, default: str = '') -> str:
        """Безопасное преобразование в строку"""
        if value is None:
            return default
        
        try:
            result = str(value).strip()
            return result if result else default
        except Exception:
            return default


class DataParser:
    """
    Оптимизированный парсер JSON данных с кэшированием и обработкой ошибок
    
    Особенности:
    - Кэширование результатов парсинга
    - Быстрое извлечение полей без полного парсинга
    - Пакетная обработка записей
    - Мониторинг производительности
    - Поддержка разных форматов данных
    """
    
    def __init__(self, config: Optional[ParserConfig] = None):
        self.config = config or ParserConfig()
        self.metrics = ParserMetrics()
        
        # Кэши
        self.cache_manager = CacheManager(
            max_size=self.config.max_cache_size,
            ttl_seconds=self.config.cache_ttl_seconds
        ) if self.config.enable_cache else None
        
        # Статистика
        self._batch_stats = defaultdict(int)
        self._error_stats = defaultdict(int)
        
        # Хэш-функция для строк
        self._hash_func = hashlib.md5
        
        logger.info(f"DataParser инициализирован: cache={self.config.enable_cache}, "
                   f"batch_size={self.config.batch_size}")
    
    def _generate_line_hash(self, line: str) -> str:
        """Генерация хэша строки"""
        return self._hash_func(line.encode('utf-8')).hexdigest()[:16]
    
    def parse_record(self, line: str, metrics: Optional[Any] = None) -> Optional[Dict[str, Any]]:
        """
        Парсинг одной записи с кэшированием и обработкой ошибок
        
        Args:
            line: Строка JSON для парсинга
            metrics: Опциональные метрики для обновления
        
        Returns:
            Dict с распарсенными данными или None при ошибке
        """
        start_time = time.time()
        
        # Быстрая валидация строки
        if not FieldExtractor.is_valid_json_line(line):
            self.metrics.total_errors += 1
            self._error_stats['invalid_format'] += 1
            if metrics and hasattr(metrics, 'json_errors'):
                metrics.json_errors += 1
            return None
        
        # Проверка кэша
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
            # Парсинг JSON
            data = JSON.loads(line.strip())
            
            # Извлечение полей с безопасными значениями по умолчанию
            result = self._extract_fields(data)
            
            # Валидация результата
            if self.config.validation_enabled and not self._validate_record(result):
                self.metrics.total_errors += 1
                self._error_stats['validation_failed'] += 1
                if metrics and hasattr(metrics, 'json_errors'):
                    metrics.json_errors += 1
                return None
            
            # Сохранение в кэш
            if self.config.enable_cache and self.cache_manager and line_hash:
                self.cache_manager.set(line_hash, result)
            
            # Обновление метрик
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
            
            # В строгом режиме перевыбрасываем исключение
            if self.config.strict_mode and not isinstance(e, (MemoryError, KeyboardInterrupt)):
                raise
            
            return None
    
    def _extract_fields(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Извлечение и преобразование полей из данных"""
        # Извлечение timestamp
        timestamp_raw = ""
        timestamp_obj = data.get('timestamp', {})
        if isinstance(timestamp_obj, dict):
            timestamp_raw = timestamp_obj.get('$date', '')
        elif isinstance(timestamp_obj, str):
            timestamp_raw = timestamp_obj
        
        # Извлечение других полей
        eva_sex = data.get('eva_sex', '')
        sex = data.get('sex', '')
        comp_score = data.get('comp_score', '')
        eva_age = data.get('eva_age', '')
        
        # Извлечение IP адреса (может быть в разных полях)
        ip_address = data.get('IP', data.get('device_ip', ''))
        
        # Извлечение MongoDB _id
        mongo_id = ""
        mongo_id_obj = data.get('_id', {})
        if isinstance(mongo_id_obj, dict):
            mongo_id = mongo_id_obj.get('$oid', '')
        elif mongo_id_obj:
            mongo_id = str(mongo_id_obj)
        
        # Применение трансформаций
        timestamp = ValueTransformer.transform_timestamp(timestamp_raw)
        gender = ValueTransformer.transform_gender(eva_sex, sex)
        score = ValueTransformer.transform_score(comp_score)
        age = ValueTransformer.transform_age(eva_age)
        
        # Создание результата
        return {
            'timestamp': timestamp,
            'device_id': ValueTransformer.safe_str(data.get('device_id', ''), "Н/Д"),
            'user_name': ValueTransformer.safe_str(data.get('user_name', ''), "Н/Д"),
            'gender': gender,
            'age': age,
            'score': score,
            'face_id': ValueTransformer.safe_str(data.get('face_id', ''), "Н/Д"),
            'company_id': ValueTransformer.safe_str(data.get('company_id', ''), "Н/Д"),
            'image_url': ValueTransformer.safe_str(data.get('image', ''), ""),
            'event_type': ValueTransformer.safe_str(data.get('event_type', ''), ""),
            'user_list': ValueTransformer.safe_str(data.get('user_list', ''), ""),
            'ip_address': ValueTransformer.safe_str(ip_address, "Н/Д"),
            
            # Дополнительные поля
            'user_id': ValueTransformer.safe_str(data.get('user_id', ''), ""),
            'frpic_name': ValueTransformer.safe_str(data.get('frpic_name', ''), ""),
            'request_type': ValueTransformer.safe_str(data.get('request_type', ''), ""),
            'group': ValueTransformer.safe_str(data.get('group', ''), ""),
            'mongo_id': mongo_id,
            'company_type': ValueTransformer.safe_str(data.get('company_type', ''), "")
        }
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """Валидация распарсенной записи"""
        # Проверяем обязательные поля
        required_fields = ['timestamp', 'device_id', 'user_name']
        
        for field in required_fields:
            if not record.get(field) or record[field] == 'Н/Д':
                return False
        
        # Дополнительные проверки
        if 'image_url' in record and record['image_url']:
            if not record['image_url'].startswith(('http://', 'https://')):
                return False
        
        return True
    
    def parse_batch(self, lines: List[str], metrics: Optional[Any] = None) -> List[Dict[str, Any]]:
        """
        Пакетный парсинг записей
        
        Args:
            lines: Список строк для парсинга
            metrics: Опциональные метрики для обновления
        
        Returns:
            Список распарсенных записей
        """
        start_time = time.time()
        batch_size = len(lines)
        results = []
        
        # Разбиваем на подбатчи для лучшего контроля памяти
        batch_size_actual = min(batch_size, self.config.batch_size)
        
        for i in range(0, batch_size, batch_size_actual):
            sub_batch = lines[i:i + batch_size_actual]
            
            # Параллельная обработка подбатча
            for line in sub_batch:
                result = self.parse_record(line, metrics)
                if result:
                    results.append(result)
            
            # Периодическая очистка кэша
            if i % (batch_size_actual * 10) == 0 and self.cache_manager:
                # Удаляем устаревшие записи
                self._clean_expired_cache()
        
        # Обновление статистики
        parse_time = time.time() - start_time
        self._batch_stats['total_batches'] += 1
        self._batch_stats['total_records_in_batches'] += batch_size
        self._batch_stats['total_time_in_batches'] += parse_time
        
        logger.debug(f"Пакет из {batch_size} записей обработан за {parse_time:.3f} сек, "
                    f"успешно: {len(results)}")
        
        return results
    
    def _clean_expired_cache(self):
        """Очистка устаревших записей в кэше"""
        if not self.cache_manager:
            return
        
        current_time = time.time()
        expired_keys = []
        
        # Находим устаревшие ключи (используем внутренние атрибуты)
        for key, timestamp in self.cache_manager._timestamps.items():
            if (current_time - timestamp) > self.cache_manager.ttl_seconds:
                expired_keys.append(key)
        
        # Удаляем устаревшие записи
        for key in expired_keys:
            if key in self.cache_manager._cache:
                self.cache_manager._cache.pop(key, None)
                self.cache_manager._timestamps.pop(key, None)
        
        if expired_keys:
            logger.debug(f"Очищено {len(expired_keys)} устаревших записей из кэша")
    
    def clear_cache(self):
        """Очистить кэш"""
        if self.cache_manager:
            self.cache_manager.clear()
            logger.info("Кэш парсера очищен")
    
    def reset_metrics(self):
        """Сбросить метрики"""
        self.metrics.reset()
        self._batch_stats.clear()
        self._error_stats.clear()
        logger.info("Метрики парсера сброшены")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Получить полную статистику парсера"""
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
        
        # Рассчитываем скорость парсинга
        uptime = time.time() - self.metrics.last_reset_time
        if uptime > 0:
            stats['parse_speed_records_per_second'] = self.metrics.total_parsed / uptime
            stats['parse_speed_records_per_hour'] = (self.metrics.total_parsed / uptime) * 3600
        
        return stats
    
    def get_performance_report(self) -> str:
        """Получить текстовый отчет о производительности"""
        stats = self.get_statistics()
        
        report_lines = [
            "📊 ОТЧЕТ ПРОИЗВОДИТЕЛЬНОСТИ ПАРСЕРА",
            "=" * 50,
            f"Обработано записей: {stats['metrics']['total_parsed']:,}",
            f"Ошибок парсинга: {stats['metrics']['total_errors']:,}",
            f"Процент ошибок: {stats['metrics']['error_rate']:.1f}%",
            f"Среднее время парсинга: {stats['metrics']['avg_parse_time_ms']:.2f} мс",
            "",
            "Кэш:",
        ]
        
        if stats['cache']:
            report_lines.extend([
                f"  Хиты: {stats['cache']['hits']:,}",
                f"  Промахи: {stats['cache']['misses']:,}",
                f"  Эффективность: {stats['cache']['hit_rate']}",
                f"  Размер: {stats['cache']['size']:,}/{stats['cache']['max_size']:,}"
            ])
        else:
            report_lines.append("  Кэш отключен")
        
        report_lines.append("")
        report_lines.append("Статистика ошибок:")
        
        for error_type, count in stats['error_stats'].items():
            report_lines.append(f"  {error_type}: {count:,}")
        
        if 'parse_speed_records_per_hour' in stats:
            report_lines.append(f"\nСкорость обработки: {stats['parse_speed_records_per_hour']:,.0f} записей/час")
        
        return "\n".join(report_lines)


class FastDataParser:
    """
    Быстрый парсер для однородных данных с предварительной настройкой формата
    
    Используется когда структура данных известна заранее и не меняется
    """
    
    def __init__(self, field_config: Optional[Dict[str, Dict]] = None):
        self.field_config = field_config or {}
        self.field_extractors = self._build_extractors()
        self.metrics = ParserMetrics()
        
    def _build_extractors(self) -> Dict[str, Callable]:
        """Построение функций извлечения полей на основе конфигурации"""
        extractors = {}
        
        for field_name, config in self.field_config.items():
            path = config.get('path', [field_name])
            default = config.get('default', '')
            transformer = config.get('transformer', lambda x: x)
            
            def make_extractor(field_path, field_default, field_transformer):
                def extractor(data: Dict) -> Any:
                    value = data
                    for key in field_path:
                        if isinstance(value, dict):
                            value = value.get(key, None)
                        else:
                            value = None
                            break
                    
                    if value is None or value == '':
                        return field_default
                    
                    return field_transformer(value)
                
                return extractor
            
            extractors[field_name] = make_extractor(path, default, transformer)
        
        return extractors
    
    def parse_record_fast(self, line: str) -> Optional[Dict[str, Any]]:
        """Быстрый парсинг записи с предварительно настроенным форматом"""
        try:
            data = JSON.loads(line.strip())
            self.metrics.total_parsed += 1
            
            result = {}
            for field_name, extractor in self.field_extractors.items():
                result[field_name] = extractor(data)
            
            return result
            
        except Exception as e:
            self.metrics.total_errors += 1
            logger.debug(f"Fast parsing error: {e}")
            return None
    
    def parse_batch_fast(self, lines: List[str]) -> List[Dict[str, Any]]:
        """Пакетный парсинг быстрым методом"""
        results = []
        
        for line in lines:
            record = self.parse_record_fast(line)
            if record:
                results.append(record)
        
        return results


# Предварительно настроенные конфигурации форматов
STANDARD_FORMAT_CONFIG = {
    'timestamp': {
        'path': ['timestamp', '$date'],
        'default': 'Н/Д',
        'transformer': ValueTransformer.transform_timestamp
    },
    'device_id': {
        'path': ['device_id'],
        'default': 'Н/Д',
        'transformer': ValueTransformer.safe_str
    },
    'user_name': {
        'path': ['user_name'],
        'default': 'Н/Д',
        'transformer': ValueTransformer.safe_str
    },
    'gender': {
        'path': ['eva_sex', 'sex'],
        'default': 'Н/Д',
        'transformer': lambda x: ValueTransformer.transform_gender(
            x[0] if isinstance(x, list) and len(x) > 0 else '',
            x[1] if isinstance(x, list) and len(x) > 1 else ''
        )
    },
    'age': {
        'path': ['eva_age'],
        'default': 'Н/Д',
        'transformer': ValueTransformer.transform_age
    },
    'score': {
        'path': ['comp_score'],
        'default': 'Н/Д',
        'transformer': ValueTransformer.transform_score
    }
}


# Глобальный экземпляр для повторного использования
_global_parser: Optional[DataParser] = None


def get_global_parser(config: Optional[ParserConfig] = None) -> DataParser:
    """Получить глобальный экземпляр парсера"""
    global _global_parser
    
    if _global_parser is None:
        _global_parser = DataParser(config)
    elif config is not None:
        # Обновляем конфигурацию если передана
        _global_parser.config = config
    
    return _global_parser


def create_standard_parser() -> DataParser:
    """Создать парсер со стандартной конфигурацией"""
    config = ParserConfig(
        max_cache_size=15000,
        cache_ttl_seconds=7200,
        enable_cache=True,
        batch_size=2000,
        validation_enabled=True,
        strict_mode=False
    )
    
    return DataParser(config)


def create_fast_parser(field_config: Optional[Dict] = None) -> FastDataParser:
    """Создать быстрый парсер с заданной конфигурацией полей"""
    config = field_config or STANDARD_FORMAT_CONFIG
    return FastDataParser(config)


# Утилиты для быстрого доступа
def parse_single_record(line: str, metrics: Optional[Any] = None) -> Optional[Dict[str, Any]]:
    """Быстрый парсинг одной записи с использованием глобального парсера"""
    parser = get_global_parser()
    return parser.parse_record(line, metrics)


def parse_batch_records(lines: List[str], metrics: Optional[Any] = None) -> List[Dict[str, Any]]:
    """Пакетный парсинг записей с использованием глобального парсера"""
    parser = get_global_parser()
    return parser.parse_batch(lines, metrics)


def extract_key_fields_fast(line: str) -> Optional[Dict[str, str]]:
    """Быстрое извлечение ключевых полей без полного парсинга"""
    return FieldExtractor.extract_fields_fast(line)


def is_valid_json_line(line: str) -> bool:
    """Быстрая проверка валидности JSON строки"""
    return FieldExtractor.is_valid_json_line(line)


@contextmanager
def parser_context(config: Optional[ParserConfig] = None):
    """
    Контекстный менеджер для работы с парсером
    
    Пример использования:
    ```python
    with parser_context() as parser:
        results = parser.parse_batch(lines)
    ```
    """
    parser = DataParser(config)
    
    try:
        yield parser
    finally:
        # Очистка ресурсов при выходе
        parser.clear_cache()


# Инициализация глобального парсера при импорте
get_global_parser()