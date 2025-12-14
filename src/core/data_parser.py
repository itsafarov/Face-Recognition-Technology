"""
Парсер JSON данных с оптимизациями производительности
"""

import json
import datetime
import re
import hashlib
import time
from typing import Optional, Dict, Tuple, List, Any
from collections import defaultdict
from functools import lru_cache
from core.models import ProcessingMetrics
from utils.logger import setup_logging

logger = setup_logging()

try:
    import ujson
    JSON = ujson
except ImportError:
    import json
    JSON = json

class DataParser:
    """Парсер JSON данных с кэшированием и оптимизацией"""
    
    def __init__(self):
        # Кэш для ускорения парсинга часто встречающихся данных
        self._cache = {}
        self._cache_size = 10000  # Увеличили кэш
        self._cache_hits = 0
        self._cache_misses = 0
        
        # Предварительная компиляция регулярных выражений
        self._timestamp_regex = re.compile(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})')
        self._score_regex = re.compile(r'([\d\.]+)%?')
        self._url_regex = re.compile(r'https?://[^\s]+')
        
        # Предварительно созданные словари для быстрого доступа
        self._gender_map = {
            '0': "Женский",
            '1': "Мужской",
            'female': "Женский",
            'f': "Женский",
            'жен': "Женский",
            'муж': "Мужской",
            'male': "Мужской",
            'm': "Мужской",
            'null': "Н/Д",
            'none': "Н/Д",
            '': "Н/Д",
            None: "Н/Д"
        }
        
        # Кэш для преобразования дат
        self._date_cache = {}
        
        # Статистика
        self.total_parsed = 0
        self.avg_parse_time = 0.0
        
        # Батчинг для групповой обработки
        self._batch_buffer = []
        self._batch_size = 100
        
        try:
            logger.info("DataParser инициализирован с оптимизациями")
        except NameError:
            # Если logger не определен, используем print как fallback
            print("DataParser инициализирован с оптимизациями")
    
    def _generate_line_hash(self, line: str) -> str:
        """Быстрая генерация хэша строки"""
        return hashlib.md5(line.encode()).hexdigest()[:16]
    
    def _safe_get(self, data: dict, key: str, default: Any = '') -> str:
        """Безопасное получение значения из словаря с преобразованием в строку"""
        try:
            value = data.get(key, default)
            if value is None:
                return str(default)
            return str(value).strip()
        except:
            return str(default)
    
    @lru_cache(maxsize=10000)
    def _parse_timestamp_cached(self, timestamp_str: str) -> str:
        """Кэшированное преобразование временной метки"""
        # Convert to string to handle cases where timestamp_str might be float (like NaN)
        timestamp_str = str(timestamp_str) if timestamp_str is not None else ''
        
        if not timestamp_str or timestamp_str in ['null', 'None', 'nan', 'NaN', '<na>']:
            return "Н/Д"
        
        try:
            # Упрощенная обработка timestamp
            if 'Z' in timestamp_str:
                timestamp_str = timestamp_str.replace('Z', '+00:00')
            
            if '$date' in timestamp_str:
                # Обработка формата MongoDB
                try:
                    timestamp_data = JSON.loads(timestamp_str)
                    timestamp_str = timestamp_data.get('$date', '')
                except:
                    pass
            
            # Быстрый парсинг даты
            if 'T' in timestamp_str:
                date_part, time_part = timestamp_str.split('T', 1)
                time_part = time_part.split('.')[0]  # Убираем миллисекунды
                if '+' in time_part:
                    time_part = time_part.split('+')[0]
                elif 'Z' in time_part:
                    time_part = time_part.replace('Z', '')
                
                # Форматируем без создания datetime объекта
                return f"{date_part} {time_part}"
            
            return timestamp_str
            
        except Exception:
            return "Н/Д"
    
    @lru_cache(maxsize=10000)
    def _parse_gender_cached(self, eva_sex: str, sex: str) -> str:
        """Кэшированное определение пола"""
        # Convert to string to handle cases where values might be float (like NaN)
        eva_sex = str(eva_sex) if eva_sex is not None else ''
        sex = str(sex) if sex is not None else ''
        
        # Приводим к нижнему регистру
        eva_sex_lower = eva_sex.lower() if eva_sex else ''
        sex_lower = sex.lower() if sex else ''
        
        # Проверяем eva_sex
        if eva_sex_lower and eva_sex_lower not in ['null', 'none', '', 'nan', 'NaN', '<na>']:
            if eva_sex_lower in ['female', 'f', 'жен']:
                return "Женский"
            else:
                return "Мужской"
        
        # Проверяем sex
        if sex_lower == '0':
            return "Женский"
        elif sex_lower == '1':
            return "Мужской"
        
        return "Н/Д"
    
    @lru_cache(maxsize=10000)
    def _parse_score_cached(self, score_str: str) -> str:
        """Кэшированное преобразование оценки"""
        # Convert to string to handle cases where score_str might be float (like NaN)
        score_str = str(score_str) if score_str is not None else ''
        
        if not score_str or score_str in ['null', 'None', 'nan', 'NaN', '<na>']:
            return "Н/Д"
        
        try:
            # Убираем символы процента и пробелы
            clean_score = score_str.replace('%', '').replace(' ', '').strip()
            if not clean_score:
                return "Н/Д"
            
            # Преобразуем в float
            score_float = float(clean_score)
            
            # Форматируем с одним знаком после запятой
            return f"{score_float:.1f}%"
            
        except (ValueError, TypeError):
            return "Н/Д"
    
    @lru_cache(maxsize=10000)
    def _parse_age_cached(self, age_str: str) -> str:
        """Кэшированное преобразование возраста"""
        # Convert to string to handle cases where age_str might be float (like NaN)
        age_str = str(age_str) if age_str is not None else ''
        
        if not age_str or age_str in ['null', 'None', 'nan', 'NaN', '<na>']:
            return "Н/Д"
        
        try:
            # Просто возвращаем строку, если это уже число
            age_int = int(age_str)
            return str(age_int)
        except:
            return "Н/Д"
    
    def parse_record(self, line: str, metrics: ProcessingMetrics) -> Optional[Dict]:
        """Парсинг одной записи с улучшенным анализом и кэшированием"""
        start_time = time.time()
        
        # Генерируем хэш строки для кэширования
        line_hash = self._generate_line_hash(line)
        
        # Проверка кэша
        if line_hash in self._cache:
            self._cache_hits += 1
            cached_result = self._cache[line_hash]
            
            # Обновляем метрики уникальности из кэша
            if cached_result:
                self._update_metrics_from_cache(cached_result, metrics)
            
            metrics.total_records += 1
            return cached_result
        
        self._cache_misses += 1
        
        try:
            # Быстрый парсинг JSON
            data = JSON.loads(line.strip())
            metrics.total_records += 1
            
            # Извлечение базовых полей
            timestamp = data.get('timestamp', {}).get('$date', '')
            eva_sex = data.get('eva_sex', '')
            sex = data.get('sex', '')
            comp_score = data.get('comp_score', '')
            eva_age = data.get('eva_age', '')
            ip_address = data.get('IP', data.get('device_ip', ''))
            event_type = data.get('event_type', '')
            user_list = data.get('user_list', '')
            user_name = data.get('user_name', '')
            device_id = data.get('device_id', '')
            company_id = str(data.get('company_id', ''))
            face_id = data.get('face_id', '')
            image_url = data.get('image', '')
            
            # Используем кэшированные функции для преобразования данных
            readable_time = self._parse_timestamp_cached(timestamp)
            gender = self._parse_gender_cached(eva_sex, sex)
            score_display = self._parse_score_cached(comp_score)
            age_display = self._parse_age_cached(eva_age)
            
            # Подготовка результата
            result = {
                'timestamp': readable_time,
                'device_id': str(device_id) if device_id else 'Н/Д',
                'user_name': str(user_name) if user_name else 'Н/Д',
                'gender': gender,
                'age': age_display,
                'score': score_display,
                'face_id': str(face_id) if face_id else 'Н/Д',
                'company_id': str(company_id) if company_id else 'Н/Д',
                'image_url': str(image_url) if image_url else '',
                'event_type': str(event_type) if event_type else 'Н/Д',
                'user_list': str(user_list) if user_list else 'Н/Д',
                'ip_address': str(ip_address) if ip_address else 'Н/Д'
            }
            
            # Обновляем метрики уникальности
            self._update_metrics(result, metrics)
            
            # Сохраняем в кэш
            self._add_to_cache(line_hash, result)
            
            # Обновляем статистику времени парсинга
            parse_time = time.time() - start_time
            self._update_parse_stats(parse_time)
            
            return result
            
        except JSON.JSONDecodeError as e:
            metrics.json_errors += 1
            if logger.isEnabledFor(10):  # DEBUG level
                logger.debug(f"JSON decode error: {e} for line: {line[:100]}...")
            return None
        except UnicodeDecodeError as e:
            logger.error(f"Unicode decode error: {e}")
            metrics.json_errors += 1
            return None
        except Exception as e:
            logger.error(f"Unexpected parsing error: {e}")
            # Перевыбрасываем критические ошибки для диагностики
            if not isinstance(e, (MemoryError, KeyboardInterrupt, SystemExit)):
                raise
            return None
    
    def parse_records_batch(self, lines: List[str], metrics: ProcessingMetrics) -> List[Dict]:
        """Пакетный парсинг записей для увеличения производительности"""
        start_time = time.time()
        results = []
        
        for i, line in enumerate(lines):
            result = self.parse_record(line, metrics)
            if result:
                results.append(result)
            
            # Периодическая очистка кэша для экономии памяти
            if i % 1000 == 0 and len(self._cache) > self._cache_size * 1.5:
                self._clean_cache()
        
        batch_time = time.time() - start_time
        self._update_batch_stats(batch_time, len(lines))
        
        return results
    
    def _add_to_cache(self, key: str, value: Dict):
        """Добавить результат в кэш"""
        if len(self._cache) >= self._cache_size:
            # Удаляем самые старые записи
            oldest_key = next(iter(self._cache))
            del self._cache[oldest_key]
        
        self._cache[key] = value
    
    def _clean_cache(self):
        """Очистка кэша для экономии памяти"""
        # Оставляем только 80% самых "свежих" записей
        new_size = int(self._cache_size * 0.8)
        if len(self._cache) > new_size:
            # Создаем новый словарь с самыми новыми записями
            all_items = list(self._cache.items())
            self._cache = dict(all_items[-new_size:])
    
    def _update_metrics(self, record: Dict, metrics: ProcessingMetrics):
        """Обновление метрик уникальности"""
        user_name = record.get('user_name', '')
        device_id = record.get('device_id', '')
        company_id = record.get('company_id', '')
        ip_address = record.get('ip_address', '')
        
        if user_name and user_name != 'Н/Д':
            metrics.unique_users.add(str(user_name))
        if device_id and device_id != 'Н/Д':
            metrics.unique_devices.add(str(device_id))
        if company_id and company_id != 'Н/Д':
            metrics.unique_companies.add(str(company_id))
        if ip_address and ip_address != 'Н/Д':
            metrics.unique_ips.add(str(ip_address))
    
    def _update_metrics_from_cache(self, record: Dict, metrics: ProcessingMetrics):
        """Обновление метрик из кэшированной записи"""
        # Для кэшированных записей обновляем только общие счетчики
        # Не обновляем уникальные множества, чтобы избежать дублирования
        pass
    
    def _update_parse_stats(self, parse_time: float):
        """Обновление статистики парсинга"""
        self.total_parsed += 1
        # Экспоненциальное скользящее среднее для быстрого расчета
        if self.avg_parse_time == 0:
            self.avg_parse_time = parse_time
        else:
            self.avg_parse_time = self.avg_parse_time * 0.99 + parse_time * 0.01
    
    def _update_batch_stats(self, batch_time: float, num_records: int):
        """Обновление статистики пакетной обработки"""
        avg_time_per_record = batch_time / num_records if num_records > 0 else 0
        self.avg_parse_time = avg_time_per_record
    
    def get_statistics(self) -> Dict[str, Any]:
        """Получить статистику парсера"""
        cache_hit_rate = 0
        if self._cache_hits + self._cache_misses > 0:
            cache_hit_rate = (self._cache_hits / (self._cache_hits + self._cache_misses)) * 100
        
        return {
            'cache_size': len(self._cache),
            'cache_hits': self._cache_hits,
            'cache_misses': self._cache_misses,
            'cache_hit_rate': f"{cache_hit_rate:.1f}%",
            'total_parsed': self.total_parsed,
            'avg_parse_time_ms': self.avg_parse_time * 1000,
            'cache_memory_mb': self._estimate_cache_memory() / 1024 / 1024
        }
    
    def _estimate_cache_memory(self) -> int:
        """Оценка использования памяти кэшем"""
        total_size = 0
        for key, value in self._cache.items():
            total_size += len(key) * 2  # utf-16
            if value:
                # Примерная оценка размера словаря
                total_size += sum(len(str(k)) + len(str(v)) for k, v in value.items()) * 2
        
        return total_size
    
    def clear_cache(self):
        """Очистить кэш"""
        self._cache.clear()
        self._cache_hits = 0
        self._cache_misses = 0
        self._parse_timestamp_cached.cache_clear()
        self._parse_gender_cached.cache_clear()
        self._parse_score_cached.cache_clear()
        self._parse_age_cached.cache_clear()

class FastDataParser:
    """Быстрый парсер для однородных данных (альтернативная реализация)"""
    
    def __init__(self):
        # Используем slots для экономии памяти
        self._field_positions = {}
        self._field_defaults = {}
        
        # Быстрые функции преобразования
        self._transformers = {}
        
        # Статистика
        self.records_processed = 0
        self.parse_errors = 0
    
    def configure(self, field_config: Dict[str, Dict]):
        """Настройка парсера для конкретного формата данных"""
        for field_name, config in field_config.items():
            self._field_positions[field_name] = config.get('path', [])
            self._field_defaults[field_name] = config.get('default', '')
            
            # Регистрация преобразователя
            transformer = config.get('transformer')
            if transformer:
                self._transformers[field_name] = transformer
    
    def parse_record_fast(self, line: str) -> Optional[Dict]:
        """Быстрый парсинг записи с предварительно настроенным форматом"""
        try:
            data = JSON.loads(line.strip())
            self.records_processed += 1
            
            result = {}
            for field_name, path in self._field_positions.items():
                value = data
                
                # Получаем значение по пути
                for key in path:
                    if isinstance(value, dict):
                        value = value.get(key, None)
                    else:
                        value = None
                        break
                
                # Применяем преобразователь если есть
                if value is None or value == '':
                    value = self._field_defaults[field_name]
                elif field_name in self._transformers:
                    value = self._transformers[field_name](value)
                
                result[field_name] = value
            
            return result
            
        except Exception:
            self.parse_errors += 1
            return None
    
    def parse_records_stream(self, lines: List[str]) -> List[Dict]:
        """Потоковый парсинг с минимальным использованием памяти"""
        results = []
        for line in lines:
            record = self.parse_record_fast(line)
            if record:
                results.append(record)
        return results

# Глобальный экземпляр для повторного использования
_global_parser = None

def get_global_parser() -> DataParser:
    """Получить глобальный экземпляр парсера"""
    global _global_parser
    if _global_parser is None:
        _global_parser = DataParser()
    return _global_parser

# Функции для быстрого доступа
def parse_single_record(line: str, metrics: ProcessingMetrics) -> Optional[Dict]:
    """Быстрый парсинг одной записи с использованием глобального парсера"""
    parser = get_global_parser()
    return parser.parse_record(line, metrics)

def parse_batch_records(lines: List[str], metrics: ProcessingMetrics) -> List[Dict]:
    """Пакетный парсинг записей"""
    parser = get_global_parser()
    return parser.parse_records_batch(lines, metrics)

# Предварительно настроенные форматы для часто встречающихся структур
STANDARD_FORMAT_CONFIG = {
    'timestamp': {
        'path': ['timestamp', '$date'],
        'default': 'Н/Д',
        'transformer': lambda x: parse_timestamp_optimized(x)
    },
    'device_id': {
        'path': ['device_id'],
        'default': 'Н/Д'
    },
    'user_name': {
        'path': ['user_name'],
        'default': 'Н/Д'
    },
    'gender': {
        'path': ['eva_sex', 'sex'],
        'default': 'Н/Д',
        'transformer': lambda x: parse_gender_optimized(x[0], x[1]) if isinstance(x, list) else x
    },
    'age': {
        'path': ['eva_age'],
        'default': 'Н/Д'
    },
    'score': {
        'path': ['comp_score'],
        'default': 'Н/Д'
    }
}

@lru_cache(maxsize=10000)
def parse_timestamp_optimized(timestamp_str: str) -> str:
    """Оптимизированное преобразование timestamp"""
    if not timestamp_str or timestamp_str.lower() in ['null', 'none', '']:
        return "Н/Д"
    
    try:
        # Быстрый парсинг ISO формата
        if 'T' in timestamp_str:
            # Убираем Z и миллисекунды
            clean_timestamp = timestamp_str.replace('Z', '').split('.')[0]
            return clean_timestamp.replace('T', ' ')
        
        return timestamp_str
    except:
        return "Н/Д"

@lru_cache(maxsize=10000)
def parse_gender_optimized(eva_sex: str, sex: str) -> str:
    """Оптимизированное определение пола"""
    # Быстрая проверка eva_sex
    if eva_sex:
        eva_lower = str(eva_sex).lower()
        if eva_lower in ['female', 'f', 'жен']:
            return "Женский"
        elif eva_lower in ['male', 'm', 'муж']:
            return "Мужской"
    
    # Быстрая проверка sex
    if sex == '0':
        return "Женский"
    elif sex == '1':
        return "Мужской"
    
    return "Н/Д"

@lru_cache(maxsize=10000)
def parse_score_optimized(score_str: str) -> str:
    """Оптимизированное преобразование оценки"""
    if not score_str or score_str.lower() in ['null', 'none', '']:
        return "Н/Д"
    
    try:
        # Убираем нецифровые символы
        clean_score = ''.join(c for c in str(score_str) if c.isdigit() or c == '.')
        if not clean_score:
            return "Н/Д"
        
        score_float = float(clean_score)
        return f"{score_float:.1f}%"
    except:
        return "Н/Д"

# Быстрые валидаторы
def is_valid_json_line(line: str) -> bool:
    """Быстрая проверка валидности JSON строки"""
    line = line.strip()
    return (len(line) > 2 and 
            line.startswith('{') and 
            line.endswith('}') and
            '"' in line)

def extract_key_fields_fast(line: str) -> Optional[Dict[str, str]]:
    """Быстрое извлечение ключевых полей без полного парсинга"""
    try:
        # Ищем ключевые поля с помощью регулярных выражений
        import re
        
        patterns = {
            'timestamp': r'"timestamp"\s*:\s*{\s*"\$date"\s*:\s*"([^"]+)"',
            'device_id': r'"device_id"\s*:\s*"([^"]+)"',
            'user_name': r'"user_name"\s*:\s*"([^"]+)"',
            'image_url': r'"image"\s*:\s*"([^"]+)"'
        }
        
        result = {}
        for field, pattern in patterns.items():
            match = re.search(pattern, line)
            if match:
                result[field] = match.group(1)
        
        return result if result else None
        
    except:
        return None

# Утилиты для обработки ошибок
class ParseErrorCounter:
    """Счетчик ошибок парсинга"""
    
    def __init__(self):
        self.errors_by_type = defaultdict(int)
        self.total_errors = 0
    
    def record_error(self, error_type: str):
        """Записать ошибку"""
        self.errors_by_type[error_type] += 1
        self.total_errors += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """Получить статистику ошибок"""
        return {
            'total_errors': self.total_errors,
            'errors_by_type': dict(self.errors_by_type)
        }

# Экспорт основных функций
__all__ = [
    'DataParser',
    'FastDataParser',
    'parse_single_record',
    'parse_batch_records',
    'get_global_parser',
    'is_valid_json_line',
    'extract_key_fields_fast',
    'ParseErrorCounter'
]

# Инициализация глобального парсера при импорте
get_global_parser()