"""
Менеджер чекпоинтов для возобновления обработки с улучшенной надежностью
"""

import os
import json
import time
import shutil
import hashlib
import logging
from typing import Dict, Any, Optional, List, Set, Tuple
from dataclasses import dataclass, asdict, field, fields, is_dataclass
from datetime import datetime, timedelta
from pathlib import Path

# Используем относительные импорты
from .config import Config

# Настройка логгера
logger = logging.getLogger(__name__)


@dataclass
class CheckpointState:
    """Состояние чекпоинта с валидацией"""
    file_name: str = ""
    total_lines: int = 0
    processed_lines: int = 0
    valid_images: int = 0
    failed_images: int = 0
    json_errors: int = 0
    cached_images: int = 0
    network_errors: int = 0
    timeout_errors: int = 0
    duplicate_records: int = 0
    last_position: int = 0  # Позиция в файле (байты)
    timestamp: float = field(default_factory=time.time)
    batch_size: int = field(default_factory=lambda: Config.INITIAL_BATCH_SIZE)
    records_processed: List[str] = field(default_factory=list)
    unique_users: List[str] = field(default_factory=list)
    unique_devices: List[str] = field(default_factory=list)
    unique_companies: List[str] = field(default_factory=list)
    unique_ips: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        """Инициализация после создания объекта"""
        # Нормализация числовых полей
        self._normalize_numeric_fields()
        # Валидация данных
        self._validate_data()
    
    def _normalize_numeric_fields(self):
        """Нормализация числовых полей"""
        # Конвертируем batch_size в int и проверяем границы
        try:
            self.batch_size = int(self.batch_size)
            if self.batch_size < 100:
                self.batch_size = Config.INITIAL_BATCH_SIZE
            elif self.batch_size > 50000:
                self.batch_size = 50000
        except (ValueError, TypeError):
            self.batch_size = Config.INITIAL_BATCH_SIZE
        
        # Обеспечиваем корректность других числовых полей
        numeric_fields = [
            'total_lines', 'processed_lines', 'valid_images', 
            'failed_images', 'json_errors', 'cached_images',
            'network_errors', 'timeout_errors', 'duplicate_records',
            'last_position'
        ]
        
        for field_name in numeric_fields:
            value = getattr(self, field_name, 0)
            try:
                setattr(self, field_name, int(float(value)))
            except (ValueError, TypeError):
                setattr(self, field_name, 0)
        
        # Обеспечиваем что timestamp - float
        try:
            self.timestamp = float(self.timestamp)
        except (ValueError, TypeError):
            self.timestamp = time.time()
    
    def _validate_data(self):
        """Валидация данных"""
        # Проверка целостности
        if self.processed_lines > self.total_lines > 0:
            logger.warning(f"Обработано строк ({self.processed_lines:,}) > всего строк ({self.total_lines:,})")
            self.processed_lines = min(self.processed_lines, self.total_lines)
        
        if self.last_position < 0:
            logger.warning(f"Некорректная позиция: {self.last_position:,}")
            self.last_position = 0
        
        # Проверка статистики изображений
        total_images = self.valid_images + self.failed_images
        if total_images > self.processed_lines:
            logger.warning(f"Количество изображений ({total_images}) > обработанных строк ({self.processed_lines})")
    
    @property
    def progress_percent(self) -> float:
        """Процент выполнения"""
        if self.total_lines == 0:
            return 0.0
        return (self.processed_lines / self.total_lines) * 100
    
    @property
    def age_seconds(self) -> float:
        """Возраст чекпоинта в секундах"""
        return time.time() - self.timestamp
    
    @property
    def age_hours(self) -> float:
        """Возраст чекпоинта в часах"""
        return self.age_seconds / 3600
    
    def is_expired(self, max_age_hours: float = 168) -> bool:
        """Проверить, не истек ли срок действия чекпоинта (по умолчанию 7 дней)"""
        return self.age_hours > max_age_hours
    
    def to_dict(self) -> Dict[str, Any]:
        """Конвертация в словарь для сериализации"""
        data = {}
        
        for field_info in fields(self):
            value = getattr(self, field_info.name)
            
            # Обрабатываем специальные типы
            if isinstance(value, (set, list)):
                data[field_info.name] = list(value)
            elif is_dataclass(value):
                data[field_info.name] = asdict(value)
            else:
                data[field_info.name] = value
        
        # Добавляем вычисляемые поля
        data['progress_percent'] = self.progress_percent
        data['age_seconds'] = self.age_seconds
        data['age_hours'] = self.age_hours
        data['is_expired'] = self.is_expired()
        
        # Форматируем временные метки
        if self.timestamp > 0:
            dt = datetime.fromtimestamp(self.timestamp)
            data['timestamp_human'] = dt.strftime("%Y-%m-%d %H:%M:%S")
            data['timestamp_iso'] = dt.isoformat()
        
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CheckpointState':
        """Создать объект из словаря"""
        # Фильтруем только поля dataclass
        field_names = {f.name for f in fields(cls)}
        filtered_data = {k: v for k, v in data.items() if k in field_names}
        return cls(**filtered_data)


class CheckpointIntegrityError(Exception):
    """Ошибка целостности чекпоинта"""
    pass


class CheckpointManager:
    """
    Управление чекпоинтами для возобновления обработки
    
    Особенности:
    - Надежное сохранение с атомарными операциями
    - Проверка целостности данных
    - Автоматическое восстановление из резервных копий
    - Контроль версий формата
    - Кэширование для быстрого доступа
    """
    
    # Версия формата чекпоинта
    CHECKPOINT_VERSION = 1
    
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.checkpoint_file = os.path.join(output_dir, Config.CHECKPOINT_FILE)
        self.checkpoint_temp = f"{self.checkpoint_file}.tmp"
        self.checkpoint_backup = f"{self.checkpoint_file}.backup"
        self.checkpoint_archive = f"{self.checkpoint_file}.archive"
        
        self.state: Optional[CheckpointState] = None
        self.last_save = 0.0
        self.save_count = 0
        self.checksum: Optional[str] = None
        
        # Кэш для быстрого доступа
        self._cache: Dict[str, Tuple[CheckpointState, float]] = {}
        self._cache_ttl = 60  # секунд
        
        # Статистика
        self.stats = {
            'loads': 0,
            'saves': 0,
            'backup_restores': 0,
            'integrity_errors': 0,
            'last_operation': None
        }
        
        # Автоматически создаем директорию если нужно
        os.makedirs(os.path.dirname(self.checkpoint_file), exist_ok=True)
        
        logger.info(f"Инициализирован CheckpointManager: {self.checkpoint_file}")
    
    def _update_stats(self, operation: str):
        """Обновить статистику операций"""
        self.stats['last_operation'] = operation
        key = f'{operation}s'
        self.stats[key] = self.stats.get(key, 0) + 1
    
    def _generate_checksum(self, data: Dict[str, Any]) -> str:
        """Генерация контрольной суммы для данных"""
        try:
            # Создаем строку для хэширования
            data_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
            return hashlib.sha256(data_str.encode()).hexdigest()[:32]
        except Exception as e:
            logger.error(f"Ошибка генерации контрольной суммы: {e}")
            return "0" * 32
    
    def _calculate_file_checksum(self, filepath: str) -> Optional[str]:
        """Вычислить контрольную сумму файла"""
        if not os.path.exists(filepath):
            return None
        
        try:
            with open(filepath, 'rb') as f:
                file_hash = hashlib.sha256()
                # Читаем файл блоками для экономии памяти
                for chunk in iter(lambda: f.read(4096), b""):
                    file_hash.update(chunk)
                return file_hash.hexdigest()
        except Exception as e:
            logger.warning(f"Ошибка вычисления контрольной суммы файла {filepath}: {e}")
            return None
    
    def validate_checkpoint_integrity(self, checkpoint_data: Dict[str, Any]) -> bool:
        """Проверить целостность данных чекпоинта"""
        try:
            required_fields = {
                'file_name', 'total_lines', 'processed_lines', 
                'last_position', 'timestamp', 'batch_size'
            }
            
            # Проверка наличия обязательных полей
            missing_fields = required_fields - set(checkpoint_data.keys())
            if missing_fields:
                logger.warning(f"Чекпоинт отсутствуют обязательные поля: {missing_fields}")
                return False
            
            # Проверка типов данных
            type_checks = [
                ('processed_lines', (int, float)),
                ('total_lines', (int, float)),
                ('last_position', (int, float)),
                ('batch_size', (int, float)),
                ('timestamp', (int, float)),
            ]
            
            for field_name, expected_types in type_checks:
                value = checkpoint_data.get(field_name)
                if not isinstance(value, expected_types):
                    logger.warning(f"Некорректный тип {field_name}: {type(value)}")
                    return False
            
            # Проверка логической целостности
            if checkpoint_data['processed_lines'] > checkpoint_data['total_lines']:
                logger.warning(f"Обработано строк ({checkpoint_data['processed_lines']:,}) > всего строк ({checkpoint_data['total_lines']:,})")
                return False
            
            if checkpoint_data['last_position'] < 0:
                logger.warning(f"Некорректная позиция: {checkpoint_data['last_position']:,}")
                return False
            
            # Проверка возраста чекпоинта (предупреждение для старых)
            checkpoint_age = time.time() - checkpoint_data['timestamp']
            if checkpoint_age > 30 * 24 * 3600:  # 30 дней
                logger.warning(f"Чекпоинт очень стар: {checkpoint_age/3600/24:.1f} дней")
            elif checkpoint_age > 7 * 24 * 3600:  # 7 дней
                logger.info(f"Чекпоинт стар: {checkpoint_age/3600/24:.1f} дней")
            
            # Проверка размера батча в разумных пределах
            try:
                batch_size = int(checkpoint_data['batch_size'])
                if not (100 <= batch_size <= 50000):
                    logger.warning(f"Некорректный размер батча: {batch_size:,}")
                    return False
            except (ValueError, TypeError):
                logger.warning(f"Некорректный тип размера батча")
                return False
            
            # Проверка контрольной суммы если есть
            if 'checksum' in checkpoint_data:
                data_copy = checkpoint_data.copy()
                saved_checksum = data_copy.pop('checksum')
                calculated_checksum = self._generate_checksum(data_copy)
                
                if saved_checksum != calculated_checksum:
                    logger.warning("Контрольная сумма не совпадает")
                    self.stats['integrity_errors'] += 1
                    return False
            
            logger.debug(f"Чекпоинт прошел проверку целостности")
            return True
            
        except (TypeError, KeyError, ValueError) as e:
            logger.warning(f"Ошибка валидации чекпоинта: {e}")
            self.stats['integrity_errors'] += 1
            return False
    
    def _safe_json_load(self, filepath: str) -> Optional[Dict[str, Any]]:
        """Безопасная загрузка JSON с обработкой ошибок"""
        if not os.path.exists(filepath):
            return None
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка декодирования JSON в файле {filepath}: {e}")
            
            # Пытаемся восстановить файл
            try:
                backup_content = self._try_recover_json(filepath)
                if backup_content:
                    logger.info(f"Успешно восстановлен JSON из {filepath}")
                    return backup_content
            except Exception as recovery_error:
                logger.error(f"Ошибка восстановления JSON: {recovery_error}")
            
            return None
        except UnicodeDecodeError as e:
            logger.error(f"Ошибка кодировки в файле {filepath}: {e}")
            return None
        except Exception as e:
            logger.error(f"Неожиданная ошибка загрузки {filepath}: {e}")
            return None
    
    def _try_recover_json(self, filepath: str) -> Optional[Dict[str, Any]]:
        """Попытка восстановления поврежденного JSON файла"""
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Пытаемся найти и извлечь JSON
            start_idx = content.find('{')
            end_idx = content.rfind('}')
            
            if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                json_str = content[start_idx:end_idx + 1]
                return json.loads(json_str)
            
            return None
        except Exception as e:
            logger.debug(f"Восстановление JSON не удалось: {e}")
            return None
    
    def load_checkpoint(self) -> Optional[CheckpointState]:
        """Загрузить состояние чекпоинта"""
        cache_key = f"checkpoint_{self.checkpoint_file}"
        current_time = time.time()
        
        # Проверка кэша
        if cache_key in self._cache:
            cached_data, timestamp = self._cache[cache_key]
            if current_time - timestamp < self._cache_ttl:
                logger.debug("Загружено из кэша")
                self.state = cached_data
                self._update_stats('load')
                return self.state
        
        # Очистка кэша
        self._cache.clear()
        
        logger.info(f"Загрузка чекпоинта из {self.checkpoint_file}")
        
        # Пытаемся загрузить из основного файла
        if os.path.exists(self.checkpoint_file):
            try:
                data = self._safe_json_load(self.checkpoint_file)
                if data is None:
                    logger.warning(f"Не удалось загрузить основной файл, пробую резервную копию")
                    return self._load_backup_checkpoint()
                
                # Проверяем версию формата
                if data.get('version', 0) != self.CHECKPOINT_VERSION:
                    logger.warning(f"Несовместимая версия чекпоинта: {data.get('version')}")
                    # Можем попробовать конвертировать, но пока просто пропускаем
                    return self._load_backup_checkpoint()
                
                # Валидация целостности данных
                if not self.validate_checkpoint_integrity(data):
                    logger.warning("Проверка целостности данных чекпоинта не пройдена")
                    return self._load_backup_checkpoint()
                
                # Создаем объект состояния
                self.state = CheckpointState.from_dict(data)
                
                # Сохраняем контрольную сумму
                self.checksum = data.get('checksum')
                
                # Сохраняем в кэш
                self._cache[cache_key] = (self.state, current_time)
                
                # Обновляем статистику
                self._update_stats('load')
                
                # Логируем успешную загрузку
                logger.info(f"Загружен чекпоинт: обработано {self.state.processed_lines:,} из {self.state.total_lines:,} записей")
                logger.info(f"Последняя позиция: {self.state.last_position:,} байт")
                logger.info(f"Размер батча: {self.state.batch_size:,}")
                logger.info(f"Прогресс: {self.state.progress_percent:.1f}%")
                
                # Проверяем срок действия
                if self.state.is_expired():
                    logger.warning(f"Чекпоинт устарел: {self.state.age_hours:.1f} часов")
                
                return self.state
                
            except Exception as e:
                logger.error(f"Ошибка загрузки основного чекпоинта: {e}")
                return self._load_backup_checkpoint()
        
        # Файл не существует
        logger.info("Чекпоинт не найден")
        return None
    
    def _load_backup_checkpoint(self) -> Optional[CheckpointState]:
        """Загрузить чекпоинт из резервной копии"""
        if not os.path.exists(self.checkpoint_backup):
            logger.info("Резервная копия чекпоинта не найдена")
            return None
        
        logger.info(f"Загрузка резервной копии чекпоинта из {self.checkpoint_backup}")
        
        try:
            data = self._safe_json_load(self.checkpoint_backup)
            if data is None:
                logger.error("Не удалось загрузить резервную копию")
                return None
            
            # Валидация целостности данных
            if not self.validate_checkpoint_integrity(data):
                logger.warning("Резервная копия не прошла проверку целостности")
                return None
            
            # Создаем объект состояния
            self.state = CheckpointState.from_dict(data)
            self.checksum = data.get('checksum')
            
            # Обновляем статистику
            self.stats['backup_restores'] += 1
            self._update_stats('load')
            
            logger.info(f"Загружен резервный чекпоинт: обработано {self.state.processed_lines:,} записей")
            
            # Восстанавливаем основной файл из резервной копии
            try:
                self._atomic_save(self.checkpoint_backup, self.checkpoint_file)
                logger.info("Основной файл чекпоинта восстановлен из резервной копии")
            except Exception as e:
                logger.error(f"Не удалось восстановить основной файл чекпоинта: {e}")
            
            return self.state
            
        except Exception as e:
            logger.error(f"Ошибка загрузки резервного чекпоинта: {e}")
            return None
    
    def _atomic_save(self, source: str, destination: str):
        """Атомарное сохранение файла"""
        try:
            # Создаем временный файл
            temp_file = f"{destination}.atomic.tmp"
            
            # Копируем файл
            shutil.copy2(source, temp_file)
            
            # Проверяем, что файл успешно скопирован
            if os.path.exists(temp_file):
                dest_size = os.path.getsize(temp_file)
                src_size = os.path.getsize(source)
                
                if dest_size == src_size:
                    # Атомарно переименовываем
                    os.replace(temp_file, destination)
                    logger.debug(f"Файл успешно сохранен: {dest_size} байт")
                else:
                    os.remove(temp_file)
                    raise IOError(f"Размеры файлов не совпадают: {src_size} != {dest_size}")
            else:
                raise IOError("Временный файл не создан")
                
        except Exception as e:
            logger.error(f"Ошибка атомарного сохранения {source} -> {destination}: {e}")
            raise
    
    def save_checkpoint(self,
                       file_name: str,
                       total_lines: int,
                       processed_lines: int,
                       valid_images: int,
                       failed_images: int,
                       json_errors: int,
                       cached_images: int,
                       network_errors: int,
                       timeout_errors: int,
                       duplicate_records: int,
                       last_position: int,
                       batch_size: int,
                       records_processed: list,
                       unique_users: list,
                       unique_devices: list,
                       unique_companies: list,
                       unique_ips: list) -> bool:
        """
        Сохранить состояние чекпоинта
        
        Сохраняет чекпоинт при выполнении одного из условий:
        1. Раз в 60 секунд
        2. Каждые CHECKPOINT_INTERVAL записей
        3. При завершении обработки
        
        Returns:
            bool: True если сохранение успешно, False в случае ошибки
        """
        current_time = time.time()
        
        # Проверяем, нужно ли сохранять
        time_condition = current_time - self.last_save >= 60
        records_condition = False
        
        if self.state:
            records_since_last = processed_lines - self.state.processed_lines
            records_condition = records_since_last >= Config.CHECKPOINT_INTERVAL
        
        completion_condition = processed_lines >= total_lines and total_lines > 0
        
        if not (time_condition or records_condition or completion_condition):
            return False
        
        # Подготовка данных
        checkpoint_data = {
            'version': self.CHECKPOINT_VERSION,
            'file_name': file_name,
            'total_lines': total_lines,
            'processed_lines': processed_lines,
            'valid_images': valid_images,
            'failed_images': failed_images,
            'json_errors': json_errors,
            'cached_images': cached_images,
            'network_errors': network_errors,
            'timeout_errors': timeout_errors,
            'duplicate_records': duplicate_records,
            'last_position': last_position,
            'timestamp': current_time,
            'batch_size': batch_size,
            'records_processed': records_processed,
            'unique_users': unique_users,
            'unique_devices': unique_devices,
            'unique_companies': unique_companies,
            'unique_ips': unique_ips,
        }
        
        # Добавляем контрольную сумму
        checksum = self._generate_checksum(checkpoint_data)
        checkpoint_data['checksum'] = checksum
        
        try:
            # Шаг 1: Сохраняем во временный файл
            with open(self.checkpoint_temp, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f, indent=2, ensure_ascii=False, default=str)
            
            # Шаг 2: Создаем резервную копию текущего чекпоинта (если есть)
            if os.path.exists(self.checkpoint_file):
                try:
                    # Создаем архивную копию предыдущего чекпоинта
                    if os.path.exists(self.checkpoint_backup):
                        try:
                            shutil.copy2(self.checkpoint_backup, self.checkpoint_archive)
                        except Exception:
                            pass
                    
                    # Обновляем резервную копию
                    shutil.copy2(self.checkpoint_file, self.checkpoint_backup)
                    logger.debug("Создана резервная копия чекпоинта")
                except Exception as e:
                    logger.warning(f"Не удалось создать резервную копию: {e}")
            
            # Шаг 3: Атомарно перемещаем временный файл в основной
            self._atomic_save(self.checkpoint_temp, self.checkpoint_file)
            
            # Шаг 4: Очищаем временный файл
            if os.path.exists(self.checkpoint_temp):
                try:
                    os.remove(self.checkpoint_temp)
                except Exception:
                    pass
            
            # Обновляем состояние
            self.state = CheckpointState.from_dict(checkpoint_data)
            self.checksum = checksum
            self.last_save = current_time
            self.save_count += 1
            
            # Очищаем кэш
            self._cache.clear()
            
            # Обновляем статистику
            self._update_stats('save')
            
            # Логируем сохранение
            if completion_condition:
                logger.info(f"💾 Финальный чекпоинт сохранен: {processed_lines:,} из {total_lines:,} записей")
            elif records_condition:
                logger.info(f"💾 Чекпоинт сохранен (каждые {Config.CHECKPOINT_INTERVAL:,}): {processed_lines:,} записей")
            elif time_condition:
                logger.debug(f"💾 Автосохранение (каждые 60 сек): {processed_lines:,} записей")
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка сохранения чекпоинта: {e}")
            
            # Пытаемся очистить временный файл в случае ошибки
            if os.path.exists(self.checkpoint_temp):
                try:
                    os.remove(self.checkpoint_temp)
                except Exception:
                    pass
            
            return False
    
    def clear_checkpoint(self) -> int:
        """Очистить все файлы чекпоинта"""
        files_to_remove = [
            self.checkpoint_file,
            self.checkpoint_backup,
            self.checkpoint_temp,
            self.checkpoint_archive
        ]
        
        removed_count = 0
        for file_path in files_to_remove:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    removed_count += 1
                    logger.debug(f"Удален файл чекпоинта: {file_path}")
                except Exception as e:
                    logger.error(f"Не удалось удалить файл {file_path}: {e}")
        
        # Сбрасываем состояние
        self.state = None
        self.checksum = None
        self.last_save = 0.0
        self._cache.clear()
        
        if removed_count > 0:
            logger.info(f"Очищено {removed_count} файлов чекпоинта")
        
        return removed_count
    
    def should_save_checkpoint(self, processed_since_last: int) -> bool:
        """Проверить, нужно ли сохранять чекпоинт"""
        return processed_since_last >= Config.CHECKPOINT_INTERVAL
    
    def get_checkpoint_info(self) -> Dict[str, Any]:
        """Получить подробную информацию о чекпоинте"""
        if not self.state:
            return {
                "exists": False,
                "file_path": self.checkpoint_file,
                "backup_exists": os.path.exists(self.checkpoint_backup)
            }
        
        info = self.state.to_dict()
        info["exists"] = True
        info["file_path"] = self.checkpoint_file
        info["backup_exists"] = os.path.exists(self.checkpoint_backup)
        info["save_count"] = self.save_count
        info["checksum"] = self.checksum
        
        # Добавляем информацию о файлах
        if os.path.exists(self.checkpoint_file):
            try:
                info["file_size"] = os.path.getsize(self.checkpoint_file)
                info["file_mtime"] = os.path.getmtime(self.checkpoint_file)
                info["file_ctime"] = os.path.getctime(self.checkpoint_file)
            except Exception:
                pass
        
        return info
    
    def get_progress_info(self) -> Dict[str, Any]:
        """Получить информацию о прогрессе обработки"""
        if not self.state:
            return {"has_checkpoint": False}
        
        info = self.get_checkpoint_info()
        info["has_checkpoint"] = True
        
        # Добавляем информацию о скорости
        if self.state.timestamp > 0 and self.state.processed_lines > 0:
            elapsed_hours = self.state.age_hours
            
            if elapsed_hours > 0:
                records_per_hour = self.state.processed_lines / elapsed_hours
                info["records_per_hour"] = int(records_per_hour)
                info["elapsed_hours"] = round(elapsed_hours, 1)
                
                # Прогноз времени завершения
                if self.state.total_lines > 0:
                    remaining = self.state.total_lines - self.state.processed_lines
                    if records_per_hour > 0:
                        hours_remaining = remaining / records_per_hour
                        info["hours_remaining"] = round(hours_remaining, 1)
                        info["eta_timestamp"] = time.time() + hours_remaining * 3600
        
        return info
    
    def validate_checkpoint(self, input_file: str) -> Tuple[bool, str]:
        """
        Проверить валидность чекпоинта для текущего файла
        
        Returns:
            Tuple[bool, str]: (Валиден ли чекпоинт, Сообщение об ошибке)
        """
        if not self.state:
            return False, "Чекпоинт не загружен"
        
        # Проверяем, что чекпоинт для того же файла
        if self.state.file_name != os.path.basename(input_file):
            message = f"Чекпоинт для другого файла: {self.state.file_name} != {os.path.basename(input_file)}"
            logger.warning(message)
            return False, message
        
        # Проверяем, что файл существует
        if not os.path.exists(input_file):
            message = "Входной файл не существует"
            logger.warning(message)
            return False, message
        
        # Проверяем позицию в файле
        try:
            file_size = os.path.getsize(input_file)
            
            # Допускаем небольшую погрешность в позиции (1KB)
            if self.state.last_position > file_size + 1024:
                message = f"Некорректная позиция в чекпоинте: {self.state.last_position:,} > {file_size:,}"
                logger.warning(message)
                return False, message
            
            # Если позиция близка к концу файла, считаем обработку завершенной
            if file_size - self.state.last_position < 1024:  # Меньше 1KB осталось
                logger.info(f"Файл почти полностью обработан, позиция: {self.state.last_position:,} из {file_size:,}")
        
        except OSError as e:
            message = f"Ошибка проверки размера файла: {e}"
            logger.warning(message)
            return False, message
        
        # Проверяем количество строк
        if self.state.total_lines < self.state.processed_lines:
            message = f"Некорректное количество строк: {self.state.processed_lines:,} > {self.state.total_lines:,}"
            logger.warning(message)
            return False, message
        
        # Проверяем срок действия
        if self.state.is_expired(max_age_hours=168):  # 7 дней
            message = f"Чекпоинт устарел: {self.state.age_hours:.1f} часов"
            logger.warning(message)
            return False, message
        
        logger.info(f"Чекпоинт валиден для файла {input_file}")
        return True, "Чекпоинт валиден"
    
    def archive_old_checkpoint(self, max_age_days: int = 30) -> bool:
        """Архивировать старый чекпоинт"""
        if not self.state:
            return False
        
        if self.state.age_hours <= max_age_days * 24:
            return False
        
        try:
            archive_dir = os.path.join(self.output_dir, "checkpoint_archive")
            os.makedirs(archive_dir, exist_ok=True)
            
            timestamp = datetime.fromtimestamp(self.state.timestamp).strftime("%Y%m%d_%H%M%S")
            archive_name = f"checkpoint_{self.state.file_name}_{timestamp}.json"
            archive_path = os.path.join(archive_dir, archive_name)
            
            # Копируем чекпоинт в архив
            if os.path.exists(self.checkpoint_file):
                shutil.copy2(self.checkpoint_file, archive_path)
                logger.info(f"Чекпоинт архивирован: {archive_path}")
                return True
        
        except Exception as e:
            logger.error(f"Ошибка архивации чекпоинта: {e}")
        
        return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """Получить статистику работы менеджера чекпоинтов"""
        stats = self.stats.copy()
        
        # Добавляем информацию о файлах
        stats['checkpoint_exists'] = os.path.exists(self.checkpoint_file)
        stats['backup_exists'] = os.path.exists(self.checkpoint_backup)
        stats['archive_exists'] = os.path.exists(self.checkpoint_archive)
        
        if os.path.exists(self.checkpoint_file):
            try:
                stats['checkpoint_size'] = os.path.getsize(self.checkpoint_file)
                stats['checkpoint_mtime'] = os.path.getmtime(self.checkpoint_file)
            except Exception:
                pass
        
        # Добавляем информацию о текущем состоянии
        if self.state:
            stats['current_state'] = {
                'processed_lines': self.state.processed_lines,
                'total_lines': self.state.total_lines,
                'progress_percent': self.state.progress_percent,
                'age_hours': self.state.age_hours,
                'batch_size': self.state.batch_size
            }
        
        stats['save_count'] = self.save_count
        stats['cache_size'] = len(self._cache)
        
        return stats
    
    def cleanup_temp_files(self):
        """Очистка временных файлов"""
        temp_files = [self.checkpoint_temp]
        
        for file_path in temp_files:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    logger.debug(f"Очищен временный файл: {file_path}")
                except Exception as e:
                    logger.debug(f"Не удалось очистить временный файл {file_path}: {e}")
    
    def __del__(self):
        """Деструктор - очистка временных файлов при удалении объекта"""
        try:
            self.cleanup_temp_files()
        except Exception:
            pass


# Утилиты для работы с чекпоинтами
class CheckpointUtils:
    """Утилиты для работы с чекпоинтами"""
    
    @staticmethod
    def scan_for_checkpoints(directory: str) -> List[Dict[str, Any]]:
        """Сканировать директорию на наличие чекпоинтов"""
        checkpoints = []
        
        try:
            for root, dirs, files in os.walk(directory):
                for file in files:
                    if file == Config.CHECKPOINT_FILE:
                        filepath = os.path.join(root, file)
                        try:
                            data = CheckpointUtils._safe_read_json(filepath)
                            if data:
                                checkpoints.append({
                                    'path': filepath,
                                    'directory': root,
                                    'file_name': data.get('file_name', ''),
                                    'processed_lines': data.get('processed_lines', 0),
                                    'total_lines': data.get('total_lines', 0),
                                    'timestamp': data.get('timestamp', 0),
                                    'progress_percent': (data.get('processed_lines', 0) / data.get('total_lines', 1) * 100) if data.get('total_lines', 0) > 0 else 0
                                })
                        except Exception as e:
                            logger.debug(f"Ошибка чтения чекпоинта {filepath}: {e}")
        
        except Exception as e:
            logger.error(f"Ошибка сканирования чекпоинтов: {e}")
        
        return checkpoints
    
    @staticmethod
    def _safe_read_json(filepath: str) -> Optional[Dict[str, Any]]:
        """Безопасное чтение JSON файла"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return None
    
    @staticmethod
    def merge_checkpoints(checkpoints: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Объединить несколько чекпоинтов"""
        if not checkpoints:
            return None
        
        # Сортируем по времени (самый свежий первый)
        checkpoints.sort(key=lambda x: x.get('timestamp', 0), reverse=True)
        
        # Берем самый свежий валидный чекпоинт
        for checkpoint in checkpoints:
            if checkpoint.get('processed_lines', 0) > 0:
                return checkpoint
        
        return None


# Фабричный метод для создания менеджера чекпоинтов
def create_checkpoint_manager(output_dir: str) -> CheckpointManager:
    """Создать менеджер чекпоинтов с предварительной проверкой"""
    # Создаем директорию если нужно
    os.makedirs(output_dir, exist_ok=True)
    
    # Создаем менеджер
    manager = CheckpointManager(output_dir)
    
    # Сканируем на наличие старых чекпоинтов
    checkpoints = CheckpointUtils.scan_for_checkpoints(output_dir)
    if len(checkpoints) > 1:
        logger.info(f"Найдено {len(checkpoints)} чекпоинтов в директории")
    
    return manager