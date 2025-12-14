"""
Менеджер чекпоинтов для возобновления обработки
"""

import os
import json
import time
import shutil
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict, field, fields
from core.config import Config

@dataclass
class CheckpointState:
    """Состояние чекпоинта"""
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
    last_position: int = 0  # Позиция в файле
    timestamp: float = 0.0
    batch_size: int = Config.INITIAL_BATCH_SIZE
    records_processed: List[str] = field(default_factory=list)
    unique_users: List[str] = field(default_factory=list)
    unique_devices: List[str] = field(default_factory=list)
    unique_companies: List[str] = field(default_factory=list)
    unique_ips: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        """Инициализация после создания объекта"""
        # Конвертируем batch_size в int и проверяем границы
        try:
            self.batch_size = int(self.batch_size)
            if self.batch_size < 100:
                self.batch_size = Config.INITIAL_BATCH_SIZE
            elif self.batch_size > 50000:
                self.batch_size = 50000
        except (ValueError, TypeError):
            self.batch_size = Config.INITIAL_BATCH_SIZE

class CheckpointManager:
    """Управление чекпоинтами для возобновления обработки"""
    
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.checkpoint_file = os.path.join(output_dir, Config.CHECKPOINT_FILE)
        self.checkpoint_temp = self.checkpoint_file + ".tmp"
        self.checkpoint_backup = self.checkpoint_file + ".backup"
        self.state: Optional[CheckpointState] = None
        self.last_save = 0.0
    
    def validate_checkpoint_integrity(self, checkpoint_data: dict) -> bool:
        """Проверить целостность данных чекпоинта"""
        required_fields = {
            'file_name', 'total_lines', 'processed_lines', 
            'last_position', 'timestamp', 'batch_size'
        }
        
        # Проверка наличия обязательных полей
        if not all(field in checkpoint_data for field in required_fields):
            print("⚠️ Чекпоинт отсутствуют обязательные поля")
            return False
            
        # Проверка типов данных
        try:
            if not isinstance(checkpoint_data['processed_lines'], (int, float)):
                print("⚠️ Некорректный тип processed_lines")
                return False
            if checkpoint_data['processed_lines'] > checkpoint_data['total_lines']:
                print(f"⚠️ Обработано строк ({checkpoint_data['processed_lines']:,}) > всего строк ({checkpoint_data['total_lines']:,})")
                return False
            if checkpoint_data['last_position'] < 0:
                print(f"⚠️ Некорректная позиция: {checkpoint_data['last_position']:,}")
                return False
                
            # Проверка возраста чекпоинта (не старше 7 дней)
            checkpoint_age = time.time() - checkpoint_data['timestamp']
            if checkpoint_age > 7 * 24 * 3600:
                print(f"⚠️ Чекпоинт слишком стар: {checkpoint_age/3600:.1f} часов")
                # Не возвращаем False, но предупреждаем
                
            # Проверка размера батча в разумных пределах
            if not (100 <= checkpoint_data['batch_size'] <= 50000):
                print(f"⚠️ Некорректный размер батча: {checkpoint_data['batch_size']:,}")
                checkpoint_data['batch_size'] = Config.INITIAL_BATCH_SIZE
                
        except (TypeError, KeyError, ValueError) as e:
            print(f"⚠️ Ошибка валидации чекпоинта: {e}")
            return False
            
        return True

    def load_checkpoint(self) -> Optional[CheckpointState]:
        """Загрузить состояние чекпоинта"""
        # Пробуем загрузить из основного файла
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Валидация целостности данных
                if not self.validate_checkpoint_integrity(data):
                    print("⚠️ Проверка целостности данных чекпоинта не пройдена")
                    return self._load_backup_checkpoint()
                
                # Фильтруем только поля, которые есть в CheckpointState
                checkpoint_fields = {f.name for f in fields(CheckpointState)}
                filtered_data = {k: v for k, v in data.items() if k in checkpoint_fields}
                
                self.state = CheckpointState(**filtered_data)
                print(f"📌 Найден чекпоинт: обработано {self.state.processed_lines:,} из {self.state.total_lines:,} записей")
                print(f"📌 Последняя позиция: {self.state.last_position:,} байт")
                print(f"📌 Размер батча: {self.state.batch_size:,}")
                return self.state
                
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                print(f"⚠️ Ошибка загрузки основного чекпоинта: {e}")
                return self._load_backup_checkpoint()
        
        return self._load_backup_checkpoint()
    
    def _load_backup_checkpoint(self) -> Optional[CheckpointState]:
        """Загрузить чекпоинт из резервной копии"""
        if not os.path.exists(self.checkpoint_backup):
            return None
        
        try:
            with open(self.checkpoint_backup, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Фильтруем только поля, которые есть в CheckpointState
            checkpoint_fields = {f.name for f in fields(CheckpointState)}
            filtered_data = {k: v for k, v in data.items() if k in checkpoint_fields}
            
            self.state = CheckpointState(**filtered_data)
            print(f"📌 Загружен резервный чекпоинт: обработано {self.state.processed_lines:,} записей")
            
            # Восстанавливаем основной файл из резервной копии
            try:
                shutil.copy2(self.checkpoint_backup, self.checkpoint_file)
                print("✅ Основной файл чекпоинта восстановлен из резервной копии")
            except Exception as e:
                print(f"⚠️ Не удалось восстановить основной файл чекпоинта: {e}")
            
            return self.state
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            print(f"⚠️ Ошибка загрузки резервного чекпоинта: {e}")
            return None
    
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
                       unique_ips: list):
        """Сохранить состояние чекпоинта"""
        current_time = time.time()
        
        # Проверяем, нужно ли сохранять
        # 1. Раз в 60 секунд
        # 2. Каждые CHECKPOINT_INTERVAL записей
        # 3. Всегда при завершении (processed_lines == total_lines)
        
        time_condition = current_time - self.last_save >= 60
        records_condition = (processed_lines - self.state.processed_lines if self.state else processed_lines) >= Config.CHECKPOINT_INTERVAL
        completion_condition = processed_lines >= total_lines and total_lines > 0
        
        if not (time_condition or records_condition or completion_condition):
            return
        
        # Проверяем, что batch_size в допустимых пределах
        if batch_size < 100:
            batch_size = Config.INITIAL_BATCH_SIZE
        elif batch_size > 50000:
            batch_size = 50000
        
        # Преобразуем set в list для сериализации JSON
        if isinstance(records_processed, set):
            records_processed = list(records_processed)
        if isinstance(unique_users, set):
            unique_users = list(unique_users)
        if isinstance(unique_devices, set):
            unique_devices = list(unique_devices)
        if isinstance(unique_companies, set):
            unique_companies = list(unique_companies)
        if isinstance(unique_ips, set):
            unique_ips = list(unique_ips)
        
        self.state = CheckpointState(
            file_name=file_name,
            total_lines=total_lines,
            processed_lines=processed_lines,
            valid_images=valid_images,
            failed_images=failed_images,
            json_errors=json_errors,
            cached_images=cached_images,
            network_errors=network_errors,
            timeout_errors=timeout_errors,
            duplicate_records=duplicate_records,
            last_position=last_position,
            timestamp=current_time,
            batch_size=batch_size,
            records_processed=records_processed,
            unique_users=unique_users,
            unique_devices=unique_devices,
            unique_companies=unique_companies,
            unique_ips=unique_ips
        )
        
        try:
            # Сначала сохраняем во временный файл
            with open(self.checkpoint_temp, 'w', encoding='utf-8') as f:
                json.dump(asdict(self.state), f, indent=2, ensure_ascii=False)
            
            # Затем создаем резервную копию текущего чекпоинта (если есть)
            if os.path.exists(self.checkpoint_file):
                try:
                    shutil.copy2(self.checkpoint_file, self.checkpoint_backup)
                except Exception as e:
                    print(f"⚠️ Не удалось создать резервную копию: {e}")
            
            # Перемещаем временный файл в основной
            shutil.move(self.checkpoint_temp, self.checkpoint_file)
            
            self.last_save = current_time
            
            # Выводим информацию о сохранении
            if completion_condition:
                print(f"💾 Финальный чекпоинт сохранен: {processed_lines:,} из {total_lines:,} записей")
            elif records_condition:
                print(f"💾 Чекпоинт сохранен (каждые {Config.CHECKPOINT_INTERVAL:,}): {processed_lines:,} записей")
            elif time_condition:
                print(f"💾 Автосохранение (каждые 60 сек): {processed_lines:,} записей")
                
        except Exception as e:
            print(f"⚠️ Ошибка сохранения чекпоинта: {e}")
            # Пробуем очистить временный файл
            if os.path.exists(self.checkpoint_temp):
                try:
                    os.remove(self.checkpoint_temp)
                except:
                    pass
    
    def clear_checkpoint(self):
        """Очистить чекпоинт"""
        files_to_remove = [
            self.checkpoint_file,
            self.checkpoint_backup,
            self.checkpoint_temp
        ]
        
        for file_path in files_to_remove:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as e:
                    print(f"⚠️ Не удалось удалить файл {file_path}: {e}")
    
    def should_save_checkpoint(self, processed_since_last: int) -> bool:
        """Проверить, нужно ли сохранять чекпоинт"""
        return processed_since_last >= Config.CHECKPOINT_INTERVAL
    
    def get_checkpoint_info(self) -> Dict[str, Any]:
        """Получить информацию о чекпоинте"""
        if not self.state:
            return {"exists": False}
        
        progress_percent = 0
        if self.state.total_lines > 0:
            progress_percent = (self.state.processed_lines / self.state.total_lines * 100)
        
        return {
            "exists": True,
            "file_name": self.state.file_name,
            "processed_lines": self.state.processed_lines,
            "total_lines": self.state.total_lines,
            "progress_percent": progress_percent,
            "last_position": self.state.last_position,
            "timestamp": self.state.timestamp,
            "age_seconds": time.time() - self.state.timestamp,
            "batch_size": self.state.batch_size,
            "unique_records": len(self.state.records_processed),
            "unique_users": len(self.state.unique_users),
            "unique_devices": len(self.state.unique_devices),
            "unique_companies": len(self.state.unique_companies),
            "unique_ips": len(self.state.unique_ips)
        }
    
    def get_progress_info(self) -> Dict[str, Any]:
        """Получить информацию о прогрессе обработки"""
        if not self.state:
            return {"has_checkpoint": False}
        
        info = self.get_checkpoint_info()
        info["has_checkpoint"] = True
        
        # Добавляем информацию о скорости (если есть timestamp)
        if self.state.timestamp > 0:
            elapsed_hours = (time.time() - self.state.timestamp) / 3600
            if elapsed_hours > 0 and self.state.processed_lines > 0:
                records_per_hour = self.state.processed_lines / elapsed_hours
                info["records_per_hour"] = int(records_per_hour)
                info["elapsed_hours"] = round(elapsed_hours, 1)
        
        return info
    
    def validate_checkpoint(self, input_file: str) -> bool:
        """Проверить валидность чекпоинта для текущего файла"""
        if not self.state:
            return False
        
        # Проверяем, что чекпоинт для того же файла
        if self.state.file_name != os.path.basename(input_file):
            print(f"⚠️ Чекпоинт для другого файла: {self.state.file_name} != {os.path.basename(input_file)}")
            return False
        
        # Проверяем, что файл существует и размер не изменился кардинально
        if not os.path.exists(input_file):
            print("⚠️ Входной файл не существует")
            return False
        
        # Проверяем позицию
        file_size = os.path.getsize(input_file)
        if self.state.last_position > file_size + 100:  # Допуск 100 байт
            print(f"⚠️ Некорректная позиция в чекпоинте: {self.state.last_position:,} > {file_size:,}")
            return False
        
        # Проверяем количество строк
        if self.state.total_lines < self.state.processed_lines:
            print(f"⚠️ Некорректное количество строк: {self.state.processed_lines:,} > {self.state.total_lines:,}")
            return False
        
        return True