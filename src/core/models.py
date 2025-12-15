"""
Модели данных
"""

import time
import hashlib
import html
import json
from dataclasses import dataclass, field
from typing import Set, Optional, List, Dict, Any
from datetime import datetime

@dataclass
class ProcessingMetrics:
    """Метрики обработки с расширенной статистикой"""
    start_time: float = field(default_factory=time.time)
    total_records: int = 0
    processed_records: int = 0
    valid_images: int = 0
    failed_images: int = 0
    json_errors: int = 0
    cached_images: int = 0
    network_errors: int = 0
    timeout_errors: int = 0
    duplicate_records: int = 0
    unique_users: Set[str] = field(default_factory=set)
    unique_devices: Set[str] = field(default_factory=set)
    unique_companies: Set[str] = field(default_factory=set)
    unique_ips: Set[str] = field(default_factory=set)
    processed_hashes: Set[str] = field(default_factory=set)
    
    # Статистика по времени
    batch_processing_times: List[float] = field(default_factory=list)
    image_processing_times: List[float] = field(default_factory=list)
    
    @property
    def elapsed_time(self) -> float:
        return time.time() - self.start_time
    
    @property
    def success_rate(self) -> float:
        total = self.valid_images + self.failed_images
        return (self.valid_images / total * 100) if total > 0 else 0
    
    @property
    def network_success_rate(self) -> float:
        total_attempts = self.valid_images + self.failed_images + self.network_errors
        if total_attempts == 0:
            return 100.0
        return (self.valid_images / total_attempts) * 100
    
    @property
    def avg_batch_time(self) -> float:
        if not self.batch_processing_times:
            return 0.0
        return sum(self.batch_processing_times) / len(self.batch_processing_times)
    
    @property
    def avg_image_time(self) -> float:
        if not self.image_processing_times:
            return 0.0
        return sum(self.image_processing_times) / len(self.image_processing_times)
    
    def add_batch_time(self, batch_time: float):
        """Добавить время обработки батча"""
        self.batch_processing_times.append(batch_time)
        # Храним только последние 100 значений
        if len(self.batch_processing_times) > 100:
            self.batch_processing_times.pop(0)
    
    def add_image_time(self, image_time: float):
        """Добавить время обработки изображения"""
        self.image_processing_times.append(image_time)
        # Храним только последние 1000 значений
        if len(self.image_processing_times) > 1000:
            self.image_processing_times.pop(0)
    
    def to_dict(self) -> Dict[str, Any]:
        """Конвертация в словарь для JSON"""
        return {
            "start_time": self.start_time,
            "elapsed_time": self.elapsed_time,
            "total_records": self.total_records,
            "processed_records": self.processed_records,
            "valid_images": self.valid_images,
            "failed_images": self.failed_images,
            "json_errors": self.json_errors,
            "cached_images": self.cached_images,
            "network_errors": self.network_errors,
            "timeout_errors": self.timeout_errors,
            "duplicate_records": self.duplicate_records,
            "success_rate": self.success_rate,
            "network_success_rate": self.network_success_rate,
            "unique_users_count": len(self.unique_users),
            "unique_devices_count": len(self.unique_devices),
            "unique_companies_count": len(self.unique_companies),
            "unique_ips_count": len(self.unique_ips),
            "avg_batch_time": self.avg_batch_time,
            "avg_image_time": self.avg_image_time,
            "unique_users": sorted(list(self.unique_users))[:100],  # Ограничиваем для JSON
            "unique_devices": sorted(list(self.unique_devices))[:100],
            "unique_companies": sorted(list(self.unique_companies))[:100],
            "unique_ips": sorted(list(self.unique_ips))[:100],
        }
    
    def to_summary_string(self) -> str:
        """Текстовое представление статистики"""
        summary = [
            f"📊 СТАТИСТИКА ОБРАБОТКИ:",
            f"   ⏱️  Время: {self.elapsed_time:.1f} сек",
            f"   📈 Всего записей: {self.total_records:,}",
            f"   ✅ Обработано: {self.processed_records:,}",
            f"   🖼️  Успешных фото: {self.valid_images:,}",
            f"   ❌ Ошибок загрузки: {self.failed_images:,}",
            f"   📄 JSON ошибок: {self.json_errors:,}",
            f"   💾 Кэшировано: {self.cached_images:,}",
            f"   📡 Сетевых ошибок: {self.network_errors:,}",
            f"   🔄 Дубликатов: {self.duplicate_records:,}",
            f"   👤 Уникальных пользователей: {len(self.unique_users):,}",
            f"   📱 Уникальных устройств: {len(self.unique_devices):,}",
            f"   🏢 Уникальных компаний: {len(self.unique_companies):,}",
            f"   🌐 Уникальных IP: {len(self.unique_ips):,}",
            f"   ⚡ Успешность фото: {self.success_rate:.1f}%",
            f"   📶 Успешность сети: {self.network_success_rate:.1f}%",
            f"   ⏳ Среднее время батча: {self.avg_batch_time:.2f} сек",
            f"   🖼️  Среднее время фото: {self.avg_image_time:.3f} сек",
        ]
        return "\n".join(summary)

@dataclass
class FaceRecord:
    """Запись о распознавании лица с улучшенными полями"""
    timestamp: str
    device_id: str
    user_name: str
    gender: str
    age: str
    score: str
    face_id: str
    company_id: str
    image_url: str
    image_hash: str = ""
    image_path: str = ""
    image_base64: str = ""
    event_type: str = ""
    user_list: str = ""
    ip_address: str = ""
    processing_time: float = 0.0
    image_size_kb: float = 0.0
    failed_reason: str = ""
    thumbnail_path: str = ""
    is_cached: bool = False
    download_time_ms: int = 0
    image_width: int = 0
    image_height: int = 0
    # НОВЫЕ ПОЛЯ для ваших данных
    user_id: str = ""
    frpic_name: str = ""
    request_type: str = ""
    mongo_id: str = ""
    # Добавляем поле group
    group: str = ""
    # ДОБАВЛЯЕМ ПОЛЕ company_type для исправления ошибки
    company_type: str = ""
    
    def __post_init__(self):
        """Инициализация после создания объекта"""
        # Убедимся, что все строковые поля не None и корректно преобразованы
        self.timestamp = self._safe_str(self.timestamp, "Н/Д")
        self.device_id = self._safe_str(self.device_id, "Н/Д")
        self.user_name = self._safe_str(self.user_name, "Н/Д")
        self.gender = self._safe_str(self.gender, "Н/Д")
        self.age = self._safe_str(self.age, "Н/Д")
        self.score = self._safe_str(self.score, "Н/Д")
        self.face_id = self._safe_str(self.face_id, "Н/Д")
        self.company_id = self._safe_str(self.company_id, "Н/Д")
        self.image_url = self._safe_str(self.image_url, "")
        self.event_type = self._safe_str(self.event_type, "")
        self.user_list = self._safe_str(self.user_list, "")
        self.ip_address = self._safe_str(self.ip_address, "Н/Д")
        self.failed_reason = self._safe_str(self.failed_reason, "")
        self.thumbnail_path = self._safe_str(self.thumbnail_path, "")
        
        # Новые поля
        self.user_id = self._safe_str(self.user_id, "")
        self.frpic_name = self._safe_str(self.frpic_name, "")
        self.request_type = self._safe_str(self.request_type, "")
        self.mongo_id = self._safe_str(self.mongo_id, "")
        self.group = self._safe_str(self.group, "")
        # Инициализируем company_type
        self.company_type = self._safe_str(self.company_type, "")
        
        # Генерация хэша изображения если его нет
        if not self.image_hash and self.image_url:
            self.image_hash = hashlib.md5(self.image_url.encode()).hexdigest()
    
    def _safe_str(self, value: Any, default: str = "") -> str:
        """Безопасное преобразование в строку"""
        if value is None:
            return default
        try:
            return str(value).strip()
        except:
            return default
    
    def to_dict(self) -> Dict[str, Any]:
        """Конвертация в словарь для JSON/Excel"""
        return {
            "timestamp": self.timestamp,
            "device_id": self.device_id,
            "user_name": self.user_name,
            "gender": self.gender,
            "age": self.age,
            "score": self.score,
            "face_id": self.face_id,
            "company_id": self.company_id,
            "image_url": self.image_url,
            "image_hash": self.image_hash,
            "image_path": self.image_path,
            "thumbnail_path": self.thumbnail_path,
            "event_type": self.event_type,
            "user_list": self.user_list,
            "ip_address": self.ip_address,
            "processing_time_seconds": self.processing_time,
            "image_size_kb": self.image_size_kb,
            "failed_reason": self.failed_reason,
            "is_cached": self.is_cached,
            "download_time_ms": self.download_time_ms,
            "image_width": self.image_width,
            "image_height": self.image_height,
            "has_image": bool(self.image_base64),
            "image_status": "✅ Успешно" if self.image_base64 else f"❌ {self.failed_reason}" if self.failed_reason else "❌ Не загружено",
            # Новые поля
            "user_id": self.user_id,
            "frpic_name": self.frpic_name,
            "request_type": self.request_type,
            "mongo_id": self.mongo_id,
            "group": self.group,
            # Добавляем company_type
            "company_type": self.company_type
        }
    
    def to_json(self) -> str:
        """Конвертация в JSON строку"""
        return json.dumps(self.to_dict(), ensure_ascii=False)
    
    def to_html_row(self, index: int) -> str:
        """Генерация HTML строки с встроенным изображением или плашкой"""
        # Разбиваем на логические блоки для улучшения читаемости
        row_data = self._prepare_row_data()
        
        # Генерация отдельных компонентов
        number_cell = self._generate_number_cell(index)
        time_cell = self._generate_time_cell()
        device_cell = self._generate_device_cell()
        user_cell = self._generate_user_cell()
        gender_cell = self._generate_gender_cell(row_data)
        age_cell = self._generate_age_cell(row_data)
        score_cell = self._generate_score_cell(row_data)
        face_id_cell = self._generate_face_id_cell(row_data)
        company_cell = self._generate_company_cell(row_data)
        event_cell = self._generate_event_cell(row_data)
        list_cell = self._generate_list_cell(row_data)
        image_cell = self._generate_image_cell()
        ip_cell = self._generate_ip_cell(row_data)  # Скрытая ячейка для фильтрации
        
        # Генерация строки таблица
        return self._assemble_html_row(
            index=index,
            row_data=row_data,
            number_cell=number_cell,
            time_cell=time_cell,
            device_cell=device_cell,
            user_cell=user_cell,
            gender_cell=gender_cell,
            age_cell=age_cell,
            score_cell=score_cell,
            face_id_cell=face_id_cell,
            company_cell=company_cell,
            event_cell=event_cell,
            list_cell=list_cell,
            image_cell=image_cell,
            ip_cell=ip_cell
        )
    
    def _prepare_row_data(self) -> Dict[str, Any]:
        """Подготовка данных для строки"""
        # Определяем цвета для типа события
        event_color = "#4caf50" if self.event_type == "1" else "#ff9800"
        event_text = "Распознавание" if self.event_type == "1" else "Событие"
        
        # Определяем цвет для user_list
        list_color = "#2196f3" if self.user_list == "1" else "#9e9e9e"
        list_text = "В списке" if self.user_list == "1" else "Не в списке"
        
        # Определяем цвет для оценки совпадения
        score_color = self._get_score_color()
        
        # Определяем цвет для возраста
        age_color = self._get_age_color()
        
        # Определяем цвет для пола
        gender_color = "#2196f3" if self.gender == "Мужской" else "#e91e63"
        gender_icon = "👨" if self.gender == "Мужской" else "👩" if self.gender == "Женский" else "👤"
        
        # Безопасное экранирование строк
        device_id_str = html.escape(self.device_id)
        user_name_str = html.escape(self.user_name)
        face_id_str = html.escape(self.face_id)
        company_id_str = html.escape(self.company_id)
        ip_address_str = html.escape(self.ip_address)
        
        return {
            "event_color": event_color,
            "event_text": event_text,
            "list_color": list_color,
            "list_text": list_text,
            "score_color": score_color,
            "age_color": age_color,
            "gender_color": gender_color,
            "gender_icon": gender_icon,
            "device_id_str": device_id_str,
            "user_name_str": user_name_str,
            "face_id_str": face_id_str,
            "company_id_str": company_id_str,
            "ip_address_str": ip_address_str
        }
    
    def _get_score_color(self) -> str:
        """Определить цвет для оценки совпадения"""
        if self.score == "Н/Д":
            return "#9e9e9e"
        
        try:
            score_value = float(str(self.score).replace('%', '').replace(' ', ''))
            if score_value < 50:
                return "#f44336"  # Красный
            elif score_value < 70:
                return "#ff9800"  # Оранжевый
            elif score_value < 90:
                return "#ffc107"  # Желтый
            else:
                return "#4caf50"  # Зеленый
        except:
            return "#9e9e9e"  # Серый при ошибке
    
    def _get_age_color(self) -> str:
        """Определить цвет для возраста"""
        if self.age == "Н/Д":
            return "#9e9e9e"
        
        try:
            age_value = int(self.age)
            if age_value < 18:
                return "#e91e63"  # Розовый для детей
            elif age_value > 60:
                return "#795548"  # Коричневый для пожилых
            else:
                return "#2196f3"  # Синий для взрослых
        except:
            return "#9e9e9e"  # Серый при ошибке
    
    def _generate_number_cell(self, index: int) -> str:
        """Генерация ячейки с номером"""
        return f'''
        <td style="padding: 12px; text-align: center; font-weight: bold; 
            color: #666; border-right: 1px solid #eee;">
            {index + 1}
        </td>
        '''
    
    def _generate_time_cell(self) -> str:
        """Генерация ячейки с временем"""
        return f'''
        <td style="padding: 12px; font-family: 'Courier New', monospace; 
            font-size: 12px; color: #2c3e50;">
            {self.timestamp}
        </td>
        '''
    
    def _generate_device_cell(self) -> str:
        """Генерация ячейки с устройством"""
        return f'''
        <td style="padding: 12px; font-family: monospace; font-size: 11px;" 
            title="{html.escape(self.ip_address)}">
            <div style="display: flex; align-items: center; gap: 5px;">
                <span style="color: #555;">📱</span>
                {html.escape(self.device_id)}
            </div>
        </td>
        '''
    
    def _generate_user_cell(self) -> str:
        """Генерация ячейки с пользователем"""
        return f'''
        <td style="padding: 12px;">
            <div style="display: flex; align-items: center; gap: 5px;">
                <span style="font-size: 14px;">👤</span>
                <span style="font-weight: 500;">{html.escape(self.user_name)}</span>
            </div>
        </td>
        '''
    
    def _generate_gender_cell(self, row_data: Dict[str, Any]) -> str:
        """Генерация ячейки с полом"""
        gender_color = row_data["gender_color"]
        gender_icon = row_data["gender_icon"]
        
        return f'''
        <td style="padding: 12px; text-align: center;">
            <div style="display: inline-flex; align-items: center; gap: 5px; 
                 padding: 4px 10px; border-radius: 20px; 
                 background: {'#e3f2fd' if self.gender == 'Мужской' else '#fce4ec'}; 
                 color: {gender_color}; font-weight: bold;">
                <span>{gender_icon}</span>
                <span>{self.gender}</span>
            </div>
        </td>
        '''
    
    def _generate_age_cell(self, row_data: Dict[str, Any]) -> str:
        """Генерация ячейки с возрастом"""
        age_color = row_data["age_color"]
        
        return f'''
        <td style="padding: 12px; text-align: center; font-weight: bold; 
            color: {age_color};">
            {self.age}
            {f'<div style="font-size: 10px; color: #999; margin-top: 2px;">лет</div>' if self.age != 'Н/Д' else ''}
        </td>
        '''
    
    def _generate_score_cell(self, row_data: Dict[str, Any]) -> str:
        """Генерация ячейки с оценкой совпадения"""
        score_color = row_data["score_color"]
        
        return f'''
        <td style="padding: 12px; text-align: center;">
            <div style="display: inline-block; padding: 6px 12px; 
                 border-radius: 20px; background: #e8f5e9; 
                 color: {score_color}; font-weight: bold; font-size: 13px;">
                {self.score}
            </div>
        </td>
        '''
    
    def _generate_face_id_cell(self, row_data: Dict[str, Any]) -> str:
        """Генерация ячейки с ID лица"""
        face_id_str = row_data["face_id_str"]
        
        return f'''
        <td style="padding: 12px; font-family: monospace; font-size: 11px; 
            color: #555;">
            <div style="display: flex; align-items: center; gap: 5px;">
                <span style="color: #9c27b0;">🆔</span>
                {face_id_str}
            </div>
        </td>
        '''
    
    def _generate_company_cell(self, row_data: Dict[str, Any]) -> str:
        """Генерация ячейки с компанией"""
        company_id_str = row_data["company_id_str"]
        
        return f'''
        <td style="padding: 12px;">
            <div style="display: flex; align-items: center; gap: 5px;">
                <span style="color: #ff9800;">🏢</span>
                <span style="font-weight: 500;">{company_id_str}</span>
            </div>
        </td>
        '''
    
    def _generate_event_cell(self, row_data: Dict[str, Any]) -> str:
        """Генерация ячейки с типом события"""
        event_color = row_data["event_color"]
        event_text = row_data["event_text"]
        
        return f'''
        <td style="padding: 12px; text-align: center;">
            <div style="display: inline-flex; align-items: center; gap: 5px;
                 padding: 6px 12px; border-radius: 20px; 
                 background: {event_color}22; color: {event_color}; 
                 font-weight: bold; font-size: 12px;">
                <span>{"👁️" if self.event_type == "1" else "📅"}</span>
                <span>{event_text}</span>
            </div>
        </td>
        '''
    
    def _generate_list_cell(self, row_data: Dict[str, Any]) -> str:
        """Генерация ячейки со статусом списка"""
        list_color = row_data["list_color"]
        list_text = row_data["list_text"]
        
        return f'''
        <td style="padding: 12px; text-align: center;">
            <div style="display: inline-flex; align-items: center; gap: 5px;
                 padding: 6px 12px; border-radius: 20px; 
                 background: {list_color}22; color: {list_color}; 
                 font-weight: bold; font-size: 12px;">
                <span>{"✅" if self.user_list == "1" else "❌"}</span>
                <span>{list_text}</span>
            </div>
        </td>
        '''
    
    def _generate_image_cell(self) -> str:
        """Генерация ячейки с изображением"""
        if self.image_base64:
            return self._generate_image_with_photo()
        else:
            return self._generate_image_placeholder()
    
    def _generate_image_with_photo(self) -> str:
        """Генерация ячейки с фотографией"""
        # Вычисляем размер для отображения
        width = self.image_width if self.image_width > 0 else 120
        height = self.image_height if self.image_height > 0 else 120
        
        # Определяем стиль для сохранения пропорций
        img_style = f"""
            max-width: {width}px;
            max-height: {height}px;
            width: auto;
            height: auto;
            object-fit: contain;
            border-radius: 8px;
            border: 2px solid #4caf50;
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
            cursor: pointer;
            transition: all 0.3s ease;
            background: #f8f9fa;
        """
        
        user_name_str = html.escape(self.user_name)
        
        image_cell = f'''
        <td class="image-cell has-image" style="text-align: center; padding: 10px; vertical-align: middle;">
            <div style="position: relative; display: inline-block; margin: 5px;">
                <img src="data:image/jpeg;base64,{self.image_base64}" 
                     alt="Фото {self.image_hash[:8]}"
                     style="{img_style}"
                     onclick="showImagePreview('data:image/jpeg;base64,{self.image_base64}', 
                              '{user_name_str}', {self.image_width}, {self.image_height})"
                     onmouseover="this.style.transform='scale(1.05)'; this.style.boxShadow='0 6px 12px rgba(0,0,0,0.2)';"
                     onmouseout="this.style.transform='scale(1)'; this.style.boxShadow='0 4px 8px rgba(0,0,0,0.1)';"
                     title="Нажмите для увеличения\nРазмер: {self.image_width}×{self.image_height}\nВремя загрузки: {self.download_time_ms}мс">
                
                <!-- Индикаторы -->
                <div style="position: absolute; top: 5px; left: 5px; display: flex; gap: 2px;">
                    {f'<span style="background: #4caf50; color: white; padding: 2px 4px; border-radius: 3px; font-size: 9px;">{self.image_size_kb:.0f}KB</span>' if self.image_size_kb > 0 else ''}
                    {f'<span style="background: #2196f3; color: white; padding: 2px 4px; border-radius: 3px; font-size: 9px;">{self.download_time_ms}ms</span>' if self.download_time_ms > 0 else ''}
                    {f'<span style="background: #ff9800; color: white; padding: 2px 4px; border-radius: 3px; font-size: 9px;">Кэш</span>' if self.is_cached else ''}
                </div>
                
                <!-- Хэш в углу -->
                <div style="position: absolute; bottom: 5px; right: 5px; 
                            background: rgba(0,0,0,0.7); color: white; 
                            padding: 2px 6px; border-radius: 4px; font-size: 10px;
                            font-family: monospace;">
                    {self.image_hash[:6]}
                </div>
            </div>
            
            <!-- Информация под фото -->
            <div style="margin-top: 8px; font-size: 11px; color: #666;">
                <div style="display: flex; justify-content: center; gap: 5px; flex-wrap: wrap;">
                    {f'<span style="background: #e8f5e8; padding: 1px 4px; border-radius: 3px;">{self.image_width}×{self.image_height}</span>' if self.image_width > 0 else ''}
                    {f'<span style="background: #e3f2fd; padding: 1px 4px; border-radius: 3px;">ID: {self.image_hash[:8]}</span>'}
                </div>
            </div>
        </td>
        '''
        
        return image_cell
    
    def _generate_image_placeholder(self) -> str:
        """Генерация плашки "Нет фото" """
        # Определяем текст причины
        if self.failed_reason:
            reason_text = html.escape(self.failed_reason[:50])
            if len(self.failed_reason) > 50:
                reason_text += "..."
        else:
            reason_text = "Не загружено"
        
        # Определяем цвет плашки в зависимости от причины
        failed_lower = self.failed_reason.lower()
        if "timeout" in failed_lower or "time out" in failed_lower:
            bg_color = "#fff3e0"  # Оранжевый для таймаутов
            icon = "⏱️"
        elif "404" in self.failed_reason or "not found" in failed_lower:
            bg_color = "#ffebee"  # Красный для 404
            icon = "❓"
        elif "network" in failed_lower or "connection" in failed_lower:
            bg_color = "#f3e5f5"  # Фиолетовый для сетевых ошибок
            icon = "📡"
        elif "invalid" in failed_lower:
            bg_color = "#e8eaf6"  # Синий для невалидных данных
            icon = "⚠️"
        else:
            bg_color = "#f5f5f5"  # Серый по умолчанию
            icon = "📷"
        
        image_cell = f'''
        <td class="image-cell no-image" style="text-align: center; color: #666; 
            font-style: normal; padding: 15px 10px; background: {bg_color}; 
            border-radius: 8px; border: 1px dashed #ddd; vertical-align: middle;">
            <div style="font-size: 36px; margin-bottom: 10px; opacity: 0.7;">{icon}</div>
            <div style="font-size: 13px; font-weight: bold; margin-bottom: 5px; color: #555;">
                НЕТ ФОТО
            </div>
            <div style="font-size: 11px; margin-top: 3px; color: #777;">
                {reason_text}
            </div>
            {f'<div style="margin-top: 8px; font-size: 10px; color: #999; font-family: monospace;">URL: {html.escape(self.image_url[:30])}...</div>' if self.image_url else ''}
        </td>
        '''
        
        return image_cell
    
    def _generate_ip_cell(self, row_data: Dict[str, Any]) -> str:
        """Генерация скрытой ячейки с IP-адресом"""
        ip_address_str = row_data["ip_address_str"]
        return f'<td style="display: none;">{ip_address_str}</td>'
    
    def _assemble_html_row(self, 
                          index: int,
                          row_data: Dict[str, Any],
                          number_cell: str,
                          time_cell: str,
                          device_cell: str,
                          user_cell: str,
                          gender_cell: str,
                          age_cell: str,
                          score_cell: str,
                          face_id_cell: str,
                          company_cell: str,
                          event_cell: str,
                          list_cell: str,
                          image_cell: str,
                          ip_cell: str) -> str:
        """Сборка всех компонентов в HTML строку"""
        # Фон для четных строк
        background = '#f8f9fa;' if index % 2 == 0 else ''
        
        return f'''
        <tr class="data-row" style="border-bottom: 1px solid #e0e0e0; 
            transition: background-color 0.2s ease;
            {background}"
            onmouseover="this.style.backgroundColor='#f5f9ff';"
            onmouseout="this.style.backgroundColor='{'#f8f9fa' if index % 2 == 0 else 'white'}';"
            data-company="{row_data['company_id_str']}"
            data-event-type="{self.event_type}"
            data-user-list="{self.user_list}"
            data-gender="{self.gender}"
            data-age="{self.age}"
            data-device="{row_data['device_id_str']}">
            
            <!-- Номер -->
            {number_cell}
            
            <!-- Время -->
            {time_cell}
            
            <!-- Устройство -->
            {device_cell}
            
            <!-- Пользователь -->
            {user_cell}
            
            <!-- Пол -->
            {gender_cell}
            
            <!-- Возраст -->
            {age_cell}
            
            <!-- Совпадение -->
            {score_cell}
            
            <!-- ID Лица -->
            {face_id_cell}
            
            <!-- Компания -->
            {company_cell}
            
            <!-- Тип события -->
            {event_cell}
            
            <!-- Статус списка -->
            {list_cell}
            
            <!-- Изображение -->
            {image_cell}
            
            <!-- IP адрес (скрытая ячейка для фильтрации) -->
            {ip_cell}
            
        </tr>
        '''

@dataclass
class CheckpointData:
    """Данные для чекпоинта"""
    file_name: str
    total_lines: int
    processed_lines: int
    valid_images: int
    failed_images: int
    json_errors: int
    cached_images: int
    network_errors: int
    duplicate_records: int
    last_position: int
    timestamp: float
    batch_size: int
    processed_hashes: List[str]
    unique_users: List[str]
    unique_devices: List[str]
    unique_companies: List[str]
    unique_ips: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "file_name": self.file_name,
            "total_lines": self.total_lines,
            "processed_lines": self.processed_lines,
            "valid_images": self.valid_images,
            "failed_images": self.failed_images,
            "json_errors": self.json_errors,
            "cached_images": self.cached_images,
            "network_errors": self.network_errors,
            "duplicate_records": self.duplicate_records,
            "last_position": self.last_position,
            "timestamp": self.timestamp,
            "batch_size": self.batch_size,
            "processed_hashes": self.processed_hashes,
            "unique_users": self.unique_users,
            "unique_devices": self.unique_devices,
            "unique_companies": self.unique_companies,
            "unique_ips": self.unique_ips
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CheckpointData':
        return cls(**data)

@dataclass
class SystemMetrics:
    """Системные метрики"""
    cpu_percent: float
    memory_percent: float
    memory_used_gb: float
    memory_total_gb: float
    disk_free_gb: float
    disk_total_gb: float
    network_sent_mb: float
    network_recv_mb: float
    timestamp: float = field(default_factory=time.time)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "cpu_percent": self.cpu_percent,
            "memory_percent": self.memory_percent,
            "memory_used_gb": self.memory_used_gb,
            "memory_total_gb": self.memory_total_gb,
            "disk_free_gb": self.disk_free_gb,
            "disk_total_gb": self.disk_total_gb,
            "network_sent_mb": self.network_sent_mb,
            "network_recv_mb": self.network_recv_mb
        }

@dataclass 
class ImageMetrics:
    """Метрики обработки изображений"""
    url: str
    hash: str
    download_time_ms: int
    processing_time_ms: int
    size_kb: float
    width: int
    height: int
    is_cached: bool
    success: bool
    error_message: str = ""
    timestamp: float = field(default_factory=time.time)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url[:100] + "..." if len(self.url) > 100 else self.url,
            "hash": self.hash,
            "download_time_ms": self.download_time_ms,
            "processing_time_ms": self.processing_time_ms,
            "size_kb": self.size_kb,
            "width": self.width,
            "height": self.height,
            "is_cached": self.is_cached,
            "success": self.success,
            "error_message": self.error_message,
            "timestamp": self.timestamp
        }

@dataclass
class BatchStatistics:
    """Статистика обработки батча"""
    batch_number: int
    batch_size: int
    processing_time_seconds: float
    records_processed: int
    images_successful: int
    images_failed: int
    avg_image_time_ms: float
    memory_before_mb: float
    memory_after_mb: float
    timestamp: float = field(default_factory=time.time)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "batch_number": self.batch_number,
            "batch_size": self.batch_size,
            "processing_time_seconds": self.processing_time_seconds,
            "records_processed": self.records_processed,
            "images_successful": self.images_successful,
            "images_failed": self.images_failed,
            "avg_image_time_ms": self.avg_image_time_ms,
            "memory_before_mb": self.memory_before_mb,
            "memory_after_mb": self.memory_after_mb,
            "timestamp": self.timestamp
        }
    
    def to_string(self) -> str:
        return (f"Батч #{self.batch_number}: {self.records_processed} записей, "
                f"{self.images_successful}✅ {self.images_failed}❌, "
                f"время: {self.processing_time_seconds:.2f}с, "
                f"память: {self.memory_before_mb:.1f}→{self.memory_after_mb:.1f}MB")

# Вспомогательные функции
def generate_record_hash(record_data: Dict[str, Any]) -> str:
    """Генерация уникального хэша для записи"""
    # Создаем строку из ключевых полей
    key_data = {
        'timestamp': record_data.get('timestamp', ''),
        'device_id': record_data.get('device_id', ''),
        'user_name': record_data.get('user_name', ''),
        'face_id': record_data.get('face_id', ''),
        'image_url': record_data.get('image_url', '')
    }
    key_string = json.dumps(key_data, sort_keys=True)
    return hashlib.md5(key_string.encode()).hexdigest()

def format_timestamp(timestamp: str) -> str:
    """Форматирование временной метки"""
    if not timestamp or timestamp == 'Н/Д':
        return 'Н/Д'
    
    try:
        # Удаляем Z и преобразуем
        if 'Z' in timestamp:
            timestamp = timestamp.replace('Z', '+00:00')
        
        # Пытаемся разобрать как ISO формат
        dt = datetime.fromisoformat(timestamp)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        # Возвращаем как есть, если не удалось разобрать
        return str(timestamp)
    
    # Функции для работы с новыми полями
def extract_mongo_id(mongo_data: Any) -> str:
    """Извлечение MongoDB _id из различных форматов"""
    if not mongo_data:
        return ""
    
    try:
        if isinstance(mongo_data, dict):
            # Формат: {"_id": {"$oid": "..."}}
            if '_id' in mongo_data:
                return extract_mongo_id(mongo_data['_id'])
            elif '$oid' in mongo_data:
                return str(mongo_data['$oid'])
        
        elif isinstance(mongo_data, str):
            # Уже строка
            return mongo_data.strip()
        
        # Любой другой тип
        return str(mongo_data).strip()
        
    except Exception:
        return ""


def parse_request_type(request_type: Any) -> str:
    """Парсинг типа запроса"""
    if not request_type:
        return ""
    
    try:
        rt_str = str(request_type).strip().lower()
        
        # Типовые значения
        rt_mapping = {
            '1': "Распознавание",
            '2': "Событие",
            'recognition': "Распознавание",
            'event': "Событие",
            'face': "Распознавание",
            'face_recognition': "Распознавание",
            '': "",
            None: "",
            'null': "",
            'none': "",
        }
        
        return rt_mapping.get(rt_str, rt_str.title())
    except Exception:
        return ""


class ExtendedFaceRecord(FaceRecord):
    """Расширенный FaceRecord с дополнительными методами для ваших данных"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
    def get_extended_info(self) -> Dict[str, Any]:
        """Получить расширенную информацию о записи"""
        base_info = self.to_dict()
        
        # Дополнительная аналитика
        extended_info = {
            'has_user_id': bool(self.user_id and self.user_id.strip()),
            'has_frpic_name': bool(self.frpic_name and self.frpic_name.strip()),
            'has_mongo_id': bool(self.mongo_id and self.mongo_id.strip()),
            'is_complete_record': self._is_complete_record(),
            'data_quality_score': self._calculate_data_quality(),
            'request_type_display': self._get_request_type_display(),
            'mongo_id_short': self.mongo_id[:8] + '...' if len(self.mongo_id) > 8 else self.mongo_id if self.mongo_id else '',
        }
        
        return {**base_info, **extended_info}
    
    def _is_complete_record(self) -> bool:
        """Проверка, является ли запись полной"""
        required_fields = [
            self.timestamp != 'Н/Д',
            self.device_id != 'Н/Д',
            self.user_name != 'Н/Д',
            bool(self.image_url)
        ]
        return all(required_fields)
    
    def _calculate_data_quality(self) -> float:
        """Расчет качества данных записи (0-100%)"""
        fields_to_check = [
            (self.timestamp, 20),
            (self.device_id, 15),
            (self.user_name, 15),
            (self.image_url, 25),
            (self.face_id, 10),
            (self.company_id, 15),
        ]
        
        score = 0
        for field_value, field_weight in fields_to_check:
            if field_value and field_value != 'Н/Д':
                score += field_weight
        
        return min(score, 100)
    
    def _get_request_type_display(self) -> str:
        """Получить отображаемое значение типа запроса"""
        if not self.request_type:
            return "Не указан"
        
        rt_lower = self.request_type.lower()
        if rt_lower in ['1', 'recognition', 'face', 'face_recognition']:
            return "Распознавание"
        elif rt_lower in ['2', 'event']:
            return "Событие"
        else:
            return self.request_type.title()


# Утилиты для работы с группами записей
class RecordGroupAnalyzer:
    """Анализатор групп записей"""
    
    @staticmethod
    def group_by_device(records: List[FaceRecord]) -> Dict[str, List[FaceRecord]]:
        """Группировка записей по устройству"""
        groups = {}
        for record in records:
            device = record.device_id
            if device not in groups:
                groups[device] = []
            groups[device].append(record)
        return groups
    
    @staticmethod
    def group_by_company(records: List[FaceRecord]) -> Dict[str, List[FaceRecord]]:
        """Группировка записей по компании"""
        groups = {}
        for record in records:
            company = record.company_id
            if company not in groups:
                groups[company] = []
            groups[company].append(record)
        return groups
    
    @staticmethod
    def group_by_user(records: List[FaceRecord]) -> Dict[str, List[FaceRecord]]:
        """Группировка записей по пользователю"""
        groups = {}
        for record in records:
            user = record.user_name
            if user not in groups:
                groups[user] = []
            groups[user].append(record)
        return groups
    
    @staticmethod
    def analyze_groups(records: List[FaceRecord]) -> Dict[str, Any]:
        """Анализ групп записей"""
        device_groups = RecordGroupAnalyzer.group_by_device(records)
        company_groups = RecordGroupAnalyzer.group_by_company(records)
        user_groups = RecordGroupAnalyzer.group_by_user(records)
        
        return {
            'device_stats': {
                'total_devices': len(device_groups),
                'records_per_device': {k: len(v) for k, v in device_groups.items()},
                'top_devices': sorted(device_groups.items(), 
                                     key=lambda x: len(x[1]), reverse=True)[:10]
            },
            'company_stats': {
                'total_companies': len(company_groups),
                'records_per_company': {k: len(v) for k, v in company_groups.items()},
                'top_companies': sorted(company_groups.items(), 
                                       key=lambda x: len(x[1]), reverse=True)[:10]
            },
            'user_stats': {
                'total_users': len(user_groups),
                'records_per_user': {k: len(v) for k, v in user_groups.items()},
                'top_users': sorted(user_groups.items(), 
                                   key=lambda x: len(x[1]), reverse=True)[:10]
            }
        }


# Фабрика для создания записей из разных форматов данных
class FaceRecordFactory:
    """Фабрика для создания объектов FaceRecord из различных форматов данных"""
    
    @staticmethod
    def from_parsed_data(parsed_data: Dict[str, Any]) -> FaceRecord:
        """Создать FaceRecord из распарсенных данных"""
        # Основные поля
        timestamp = parsed_data.get('timestamp', 'Н/Д')
        device_id = parsed_data.get('device_id', 'Н/Д')
        user_name = parsed_data.get('user_name', 'Н/Д')
        gender = parsed_data.get('gender', 'Н/Д')
        age = parsed_data.get('age', 'Н/Д')
        score = parsed_data.get('score', 'Н/Д')
        face_id = parsed_data.get('face_id', 'Н/Д')
        company_id = parsed_data.get('company_id', 'Н/Д')
        image_url = parsed_data.get('image_url', '')
        event_type = parsed_data.get('event_type', '')
        user_list = parsed_data.get('user_list', '')
        ip_address = parsed_data.get('ip_address', 'Н/Д')
        
        # Новые поля
        user_id = parsed_data.get('user_id', '')
        frpic_name = parsed_data.get('frpic_name', '')
        request_type = parsed_data.get('request_type', '')
        mongo_id = extract_mongo_id(parsed_data.get('mongo_id', ''))
        # Добавляем company_type
        company_type = parsed_data.get('company_type', '')
        
        return FaceRecord(
            timestamp=timestamp,
            device_id=device_id,
            user_name=user_name,
            gender=gender,
            age=age,
            score=score,
            face_id=face_id,
            company_id=company_id,
            image_url=image_url,
            event_type=event_type,
            user_list=user_list,
            ip_address=ip_address,
            user_id=user_id,
            frpic_name=frpic_name,
            request_type=request_type,
            mongo_id=mongo_id,
            company_type=company_type  # Добавляем здесь
        )
    
    @staticmethod
    def from_json_line(line: str) -> Optional[FaceRecord]:
        """Создать FaceRecord из JSON строки"""
        try:
            import json as json_module
            data = json_module.loads(line.strip())
            
            # Используем парсер из data_parser.py если доступен
            try:
                from core.data_parser import parse_single_record
                from core.models import ProcessingMetrics
                
                metrics = ProcessingMetrics()
                parsed_data = parse_single_record(line, metrics)
                if parsed_data:
                    return FaceRecordFactory.from_parsed_data(parsed_data)
            except ImportError:
                pass
            
            # Прямое создание из словаря
            return FaceRecordFactory.from_dict(data)
            
        except Exception:
            return None
    
    @staticmethod
    def from_dict(data: Dict[str, Any]) -> FaceRecord:
        """Создать FaceRecord из словаря"""
        # Извлечение основных полей
        timestamp_obj = data.get('timestamp', {})
        timestamp = ""
        if isinstance(timestamp_obj, dict):
            timestamp = timestamp_obj.get('$date', '')
        elif isinstance(timestamp_obj, str):
            timestamp = timestamp_obj
        
        # Извлечение пола
        eva_sex = data.get('eva_sex', '')
        sex = data.get('sex', '')
        
        # Функция для определения пола
        def determine_gender(eva_sex_val, sex_val):
            eva_str = str(eva_sex_val).lower() if eva_sex_val else ''
            sex_str = str(sex_val).lower() if sex_val else ''
            
            if 'female' in eva_str or 'f' in eva_str or 'жен' in eva_str:
                return "Женский"
            elif 'male' in eva_str or 'm' in eva_str or 'муж' in eva_str:
                return "Мужской"
            elif sex_str == '0':
                return "Женский"
            elif sex_str == '1':
                return "Мужской"
            else:
                return "Н/Д"
        
        # Извлечение оценки
        comp_score = data.get('comp_score', '')
        score_display = "Н/Д"
        if comp_score:
            try:
                score_float = float(str(comp_score).replace('%', ''))
                score_display = f"{score_float:.1f}%"
            except:
                pass
        
        # Извлечение возраста
        eva_age = data.get('eva_age', '')
        age_display = "Н/Д"
        if eva_age:
            try:
                age_int = int(float(str(eva_age)))
                age_display = str(age_int)
            except:
                pass
        
        # Создание записи
        return FaceRecord(
            timestamp=timestamp,
            device_id=str(data.get('device_id', 'Н/Д')),
            user_name=str(data.get('user_name', 'Н/Д')),
            gender=determine_gender(eva_sex, sex),
            age=age_display,
            score=score_display,
            face_id=str(data.get('face_id', 'Н/Д')),
            company_id=str(data.get('company_id', 'Н/Д')),
            image_url=str(data.get('image', '')),
            event_type=str(data.get('event_type', '')),
            user_list=str(data.get('user_list', '')),
            ip_address=str(data.get('IP', data.get('device_ip', 'Н/Д'))),
            user_id=str(data.get('user_id', '')),
            frpic_name=str(data.get('frpic_name', '')),
            request_type=str(data.get('request_type', '')),
            mongo_id=extract_mongo_id(data.get('_id', '')),
            company_type=str(data.get('company_type', ''))  # Добавляем здесь
        )


# Кэш для ускорения создания записей
class FaceRecordCache:
    """Кэш для объектов FaceRecord"""
    
    def __init__(self, max_size: int = 10000):
        self.cache = {}
        self.max_size = max_size
        self.hits = 0
        self.misses = 0
    
    def get_or_create(self, key: str, creator_func) -> FaceRecord:
        """Получить из кэша или создать новый объект"""
        if key in self.cache:
            self.hits += 1
            return self.cache[key]
        
        self.misses += 1
        record = creator_func()
        
        # Добавляем в кэш если есть место
        if len(self.cache) < self.max_size:
            self.cache[key] = record
        
        return record
    
    def clear(self):
        """Очистить кэш"""
        self.cache.clear()
        self.hits = 0
        self.misses = 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Получить статистику кэша"""
        hit_rate = (self.hits / (self.hits + self.misses)) * 100 if (self.hits + self.misses) > 0 else 0
        return {
            'size': len(self.cache),
            'hits': self.hits,
            'misses': self.misses,
            'hit_rate': f"{hit_rate:.1f}%",
            'max_size': self.max_size
        }


# Экспорт основных классов и функций
__all__ = [
    'ProcessingMetrics',
    'FaceRecord',
    'ExtendedFaceRecord',
    'CheckpointData',
    'SystemMetrics',
    'ImageMetrics',
    'BatchStatistics',
    'generate_record_hash',
    'format_timestamp',
    'extract_mongo_id',
    'parse_request_type',
    'RecordGroupAnalyzer',
    'FaceRecordFactory',
    'FaceRecordCache'
]