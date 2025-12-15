"""
Статистический анализатор
"""

import json
from collections import defaultdict
from typing import List, Dict, Any
from core.models import FaceRecord

class StatisticsAnalyzer:
    """Анализатор статистики данных"""
    
    @staticmethod
    def analyze(records: List[FaceRecord]) -> Dict[str, Any]:
        """Анализ статистики записей"""
        stats = {
            'total_records': len(records),
            'by_company': defaultdict(int),
            'by_gender': defaultdict(int),
            'by_age_group': defaultdict(int),
            'by_device': defaultdict(int),
            'by_event_type': defaultdict(int),
            'by_user_list': defaultdict(int),
            'with_images': 0,
            'without_images': 0,
            'score_distribution': defaultdict(int),
            'hourly_distribution': defaultdict(int),
            'top_users': defaultdict(int),
            'top_devices': defaultdict(int)
        }
        
        for record in records:
            # По компании
            stats['by_company'][record.company_id] += 1
            
            # По полу
            stats['by_gender'][record.gender] += 1
            
            # По возрастным группам
            try:
                age = int(record.age) if record.age != 'Н/Д' else 0
                if age == 0:
                    stats['by_age_group']['Неизвестно'] += 1
                elif age < 18:
                    stats['by_age_group']['Дети (<18)'] += 1
                elif age < 30:
                    stats['by_age_group']['Молодые (18-29)'] += 1
                elif age < 50:
                    stats['by_age_group']['Взрослые (30-49)'] += 1
                else:
                    stats['by_age_group']['Старшие (50+)'] += 1
            except:
                stats['by_age_group']['Неизвестно'] += 1
            
            # По устройству
            stats['by_device'][record.device_id] += 1
            
            # По типу события
            stats['by_event_type'][record.event_type] += 1
            
            # По статусу в списке
            stats['by_user_list'][record.user_list] += 1
            
            # По наличию изображений
            if record.image_base64:
                stats['with_images'] += 1
            else:
                stats['without_images'] += 1
            
            # Распределение по оценкам
            try:
                if record.score != 'Н/Д':
                    # Преобразуем score в строку, если это не строка
                    score_str = str(record.score)
                    # Убираем символы процента и пробелы
                    score_clean = score_str.replace('%', '').replace(' ', '')
                    # Пытаемся преобразовать в float
                    score = float(score_clean)
                    if score < 50:
                        stats['score_distribution']['<50%'] += 1
                    elif score < 70:
                        stats['score_distribution']['50-69%'] += 1
                    elif score < 90:
                        stats['score_distribution']['70-89%'] += 1
                    else:
                        stats['score_distribution']['90-100%'] += 1
                else:
                    stats['score_distribution']['Н/Д'] += 1
            except:
                stats['score_distribution']['Н/Д'] += 1
            
            # Распределение по часам
            try:
                hour = record.timestamp.split()[1].split(':')[0]
                stats['hourly_distribution'][f"{hour}:00"] += 1
            except:
                pass
            
            # Топ пользователей
            stats['top_users'][record.user_name] += 1
            
            # Топ устройств
            stats['top_devices'][record.device_id] += 1
        
        # Сортируем топы
        stats['top_users'] = dict(sorted(stats['top_users'].items(), 
                                        key=lambda x: x[1], reverse=True)[:10])
        stats['top_devices'] = dict(sorted(stats['top_devices'].items(), 
                                          key=lambda x: x[1], reverse=True)[:10])
        
        return stats