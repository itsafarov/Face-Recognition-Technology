"""
Модуль оптимизации производительности системы
"""

import os
import sys
import gc
import psutil
import asyncio
import platform
import ctypes
import time
import hashlib
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from functools import lru_cache
from collections import OrderedDict

from core.config import Config
from utils.logger import setup_logging

logger = setup_logging()

@dataclass
class OptimizationConfig:
    """Конфигурация оптимизации"""
    # Настройки памяти
    target_memory_percent: float = 80.0
    min_available_memory_gb: float = 1.0
    aggressive_gc_threshold: float = 85.0
    
    # Настройки ввода-вывода
    file_buffer_size_mb: int = 10
    max_open_files: int = 1000
    
    # Настройки сети
    max_connections: int = 20
    connection_timeout: int = 10
    
    # Настройки процессора
    cpu_threshold: float = 80.0
    
    # Настройки кэша
    cache_max_size_mb: int = 200
    cache_ttl_seconds: int = 3600
    
    # Оптимизации парсинга
    batch_parsing_size: int = 1000
    use_fast_json: bool = True
    enable_cache: bool = True

class SystemOptimizer:
    """Оптимизатор производительности системы"""
    
    def __init__(self):
        self.config = OptimizationConfig()
        self.initial_state = {}
        self.optimizations_applied = []
        self.performance_stats = {
            'start_time': time.time(),
            'memory_before': {},
            'memory_after': {},
            'cpu_before': 0,
            'cpu_after': 0,
            'optimization_time': 0
        }
        
        # Состояние системы
        self.system_info = self._get_system_info()
        
        logger.info(f"Инициализирован SystemOptimizer для {self.system_info['os']}")
    
    async def optimize_system(self) -> Dict[str, Any]:
        """Оптимизация системы перед запуском обработки"""
        start_time = time.time()
        
        try:
            # Сохраняем начальное состояние
            self.performance_stats['memory_before'] = self._get_memory_stats()
            self.performance_stats['cpu_before'] = psutil.cpu_percent(interval=0.1)
            
            # Применяем оптимизации
            optimizations = [
                self._clear_memory_caches,
                self._optimize_python_runtime,
                self._optimize_file_system,
                self._optimize_network_settings,
                self._set_process_priority,
                self._configure_memory_limits,
                self._optimize_gc_settings,
                self._warm_up_caches
            ]
            
            for optimization in optimizations:
                try:
                    result = await optimization() if asyncio.iscoroutinefunction(optimization) else optimization()
                    if result:
                        self.optimizations_applied.append(result)
                except Exception as e:
                    logger.warning(f"Ошибка оптимизации {optimization.__name__}: {e}")
            
            # Сохраняем конечное состояние
            self.performance_stats['memory_after'] = self._get_memory_stats()
            self.performance_stats['cpu_after'] = psutil.cpu_percent(interval=0.1)
            self.performance_stats['optimization_time'] = time.time() - start_time
            
            # Адаптируем конфигурацию под систему
            self._adapt_configuration_to_system()
            
            logger.info(f"Применено {len(self.optimizations_applied)} оптимизаций за {self.performance_stats['optimization_time']:.2f} сек")
            return self.get_optimization_report()
            
        except Exception as e:
            logger.error(f"Ошибка оптимизации системы: {e}")
            return {'error': str(e)}
    
    def _get_system_info(self) -> Dict[str, Any]:
        """Получить информацию о системе"""
        try:
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('.')
            
            return {
                'os': platform.system(),
                'os_version': platform.release(),
                'architecture': platform.architecture()[0],
                'cpu_count': psutil.cpu_count(logical=False),
                'cpu_logical_count': psutil.cpu_count(logical=True),
                'memory_total_gb': memory.total / (1024**3),
                'memory_available_gb': memory.available / (1024**3),
                'disk_total_gb': disk.total / (1024**3),
                'disk_free_gb': disk.free / (1024**3),
                'python_version': platform.python_version(),
                'python_implementation': platform.python_implementation()
            }
        except Exception as e:
            logger.error(f"Ошибка получения информации о системе: {e}")
            return {}
    
    def _get_memory_stats(self) -> Dict[str, float]:
        """Получить статистику памяти"""
        try:
            memory = psutil.virtual_memory()
            return {
                'total_gb': memory.total / (1024**3),
                'available_gb': memory.available / (1024**3),
                'used_gb': memory.used / (1024**3),
                'percent': memory.percent,
                'free_gb': memory.free / (1024**3)
            }
        except:
            return {}
    
    def _clear_memory_caches(self) -> str:
        """Очистка кэшей памяти"""
        try:
            if platform.system() == "Windows":
                # Очистка кэша памяти в Windows
                ctypes.windll.kernel32.SetProcessWorkingSetSize(-1, 2**31-1, 2**31-1)
                return "Очищен кэш памяти Windows"
            elif platform.system() == "Linux":
                # Очистка кэша памяти в Linux
                os.system('sync; echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true')
                return "Очищен кэш памяти Linux"
            else:
                return "Очистка кэша не поддерживается для данной ОС"
        except Exception as e:
            return f"Ошибка очистки кэша: {e}"
    
    def _optimize_python_runtime(self) -> str:
        """Оптимизация среды выполнения Python"""
        optimizations = []
        
        try:
            # Оптимизация сборщика мусора
            gc.enable()
            gc.set_threshold(700, 10, 10)  # Более агрессивная сборка мусора
            
            # Отключение проверок отладки
            if hasattr(sys, 'gettrace') and sys.gettrace() is None:
                sys.settrace(None)
            
            # Оптимизация пула интернирования строк
            import sys
            sys.intern('')  # Инициализация пула
            
            # Установка оптимальных лимитов рекурсии
            sys.setrecursionlimit(10000)
            
            optimizations.append("Оптимизирована среда выполнения Python")
            
        except Exception as e:
            optimizations.append(f"Ошибка оптимизации Python: {e}")
        
        return "; ".join(optimizations)
    
    def _optimize_file_system(self) -> str:
        """Оптимизация файловой системы"""
        try:
            # Увеличение лимита открытых файлов
            if platform.system() != "Windows":
                import resource
                soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                resource.setrlimit(resource.RLIMIT_NOFILE, (min(10000, hard), hard))
                return f"Увеличен лимит открытых файлов до {min(10000, hard)}"
            return "Оптимизация файловой системы (только для Unix)"
        except Exception as e:
            return f"Ошибка оптимизации файловой системы: {e}"
    
    def _optimize_network_settings(self) -> str:
        """Оптимизация сетевых настроек"""
        try:
            # Эти настройки влияют на TCP/IP стек
            if platform.system() == "Windows":
                # Оптимизация TCP параметров для Windows
                import winreg
                
                try:
                    key = winreg.OpenKey(
                        winreg.HKEY_LOCAL_MACHINE,
                        r"SYSTEM\CurrentControlSet\Services\Tcpip\Parameters",
                        0,
                        winreg.KEY_WRITE
                    )
                    
                    # Увеличение размера окон TCP
                    winreg.SetValueEx(key, "TcpWindowSize", 0, winreg.REG_DWORD, 64240)
                    winreg.SetValueEx(key, "Tcp1323Opts", 0, winreg.REG_DWORD, 1)
                    
                    winreg.CloseKey(key)
                    return "Оптимизированы TCP настройки Windows"
                except:
                    return "Не удалось оптимизировать TCP настройки"
            
            return "Оптимизация сети не требуется"
        except Exception as e:
            return f"Ошибка оптимизации сети: {e}"
    
    def _set_process_priority(self) -> str:
        """Установка приоритета процесса"""
        try:
            if platform.system() == "Windows":
                import win32api
                import win32process
                import win32con
                
                pid = win32api.GetCurrentProcessId()
                handle = win32api.OpenProcess(win32con.PROCESS_ALL_ACCESS, True, pid)
                win32process.SetPriorityClass(handle, win32process.HIGH_PRIORITY_CLASS)
                win32api.CloseHandle(handle)
                return "Установлен высокий приоритет процесса"
            
            elif platform.system() == "Linux":
                import os
                os.nice(-10)  # Повышаем приоритет
                return "Повышен приоритет процесса (nice -10)"
            
            return "Установка приоритета не поддерживается"
        except Exception as e:
            return f"Ошибка установки приоритета: {e}"
    
    def _configure_memory_limits(self) -> str:
        """Настройка лимитов памяти"""
        try:
            # Адаптация конфигурации на основе доступной памяти
            memory_gb = psutil.virtual_memory().total / (1024**3)
            
            if memory_gb < 4:
                # Меньше 4GB RAM
                Config.MAX_WORKERS = 4
                Config.INITIAL_BATCH_SIZE = 1000
                Config.MAX_CACHE_SIZE_MB = 100
                self.config.cache_max_size_mb = 100
                
            elif memory_gb < 8:
                # 4-8GB RAM
                Config.MAX_WORKERS = 8
                Config.INITIAL_BATCH_SIZE = 2000
                Config.MAX_CACHE_SIZE_MB = 200
                self.config.cache_max_size_mb = 200
                
            elif memory_gb < 16:
                # 8-16GB RAM
                Config.MAX_WORKERS = 12
                Config.INITIAL_BATCH_SIZE = 4000
                Config.MAX_CACHE_SIZE_MB = 400
                self.config.cache_max_size_mb = 400
                
            else:
                # 16+ GB RAM
                Config.MAX_WORKERS = 16
                Config.INITIAL_BATCH_SIZE = 8000
                Config.MAX_CACHE_SIZE_MB = 800
                self.config.cache_max_size_mb = 800
            
            return f"Настроены лимиты памяти для {memory_gb:.1f}GB RAM"
            
        except Exception as e:
            return f"Ошибка настройки лимитов памяти: {e}"
    
    def _optimize_gc_settings(self) -> str:
        """Оптимизация настроек сборщика мусора"""
        try:
            # Более агрессивная сборка мусора
            gc.set_threshold(700, 10, 10)
            
            # Включение отладочных флагов (только для отладки)
            # gc.set_debug(gc.DEBUG_STATS | gc.DEBUG_SAVEALL)
            
            return "Оптимизированы настройки сборщика мусора"
        except Exception as e:
            return f"Ошибка оптимизации GC: {e}"
    
    async def _warm_up_caches(self) -> str:
        """Прогрев кэшей и библиотек"""
        try:
            # Прогрев часто используемых библиотек
            import numpy as np
            import cv2
            from PIL import Image
            
            # Прогрев numpy
            np.zeros((100, 100))
            np.ones((100, 100))
            
            # Прогрев OpenCV
            cv2.getBuildInformation()
            
            # Прогрев PIL
            Image.new('RGB', (100, 100), (255, 255, 255))
            
            # Прогрев hashlib
            hashlib.md5(b'test').hexdigest()
            hashlib.sha256(b'test').hexdigest()
            
            return "Прогреты кэши библиотек"
        except Exception as e:
            return f"Ошибка прогрева кэшей: {e}"
    
    def _adapt_configuration_to_system(self):
        """Адаптация конфигурации под конкретную систему"""
        try:
            # Адаптация под ОС
            if platform.system() == "Windows":
                Config.MAX_WORKERS = min(Config.MAX_WORKERS, 15)
                Config.INITIAL_BATCH_SIZE = min(Config.INITIAL_BATCH_SIZE, 5000)
                
            elif platform.system() == "Linux":
                Config.MAX_WORKERS = min(Config.MAX_WORKERS, 30)
                Config.INITIAL_BATCH_SIZE = min(Config.INITIAL_BATCH_SIZE, 10000)
                
            elif platform.system() == "Darwin":  # macOS
                Config.MAX_WORKERS = min(Config.MAX_WORKERS, 20)
                Config.INITIAL_BATCH_SIZE = min(Config.INITIAL_BATCH_SIZE, 8000)
            
            # Адаптация под тип диска
            try:
                import psutil
                disk_io_counters = psutil.disk_io_counters()
                if disk_io_counters:
                    # Если это SSD, можно увеличить параллелизм
                    Config.MAX_WORKERS = int(Config.MAX_WORKERS * 1.2)
            except:
                pass
            
            logger.info(f"Адаптированная конфигурация: Workers={Config.MAX_WORKERS}, Batch={Config.INITIAL_BATCH_SIZE}")
            
        except Exception as e:
            logger.error(f"Ошибка адаптации конфигурации: {e}")
    
    def get_optimization_report(self) -> Dict[str, Any]:
        """Получить отчет об оптимизациях"""
        return {
            'system_info': self.system_info,
            'optimizations_applied': self.optimizations_applied,
            'performance_stats': self.performance_stats,
            'config_after': {
                'MAX_WORKERS': Config.MAX_WORKERS,
                'INITIAL_BATCH_SIZE': Config.INITIAL_BATCH_SIZE,
                'MAX_CACHE_SIZE_MB': Config.MAX_CACHE_SIZE_MB,
                'MAX_MEMORY_PERCENT': Config.MAX_MEMORY_PERCENT
            },
            'optimizer_config': {
                'target_memory_percent': self.config.target_memory_percent,
                'cache_max_size_mb': self.config.cache_max_size_mb,
                'file_buffer_size_mb': self.config.file_buffer_size_mb
            }
        }
    
    def get_performance_tips(self) -> List[str]:
        """Получить советы по оптимизации производительности"""
        tips = []
        
        # Советы на основе информации о системе
        memory_gb = self.system_info.get('memory_total_gb', 0)
        disk_free_gb = self.system_info.get('disk_free_gb', 0)
        
        if memory_gb < 4:
            tips.append("⚠️ Мало оперативной памяти (<4GB). Рекомендуется:")
            tips.append("  • Закройте другие программы")
            tips.append("  • Используйте только HTML отчет")
            tips.append("  • Уменьшите размер батча в конфигурации")
        
        if disk_free_gb < 10:
            tips.append("⚠️ Мало свободного места на диске (<10GB). Рекомендуется:")
            tips.append("  • Освободите место на диске")
            tips.append("  • Используйте другой диск для временных файлов")
        
        if platform.system() == "Windows":
            tips.append("💡 Для Windows рекомендуется:")
            tips.append("  • Отключить антивирус на время обработки")
            tips.append("  • Использовать SSD для ускорения ввода-вывода")
            tips.append("  • Закрыть фоновые приложения")
        
        elif platform.system() == "Linux":
            tips.append("💡 Для Linux рекомендуется:")
            tips.append("  • Использовать nohup для длительных задач")
            tips.append("  • Настроить ulimit для большего количества файлов")
            tips.append("  • Использовать tmpfs для временных файлов")
        
        # Общие советы
        tips.append("🚀 Общие рекомендации:")
        tips.append("  • Используйте --resume при прерывании обработки")
        tips.append("  • Для файлов >1GB используйте только HTML отчет")
        tips.append("  • Мониторьте использование памяти в реальном времени")
        tips.append("  • Регулярно обновляйте зависимости")
        
        return tips

class MemoryOptimizer:
    """Оптимизатор использования памяти"""
    
    def __init__(self):
        self.memory_stats = []
        self.optimization_history = []
        self.last_optimization = time.time()
        
    def monitor_memory_usage(self) -> Dict[str, float]:
        """Мониторинг использования памяти"""
        try:
            memory = psutil.virtual_memory()
            process = psutil.Process()
            
            stats = {
                'timestamp': time.time(),
                'system_total_gb': memory.total / (1024**3),
                'system_used_gb': memory.used / (1024**3),
                'system_available_gb': memory.available / (1024**3),
                'system_percent': memory.percent,
                'process_rss_gb': process.memory_info().rss / (1024**3),
                'process_vms_gb': process.memory_info().vms / (1024**3),
                'process_percent': process.memory_percent()
            }
            
            self.memory_stats.append(stats)
            
            # Ограничиваем размер истории
            if len(self.memory_stats) > 1000:
                self.memory_stats = self.memory_stats[-1000:]
            
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка мониторинга памяти: {e}")
            return {}
    
    def should_optimize_memory(self) -> bool:
        """Проверить, нужно ли оптимизировать память"""
        try:
            memory = psutil.virtual_memory()
            
            # Проверка условий для оптимизации
            conditions = [
                memory.percent > Config.MAX_MEMORY_PERCENT,
                memory.available / (1024**3) < 0.5,  # Меньше 500MB свободно
                time.time() - self.last_optimization > 30  # Не чаще чем раз в 30 секунд
            ]
            
            return any(conditions)
            
        except:
            return False
    
    def optimize_memory(self) -> Dict[str, Any]:
        """Оптимизация использования памяти"""
        start_time = time.time()
        optimizations = []
        
        try:
            # 1. Принудительный сбор мусора
            collected = gc.collect()
            optimizations.append(f"Собрано мусора: {collected} объектов")
            
            # 2. Очистка кэшей Python
            import sys
            cleared_modules = self._clear_unused_modules()
            optimizations.append(f"Очищено модулей: {cleared_modules}")
            
            # 3. Освобождение кэша файловой системы
            if platform.system() == "Linux":
                os.system('sync; echo 1 > /proc/sys/vm/drop_caches 2>/dev/null || true')
                optimizations.append("Очищен кэш файловой системы")
            
            # 4. Очистка внутренних кэшей
            cleared = self._clear_internal_caches()
            optimizations.append(f"Очищено внутренних кэшей: {cleared}")
            
            self.last_optimization = time.time()
            
            result = {
                'success': True,
                'optimizations': optimizations,
                'time_seconds': time.time() - start_time,
                'memory_before': self.memory_stats[-1] if self.memory_stats else {},
                'memory_after': self.monitor_memory_usage()
            }
            
            self.optimization_history.append(result)
            
            logger.info(f"Оптимизация памяти выполнена за {result['time_seconds']:.2f} сек")
            return result
            
        except Exception as e:
            error_result = {
                'success': False,
                'error': str(e),
                'time_seconds': time.time() - start_time
            }
            self.optimization_history.append(error_result)
            return error_result
    
    def _clear_unused_modules(self) -> int:
        """Очистка неиспользуемых модулей"""
        try:
            import sys
            import types
            
            modules_to_clear = []
            
            for name, module in list(sys.modules.items()):
                if (isinstance(module, types.ModuleType) and 
                    not name.startswith('_') and
                    name not in ['sys', 'builtins', '__main__'] and
                    'site-packages' in str(module.__file__) if hasattr(module, '__file__') else False):
                    
                    # Проверяем, используется ли модуль
                    refcount = sys.getrefcount(module)
                    if refcount <= 3:  # Мало ссылок
                        modules_to_clear.append(name)
            
            # Удаляем модули
            for name in modules_to_clear[:10]:  # Ограничиваем количество
                del sys.modules[name]
            
            return len(modules_to_clear)
        except:
            return 0
    
    def _clear_internal_caches(self) -> int:
        """Очистка внутренних кэшей"""
        cleared = 0
        
        try:
            # Очистка кэша functools.lru_cache
            import functools
            for attr in dir(functools):
                obj = getattr(functools, attr)
                if hasattr(obj, 'cache_clear'):
                    try:
                        obj.cache_clear()
                        cleared += 1
                    except:
                        pass
            
            # Очистка кэша re
            import re
            if hasattr(re, '_cache'):
                re._cache.clear()
                cleared += 1
            
            # Очистка кэша locale
            import locale
            locale._localized_groups_cache.clear()
            cleared += 1
            
        except Exception as e:
            logger.debug(f"Ошибка очистки кэшей: {e}")
        
        return cleared
    
    def get_memory_report(self) -> Dict[str, Any]:
        """Получить отчет об использовании памяти"""
        if not self.memory_stats:
            return {'error': 'Нет данных о памяти'}
        
        latest = self.memory_stats[-1]
        avg_percent = sum(s.get('system_percent', 0) for s in self.memory_stats) / len(self.memory_stats)
        
        return {
            'current': latest,
            'average_percent': avg_percent,
            'stats_count': len(self.memory_stats),
            'optimizations_count': len(self.optimization_history),
            'last_optimization': self.last_optimization
        }

class IOBufferOptimizer:
    """Оптимизатор буферизации ввода-вывода"""
    
    @staticmethod
    @lru_cache(maxsize=128)
    def get_optimal_buffer_size(file_size_bytes: int) -> int:
        """Получить оптимальный размер буфера для файла"""
        if file_size_bytes < 1024 * 1024:  # < 1MB
            return 64 * 1024  # 64KB
        elif file_size_bytes < 100 * 1024 * 1024:  # < 100MB
            return 512 * 1024  # 512KB
        elif file_size_bytes < 1024 * 1024 * 1024:  # < 1GB
            return 2 * 1024 * 1024  # 2MB
        else:  # >= 1GB
            return 8 * 1024 * 1024  # 8MB
    
    @staticmethod
    def optimize_file_handles(max_files: int = 1000) -> bool:
        """Оптимизация количества открытых файлов"""
        try:
            if platform.system() != "Windows":
                import resource
                soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                new_soft = min(max_files, hard)
                resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft, hard))
                return True
            return False
        except:
            return False

class PerformanceProfiler:
    """Профайлер производительности"""
    
    def __init__(self):
        self.metrics = {}
        self.start_time = time.time()
        
    def start_section(self, section_name: str):
        """Начать отсчет времени для секции"""
        self.metrics[section_name] = {
            'start': time.time(),
            'end': None,
            'duration': None,
            'memory_before': self._get_process_memory(),
            'memory_after': None,
            'memory_delta': None
        }
    
    def end_section(self, section_name: str):
        """Завершить отсчет времени для секции"""
        if section_name in self.metrics:
            self.metrics[section_name]['end'] = time.time()
            self.metrics[section_name]['duration'] = (
                self.metrics[section_name]['end'] - self.metrics[section_name]['start']
            )
            self.metrics[section_name]['memory_after'] = self._get_process_memory()
            self.metrics[section_name]['memory_delta'] = (
                self.metrics[section_name]['memory_after'] - self.metrics[section_name]['memory_before']
            )
    
    def _get_process_memory(self) -> float:
        """Получить использование памяти процессом"""
        try:
            process = psutil.Process()
            return process.memory_info().rss / (1024**2)  # MB
        except:
            return 0.0
    
    def get_profile_report(self) -> Dict[str, Any]:
        """Получить отчет профилирования"""
        total_time = time.time() - self.start_time
        
        # Рассчитываем проценты времени
        for name, data in self.metrics.items():
            if data['duration']:
                data['percent'] = (data['duration'] / total_time) * 100
        
        # Сортируем по времени выполнения
        sorted_metrics = sorted(
            [(name, data) for name, data in self.metrics.items() if data.get('duration')],
            key=lambda x: x[1]['duration'],
            reverse=True
        )
        
        return {
            'total_time_seconds': total_time,
            'sections': dict(sorted_metrics),
            'top_bottlenecks': sorted_metrics[:5]  # Топ-5 узких мест
        }

# Глобальные экземпляры для повторного использования
_system_optimizer = None
_memory_optimizer = None
_performance_profiler = None

def get_system_optimizer() -> SystemOptimizer:
    """Получить глобальный экземпляр оптимизатора системы"""
    global _system_optimizer
    if _system_optimizer is None:
        _system_optimizer = SystemOptimizer()
    return _system_optimizer

def get_memory_optimizer() -> MemoryOptimizer:
    """Получить глобальный экземпляр оптимизатора памяти"""
    global _memory_optimizer
    if _memory_optimizer is None:
        _memory_optimizer = MemoryOptimizer()
    return _memory_optimizer

def get_performance_profiler() -> PerformanceProfiler:
    """Получить глобальный экземпляр профайлера"""
    global _performance_profiler
    if _performance_profiler is None:
        _performance_profiler = PerformanceProfiler()
    return _performance_profiler

async def optimize_for_file_size(file_size_gb: float) -> Dict[str, Any]:
    """Оптимизация под конкретный размер файла"""
    config_updates = {}
    
    if file_size_gb > 10:
        config_updates = {
            'MAX_WORKERS': 8,
            'INITIAL_BATCH_SIZE': 1000,
            'MAX_CACHE_SIZE_MB': 200,
            'CHECKPOINT_INTERVAL': 50000,
            'MAX_MEMORY_PERCENT': 80
        }
        logger.info("Настройки для файлов >10GB")
        
    elif file_size_gb > 5:
        config_updates = {
            'MAX_WORKERS': 12,
            'INITIAL_BATCH_SIZE': 2000,
            'MAX_CACHE_SIZE_MB': 400,
            'CHECKPOINT_INTERVAL': 100000,
            'MAX_MEMORY_PERCENT': 85
        }
        logger.info("Настройки для файлов 5-10GB")
        
    elif file_size_gb > 1:
        config_updates = {
            'MAX_WORKERS': 16,
            'INITIAL_BATCH_SIZE': 4000,
            'MAX_CACHE_SIZE_MB': 600,
            'CHECKPOINT_INTERVAL': 200000,
            'MAX_MEMORY_PERCENT': 90
        }
        logger.info("Настройки для файлов 1-5GB")
    
    else:
        config_updates = {
            'MAX_WORKERS': 20,
            'INITIAL_BATCH_SIZE': 8000,
            'MAX_CACHE_SIZE_MB': 800,
            'CHECKPOINT_INTERVAL': 500000,
            'MAX_MEMORY_PERCENT': 95
        }
        logger.info("Настройки для файлов <1GB")
    
    # Применяем обновления конфигурации
    for key, value in config_updates.items():
        if hasattr(Config, key):
            setattr(Config, key, value)
    
    return config_updates

def print_optimization_tips():
    """Вывести советы по оптимизации"""
    optimizer = get_system_optimizer()
    tips = optimizer.get_performance_tips()
    
    print("\n" + "="*80)
    print("💡 СОВЕТЫ ПО ОПТИМИЗАЦИИ ПРОИЗВОДИТЕЛЬНОСТИ")
    print("="*80)
    
    for tip in tips:
        print(tip)
    
    print("="*80)

async def run_comprehensive_optimization() -> Dict[str, Any]:
    """Выполнить комплексную оптимизацию"""
    print("🔄 Запуск комплексной оптимизации системы...")
    
    # 1. Оптимизация системы
    system_optimizer = get_system_optimizer()
    system_report = await system_optimizer.optimize_system()
    
    # 2. Инициализация мониторинга памяти
    memory_optimizer = get_memory_optimizer()
    memory_optimizer.monitor_memory_usage()
    
    # 3. Инициализация профайлера
    profiler = get_performance_profiler()
    profiler.start_section('total_processing')
    
    # 4. Оптимизация буферов ввода-вывода
    IOBufferOptimizer.optimize_file_handles()
    
    # 5. Вывод советов
    # print_optimization_tips()  # Закомментировано для удаления советов по оптимизации
    
    return {
        'system_optimization': system_report,
        'memory_optimizer_initialized': True,
        'profiler_initialized': True,
        'io_optimized': True
    }

# Экспорт основных функций
__all__ = [
    'SystemOptimizer',
    'MemoryOptimizer',
    'IOBufferOptimizer',
    'PerformanceProfiler',
    'get_system_optimizer',
    'get_memory_optimizer',
    'get_performance_profiler',
    'optimize_for_file_size',
    'print_optimization_tips',
    'run_comprehensive_optimization'
]

# Автоматическая инициализация при импорте
get_system_optimizer()
get_memory_optimizer()