"""
Мониторинг использования памяти
"""

import psutil
import threading
import time
from typing import Dict, List

class MemoryMonitor:
    """Мониторинг использования памяти в реальном времени"""
    
    def __init__(self):
        self.memory_samples: List[float] = []
        self.peak_memory = 0
        self.running = False
        self.thread = None
    
    def start(self):
        """Запуск мониторинга памяти"""
        if self.running:
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()
    
    def stop(self):
        """Остановка мониторинга"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=2)
    
    def _monitor_loop(self):
        """Цикл мониторинга"""
        while self.running:
            try:
                memory_percent = psutil.virtual_memory().percent
                memory_mb = psutil.virtual_memory().used / 1024 / 1024
                
                self.memory_samples.append(memory_mb)
                self.peak_memory = max(self.peak_memory, memory_mb)
                
                # Ограничиваем размер списка
                if len(self.memory_samples) > 1000:
                    self.memory_samples = self.memory_samples[-1000:]
                
            except Exception:
                pass
            
            time.sleep(1)  # Проверяем каждую секунду
    
    def get_current_memory(self) -> float:
        """Получить текущее использование памяти в MB"""
        return psutil.virtual_memory().used / 1024 / 1024
    
    def get_memory_percent(self) -> float:
        """Получить процент использования памяти"""
        return psutil.virtual_memory().percent
    
    def get_statistics(self) -> Dict:
        """Получить статистику использования памяти"""
        if not self.memory_samples:
            return {
                'current_memory_mb': self.get_current_memory(),
                'current_memory_percent': self.get_memory_percent(),
                'peak_memory_mb': 0,
                'avg_memory_mb': 0
            }
        
        return {
            'current_memory_mb': self.get_current_memory(),
            'current_memory_percent': self.get_memory_percent(),
            'peak_memory_mb': self.peak_memory,
            'avg_memory_mb': sum(self.memory_samples) / len(self.memory_samples)
        }