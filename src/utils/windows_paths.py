"""
Утилиты для работы с путями в Windows
"""

import os
import platform
from typing import Optional
from pathlib import Path, PureWindowsPath

def get_windows_safe_path(base_dir: str, *paths: str, max_length: int = 200) -> str:
    """
    Получить безопасный путь для Windows с учетом ограничений.
    
    Args:
        base_dir: Базовая директория
        *paths: Дополнительные компоненты пути
        max_length: Максимальная длина полного пути (по умолчанию 200 для запаса)
    
    Returns:
        str: Безопасный путь
    """
    if platform.system() != "Windows":
        return os.path.join(base_dir, *paths)
    
    # Собираем полный путь
    full_path = os.path.join(base_dir, *paths)
    
    # Проверяем длину
    if len(full_path) <= max_length:
        return full_path
    
    # Если путь слишком длинный, укорачиваем имя файла
    path_obj = Path(full_path)
    if not path_obj.is_file():
        return full_path  # Для директорий не укорачиваем
    
    # Укорачиваем имя файла
    parent = str(path_obj.parent)
    name, ext = os.path.splitext(path_obj.name)
    
    # Вычисляем максимальную длину имени
    max_name_length = max_length - len(parent) - len(ext) - 1  # -1 для разделителя
    
    if max_name_length < 10:  # Минимум 10 символов для имени
        max_name_length = 10
    
    # Укорачиваем имя, сохраняя начало и конец
    if len(name) > max_name_length:
        keep_chars = max_name_length - 3  # Оставляем место для "..."
        if keep_chars < 1:
            name = name[:max_name_length]
        else:
            first_part = name[:keep_chars // 2]
            last_part = name[-(keep_chars - len(first_part)):]
            name = f"{first_part}...{last_part}"
    
    new_filename = f"{name}{ext}"
    return os.path.join(parent, new_filename)


def enable_windows_long_paths():
    """
    Попытаться включить поддержку длинных путей в Windows.
    Требует прав администратора или настройки реестра.
    """
    if platform.system() != "Windows":
        return False
    
    try:
        import winreg
        key_path = r"SYSTEM\CurrentControlSet\Control\FileSystem"
        value_name = "LongPathsEnabled"
        
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, key_path, 
                          0, winreg.KEY_READ | winreg.KEY_WOW64_64KEY) as key:
            value, reg_type = winreg.QueryValueEx(key, value_name)
            if value == 1:
                return True
                
        # Пытаемся установить
        try:
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, key_path,
                              0, winreg.KEY_SET_VALUE | winreg.KEY_WOW64_64KEY) as key:
                winreg.SetValueEx(key, value_name, 0, winreg.REG_DWORD, 1)
                return True
        except PermissionError:
            # Требуются права администратора
            return False
            
    except Exception as e:
        # Если не удалось, работаем с ограничениями
        return False


def normalize_windows_path(path: str) -> str:
    """
    Нормализация пути для Windows.
    Заменяет недопустимые символы и приводит к стандартному виду.
    """
    if platform.system() != "Windows":
        return path
    
    # Недопустимые символы в именах файлов Windows
    invalid_chars = '<>:"|?*' + ''.join(chr(i) for i in range(0, 32))
    trans_table = str.maketrans(invalid_chars, '_' * len(invalid_chars))
    
    # Разбираем путь
    path_obj = Path(path)
    
    if path_obj.is_file():
        # Нормализуем имя файла
        parent = str(path_obj.parent)
        name = path_obj.name.translate(trans_table)
        
        # Убираем точки и пробелы в конце (проблема Windows)
        name = name.rstrip('. ')
        
        # Ограничение длины имени файла в Windows (без расширения)
        if len(os.path.splitext(name)[0]) > 255:
            name_parts = os.path.splitext(name)
            name = name_parts[0][:250] + name_parts[1]
        
        return os.path.join(parent, name)
    
    # Для директорий нормализуем каждую часть
    parts = []
    for part in path_obj.parts:
        normalized = part.translate(trans_table).rstrip('. ')
        parts.append(normalized)
    
    return str(Path(*parts))


def is_windows_long_path_supported() -> bool:
    """
    Проверить, поддерживаются ли длинные пути в текущей системе Windows.
    """
    if platform.system() != "Windows":
        return True
    
    # Проверка через попытку создания длинного пути
    import tempfile
    try:
        # Создаем временный файл с длинным путем
        with tempfile.NamedTemporaryFile(prefix='a'*100, suffix='.txt', delete=False) as f:
            long_path = f.name
            # Если не выброшено исключение, длинные пути поддерживаются
            os.unlink(long_path)
            return True
    except (OSError, IOError):
        return False


# Экспорт функций
__all__ = [
    'get_windows_safe_path',
    'enable_windows_long_paths',
    'normalize_windows_path',
    'is_windows_long_path_supported'
]