"""
Утилиты для работы с путями в Windows
"""

import os
import platform
import sys
from pathlib import Path, PureWindowsPath
from typing import Optional, Union

# Проблема 1: Отсутствует обработка импорта winreg для Windows
# Проблема 2: Неправильное создание таблицы перевода символов
# Проблема 3: Ошибки в логике обработки путей

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
    
    # Собираем полный путь с использованием os.path.join
    full_path = os.path.join(base_dir, *paths)
    
    # Проверяем длину пути
    if len(full_path) <= max_length:
        return full_path
    
    # Если путь слишком длинный, пытаемся его укоротить
    try:
        path_obj = Path(full_path)
    except Exception:
        # Если не удалось создать Path объект, возвращаем урезанную версию
        return full_path[:max_length]
    
    # Проверяем, является ли это файлом
    try:
        is_file = path_obj.is_file()
    except (OSError, ValueError):
        # Если не удалось проверить, предполагаем что это файл
        is_file = True
    
    if not is_file:
        # Для директорий возвращаем урезанный путь
        return full_path[:max_length]
    
    # Для файлов: укорачиваем имя файла
    parent = str(path_obj.parent)
    name = path_obj.name
    
    # Разделяем имя и расширение
    name_without_ext, ext = os.path.splitext(name)
    
    # Вычисляем максимальную длину имени
    # +1 для разделителя между директорией и файлом
    max_name_length = max_length - len(parent) - len(ext) - 1
    
    if max_name_length < 3:  # Минимум 3 символа для имени
        # Если даже с минимальным именем не помещается, урезаем весь путь
        return full_path[:max_length]
    
    # Укорачиваем имя файла, сохраняя начало
    if len(name_without_ext) > max_name_length:
        # Оставляем место для "~" в конце (Windows-стиль)
        if max_name_length > 4:
            shortened_name = name_without_ext[:max_name_length - 1] + "~"
        else:
            shortened_name = name_without_ext[:max_name_length]
    else:
        shortened_name = name_without_ext
    
    new_filename = f"{shortened_name}{ext}"
    return os.path.join(parent, new_filename)


def enable_windows_long_paths() -> bool:
    """
    Попытаться включить поддержку длинных путей в Windows.
    Требует прав администратора или настройки реестра.
    Возвращает True если длинные пути уже включены или удалось их включить.
    """
    if platform.system() != "Windows":
        return True
    
    try:
        # Условный импорт для Windows
        import winreg
        
        key_path = r"SYSTEM\CurrentControlSet\Control\FileSystem"
        value_name = "LongPathsEnabled"
        
        # Пытаемся прочитать текущее значение
        try:
            with winreg.OpenKey(
                winreg.HKEY_LOCAL_MACHINE, 
                key_path, 
                0, 
                winreg.KEY_READ
            ) as key:
                value, reg_type = winreg.QueryValueEx(key, value_name)
                if value == 1:
                    return True
        except FileNotFoundError:
            # Ключ не существует, нужно создать
            pass
        except Exception as e:
            # Не удалось прочитать, продолжаем попытку записи
            pass
        
        # Пытаемся установить значение
        try:
            with winreg.OpenKey(
                winreg.HKEY_LOCAL_MACHINE, 
                key_path,
                0, 
                winreg.KEY_SET_VALUE | winreg.KEY_WOW64_64KEY
            ) as key:
                winreg.SetValueEx(key, value_name, 0, winreg.REG_DWORD, 1)
                return True
        except (PermissionError, FileNotFoundError):
            # Нет прав или ключ не существует
            # Пробуем создать ключ с правами пользователя
            try:
                # Создаем ключ если не существует
                try:
                    key = winreg.CreateKeyEx(
                        winreg.HKEY_LOCAL_MACHINE,
                        key_path,
                        0,
                        winreg.KEY_SET_VALUE | winreg.KEY_WOW64_64KEY
                    )
                except PermissionError:
                    # Пробуем в HKEY_CURRENT_USER
                    key_path_user = r"Software\Microsoft\Windows\CurrentVersion\AppModel\Unlock"
                    try:
                        key = winreg.CreateKeyEx(
                            winreg.HKEY_CURRENT_USER,
                            key_path_user,
                            0,
                            winreg.KEY_SET_VALUE
                        )
                        winreg.SetValueEx(key, "AllowDevelopmentWithoutDevLicense", 0, winreg.REG_DWORD, 1)
                        winreg.CloseKey(key)
                    except Exception:
                        pass
                    return False
                
                winreg.SetValueEx(key, value_name, 0, winreg.REG_DWORD, 1)
                winreg.CloseKey(key)
                return True
            except Exception:
                return False
                
    except ImportError:
        # Модуль winreg недоступен (не Windows или проблемы с импортом)
        return False
    except Exception as e:
        # Любая другая ошибка
        return False


def normalize_windows_path(path: str) -> str:
    """
    Нормализация пути для Windows.
    Заменяет недопустимые символы и приводит к стандартному виду.
    
    Args:
        path: Путь для нормализации
        
    Returns:
        Нормализованный путь
    """
    if platform.system() != "Windows":
        return path
    
    # Недопустимые символы в именах файлов Windows
    # Исправляем генерацию таблицы перевода
    invalid_chars = '<>:"|?*'
    # Добавляем управляющие символы (0-31)
    invalid_chars += ''.join(chr(i) for i in range(0, 32))
    
    # Создаем таблицу перевода: заменяем все недопустимые символы на '_'
    trans_table = str.maketrans(
        invalid_chars,
        '_' * len(invalid_chars)
    )
    
    try:
        path_obj = Path(path)
    except Exception:
        # Если не удалось создать Path, обрабатываем как строку
        normalized = str(path).translate(trans_table)
        # Убираем точки и пробелы в конце
        normalized = normalized.rstrip('. ')
        return normalized
    
    # Разбираем путь на части и нормализуем каждую
    parts = []
    for part in path_obj.parts:
        normalized_part = part.translate(trans_table).rstrip('. ')
        parts.append(normalized_part)
    
    # Собираем путь обратно
    try:
        result = str(Path(*parts))
    except Exception:
        # Если не удалось собрать через Path, собираем через os.path
        result = os.path.join(*parts)
    
    return result


def is_windows_long_path_supported() -> bool:
    """
    Проверить, поддерживаются ли длинные пути в текущей системе Windows.
    
    Returns:
        True если длинные пути поддерживаются
    """
    if platform.system() != "Windows":
        return True
    
    # Сначала проверяем через реестр
    if enable_windows_long_paths():
        return True
    
    # Проверяем через попытку создания длинного пути
    import tempfile
    try:
        # Создаем временную директорию
        temp_dir = tempfile.gettempdir()
        
        # Генерируем длинное имя файла
        long_name = 'a' * 100 + '.txt'
        long_path = os.path.join(temp_dir, 'test_long_paths', *(['subdir'] * 10), long_name)
        
        # Создаем директории
        os.makedirs(os.path.dirname(long_path), exist_ok=True)
        
        # Пытаемся создать файл
        with open(long_path, 'w', encoding='utf-8') as f:
            f.write('test')
        
        # Проверяем, что файл создан
        if os.path.exists(long_path):
            os.remove(long_path)
            # Пытаемся удалить созданные директории
            try:
                os.removedirs(os.path.dirname(long_path))
            except OSError:
                pass
            return True
    except (OSError, IOError) as e:
        # Проверяем конкретную ошибку Windows
        if hasattr(e, 'winerror'):
            # ERROR_FILENAME_EXCED_RANGE = 206
            if e.winerror == 206:
                return False
    except Exception:
        pass
    
    return False


def get_extended_path(path: str) -> str:
    """
    Получить путь с префиксом \\?\ для обхода ограничений Windows MAX_PATH.
    
    Args:
        path: Исходный путь
        
    Returns:
        Путь с префиксом \\?\, если требуется
    """
    if platform.system() != "Windows":
        return path
    
    # Уже имеет префикс?
    if path.startswith('\\\\?\\'):
        return path
    
    # Абсолютный ли путь?
    try:
        abs_path = os.path.abspath(path)
    except Exception:
        abs_path = path
    
    # Для UNC путей (сетевых)
    if abs_path.startswith('\\\\'):
        if not abs_path.startswith('\\\\?\\UNC\\'):
            return '\\\\?\\UNC\\' + abs_path[2:]
    
    # Для локальных путей
    if len(abs_path) > 260 and not abs_path.startswith('\\\\?\\'):
        # Проверяем, является ли путь абсолютным
        if os.path.isabs(abs_path):
            if abs_path[1] == ':':  # Диск C:\
                return '\\\\?\\' + abs_path
        else:
            # Делаем абсолютным
            try:
                abs_path = os.path.abspath(abs_path)
                if abs_path[1] == ':':
                    return '\\\\?\\' + abs_path
            except Exception:
                pass
    
    return path


def create_windows_directory_safe(dir_path: str) -> bool:
    """
    Безопасное создание директории в Windows с учетом ограничений путей.
    
    Args:
        dir_path: Путь к директории
        
    Returns:
        True если директория создана или уже существует
    """
    try:
        # Нормализуем путь
        safe_path = normalize_windows_path(dir_path)
        
        # Проверяем длину пути
        if len(safe_path) > 240:  # Оставляем запас для вложенных файлов
            # Пытаемся использовать расширенный путь
            extended_path = get_extended_path(safe_path)
            if extended_path != safe_path:
                safe_path = extended_path
        
        # Создаем директорию
        os.makedirs(safe_path, exist_ok=True)
        return True
        
    except Exception as e:
        # Логируем ошибку
        print(f"Ошибка создания директории {dir_path}: {e}", file=sys.stderr)
        return False


# Экспорт функций
__all__ = [
    'get_windows_safe_path',
    'enable_windows_long_paths',
    'normalize_windows_path',
    'is_windows_long_path_supported',
    'get_extended_path',
    'create_windows_directory_safe'
]


# Тестирование функций (только при прямом запуске)
if __name__ == "__main__":
    if platform.system() == "Windows":
        print("=== Тестирование Windows Path Utils ===")
        
        # Тест нормализации пути
        test_path = 'C:\\test<file>.txt'
        normalized = normalize_windows_path(test_path)
        print(f"Нормализация пути '{test_path}' -> '{normalized}'")
        
        # Тест безопасного пути
        long_path = 'C:\\' + 'a' * 100 + '\\' + 'b' * 100 + '.txt'
        safe_path = get_windows_safe_path('C:\\', *['a' * 100, 'b' * 100 + '.txt'])
        print(f"Безопасный путь для длинного имени: {safe_path[:50]}...")
        
        # Проверка поддержки длинных путей
        long_supported = is_windows_long_path_supported()
        print(f"Длинные пути поддерживаются: {long_supported}")
        
        # Получение расширенного пути
        extended = get_extended_path('C:\\very\\long\\path\\' + 'a' * 200 + '.txt')
        print(f"Расширенный путь: {extended[:80]}...")
        
        print("=== Тестирование завершено ===")
    else:
        print("Этот модуль предназначен только для Windows")