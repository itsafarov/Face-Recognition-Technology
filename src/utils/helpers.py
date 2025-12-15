"""
Вспомогательные функции
"""

import os
import sys
import glob
import psutil
import platform
import shutil
import json
import time
from typing import Tuple, Dict, List, Optional, Any
from core.config import Config

def ensure_directories():
    """Создание необходимых директорий"""
    Config.ensure_base_directories()
    print(f"📁 Папка для входных данных: {Config.get_input_dir()}")
    print(f"📁 Папка для результатов: {Config.get_output_dir()}")
    print()

def print_banner():
    """Вывод баннера с информацией о системе"""
    # Очистка экрана
    if platform.system() == "Windows":
        os.system('cls')
    else:
        os.system('clear')
    
    banner = f"""
    ============================================================
                FACE RECOGNITION ANALYTICS SUITE
                          Версия {Config.VERSION}
    ============================================================

    Профессиональная система обработки данных распознавания лиц

    ============================================================
"""

    print(banner)

def select_file() -> str:
    """Выбор файла для обработки с детальной информацией"""
    input_dir = Config.get_input_dir()
    
    # Проверяем папку input_data
    if not os.path.exists(input_dir):
        print(f"⚠️  Папка input_data не существует. Создаю...")
        os.makedirs(input_dir, exist_ok=True)
        print(f"✅ Папка создана: {input_dir}")
    
    # Получаем список файлов
    files = get_available_files(input_dir)
    
    if not files:
        print_no_files_message(input_dir)
        return ""
    
    # Выводим список файлов
    display_files_list(files)
    
    # Получаем выбор пользователя
    return get_user_file_selection(files)

def get_available_files(input_dir: str) -> List[str]:
    """Получить список доступных файлов"""
    files = []
    for pattern in ['*.json', '*.jsonl', '*.txt']:
        files.extend(glob.glob(os.path.join(input_dir, pattern)))
    
    # Фильтруем файлы, удаляем пустые
    filtered_files = []
    for file in files:
        try:
            if os.path.getsize(file) > 0:
                filtered_files.append(file)
        except:
            continue
    
    return sorted(filtered_files, key=lambda x: os.path.getsize(x), reverse=True)

def print_no_files_message(input_dir: str):
    """Вывести сообщение об отсутствии файлов"""
    print("❌ Файлы не найдены в папке input_data")
    print(f"📁 Поместите JSON/JSONL файлы в папку: {input_dir}")
    
    print(f"\n📂 Текущая структура проекта:")
    print(f"  {os.path.basename(Config.BASE_DIR)}/")
    print(f"  ├── input_data/       {'← КЛАДИТЕ ФАЙЛЫ СЮДА':<40}")
    print(f"  ├── output_results/   {'← СЮДА СОХРАНЯТСЯ РЕЗУЛЬТАТЫ':<40}")
    print(f"  └── src/              {'← ИСХОДНЫЙ КОД':<40}")
    
    # Создаем пример файла
    create_example_file(input_dir)

def create_example_file(input_dir: str):
    """Создать пример файла с правильной структурой"""
    example_file = os.path.join(input_dir, "example_structure.txt")
    try:
        with open(example_file, 'w', encoding='utf-8') as f:
            f.write("""# Пример структуры JSON файла:
# Каждая строка - отдельный JSON объект

{"timestamp": {"$date": "2024-01-01T10:00:00Z"}, "device_id": "CAM001", "user_name": "Иван Иванов", "image": "http://example.com/photo.jpg"}
{"timestamp": {"$date": "2024-01-01T10:01:00Z"}, "device_id": "CAM001", "user_name": "Мария Петрова", "image": "http://example.com/photo2.jpg"}

# Поддерживаемые форматы: .json, .jsonl, .txt""")
        
        print(f"\n💡 Пример структуры файла создан в: {example_file}")
    except Exception as e:
        print(f"❌ Ошибка создания примера файла: {e}")

def display_files_list(files: List[str]):
    """Отобразить список файлов с группировкой по размеру"""
    print("\n📁 ВЫБОР ФАЙЛА ДЛЯ ОБРАБОТКИ")
    
    # Группируем файлы по размеру
    large_files = []
    medium_files = []
    small_files = []
    
    for file in files:
        size = os.path.getsize(file)
        if size > 1024**3:  # > 1 GB
            large_files.append(file)
        elif size > 100 * 1024**2:  # > 100 MB
            medium_files.append(file)
        else:
            small_files.append(file)
    
    # Выводим файлы по группам
    display_file_group("🔴 КРУПНЫЕ ФАЙЛЫ (>1 GB):", large_files, 0)
    display_file_group("🟡 СРЕДНИЕ ФАЙЛЫ (100 MB - 1 GB):", medium_files, len(large_files))
    display_file_group("🟢 МАЛЕНЬКИЕ ФАЙЛЫ (<100 MB):", small_files, len(large_files) + len(medium_files))

def display_file_group(title: str, files: List[str], start_index: int):
    """Отобразить группу файлов"""
    if not files:
        return
    
    print(f"\n{title}")
    for i, file in enumerate(files[:5], start_index + 1):
        filename = os.path.basename(file)
        size_str = format_file_size(os.path.getsize(file))
        print(f"  {i:2d}. {filename:40s} | {size_str:>10s}")
    
    if len(files) > 5:
        print(f"     ... и еще {len(files) - 5} файлов")

def get_user_file_selection(files: List[str]) -> str:
    """Получить выбор файла от пользователя"""
    while True:
        choice = input(f"\n👉 Выберите файл (1-{len(files)}) или введите путь к файлу: ").strip()
        
        if choice.lower() in ['q', 'выход', 'exit', 'quit']:
            return ""
        
        # Если введен путь к файлу
        if os.path.exists(choice):
            selected = choice
            break
        
        # Если введен номер
        if choice.isdigit() and 1 <= int(choice) <= len(files):
            selected = files[int(choice) - 1]
            break
        
        # Показать детали файла по номеру
        if choice.lower().startswith('info'):
            parts = choice.split()
            if len(parts) > 1 and parts[1].isdigit():
                file_num = int(parts[1])
                if 1 <= file_num <= len(files):
                    show_file_details(files[file_num - 1])
                    continue
        
        print(f"❌ Неверный выбор. Введите число от 1 до {len(files)} или путь к файлу")
        print("   Для информации о файле введите 'info <номер>'")
        print("   Для выхода введите 'q' или 'выход'")
    
    return process_selected_file(selected)

def process_selected_file(file_path: str) -> str:
    """Обработать выбранный файл"""
    print(f"\n✅ Выбран: {os.path.basename(file_path)}")
    file_size = os.path.getsize(file_path)
    
    # Показываем информацию о файле
    print(f"📊 Размер файла: {format_file_size(file_size)}")
    
    # Показываем рекомендации в зависимости от размера
    show_file_recommendations(file_size)
    
    return file_path

def show_file_recommendations(file_size: int):
    """Показать рекомендации в зависимости от размера файла"""
    if file_size > 1024**3:
        print("⏱️ Ориентировочное время обработки: 30-60 минут")
    elif file_size > 500 * 1024**2:
        print("⏱️ Ориентировочное время обработки: 10-30 минут")
    else:
        print("⏱️ Ориентировочное время обработки: 1-10 минут")

def show_file_details(file_path: str):
    """Показать детальную информацию о файле"""
    print("\n" + "="*80)
    print("📋 ДЕТАЛЬНАЯ ИНФОРМАЦИЯ О ФАЙЛЕ")
    print("="*80)
    
    try:
        filename = os.path.basename(file_path)
        size = os.path.getsize(file_path)
        modified = os.path.getmtime(file_path)
        created = os.path.getctime(file_path)
        
        modified_str = time.strftime("%d.%m.%Y %H:%M", time.localtime(modified))
        created_str = time.strftime("%d.%m.%Y %H:%M", time.localtime(created))
        
        print(f"📄 Имя файла: {filename}")
        print(f"📁 Полный путь: {file_path}")
        print(f"💾 Размер: {format_file_size(size)}")
        print(f"📅 Создан: {created_str}")
        print(f"📅 Изменен: {modified_str}")
        
        # Попробуем посчитать строки
        line_count = estimate_line_count(file_path)
        if line_count > 0:
            print(f"📊 Примерное количество записей: {line_count:,}")
        
        # Проверяем, является ли файл валидным JSON
        if check_json_validity(file_path):
            print("✅ Формат: Валидный JSON/JSONL")
        else:
            print("⚠️  Формат: Неизвестный (может содержать ошибки)")
        
        print("="*80)
        
    except Exception as e:
        print(f"❌ Ошибка получения информации о файле: {e}")
        print("="*80)

def estimate_line_count(file_path: str) -> int:
    """Оценить количество строк в файле"""
    try:
        line_count = 0
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for i, line in enumerate(f):
                if line.strip():
                    line_count += 1
                if i > 100000:  # Ограничим подсчет
                    line_count = 100000
                    break
        return line_count
    except:
        return 0

def check_json_validity(file_path: str) -> bool:
    """Проверить валидность JSON файла"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Пробуем прочитать первые 10 строк как JSON
            for i, line in enumerate(f):
                if line.strip():
                    json.loads(line.strip())
                if i >= 9:
                    break
        return True
    except:
        return False

def select_formats() -> list:
    """Выбор форматов отчетов с рекомендациями"""
    print("\n" + "="*80)
    print("                    ВЫБОР ФОРМАТОВ ОТЧЕТОВ")
    print("="*80)

    formats_info = get_formats_info()
    for key, info in formats_info.items():
        print(f"\n{key}. {info['name'].upper()}")
        print(f"   • {info['description']}")
        if key == "1":  # HTML
            print(f"   • Для файлов >1 GB — единственный оптимальный выбор")

    print("\n" + "="*80)
    
    return get_user_formats_selection(formats_info)

def get_formats_info() -> Dict[str, Dict[str, str]]:
    """Получить информацию о форматах отчетов"""
    return {
        "1": {
            "name": "HTML отчет",
            "description": "Все фото встроены в таблицу, фильтрация и поиск в браузере, предпросмотр фото при клике, статистика с графиками"
        },
        "2": {
            "name": "PDF отчет",
            "description": "Удобно для документации, только таблица без фото, ограничение: 200 записей"
        },
        "3": {
            "name": "Excel отчет",
            "description": "Полные данные в таблице, можно сортировать и фильтровать, ссылки на файлы фото"
        },
        "4": {
            "name": "JSON отчет",
            "description": "Статистика и метаданные, легко обрабатывать программами, минимальный размер"
        }
    }

def show_formats_recommendations():
    """Пустая функция для совместимости"""
    pass

def get_user_formats_selection(formats_info: Dict[str, Dict[str, str]]) -> list:
    """Получить выбор форматов от пользователя"""
    while True:
        choice = input("\n👉 Укажите форматы (например: 1): ").strip().lower()
        
        if choice in ['все', 'all', ''] and 'все' in formats_info:
            print("✅ Выбраны все форматы")
            return ["HTML", "PDF", "Excel", "JSON"]
        
        selected = []
        valid = True
        
        for part in choice.split(','):
            part = part.strip()
            if part in formats_info:
                format_name = formats_info[part]['name'].split()[0].upper()
                if format_name not in selected:
                    selected.append(format_name)
            else:
                print(f"❌ Неверный формат: {part}")
                valid = False
                break
        
        if valid and selected:
            print(f"✅ Выбрано: {', '.join(selected)} отчет")
            return selected
        elif valid:
            print("❌ Не выбрано ни одного формата")

def show_format_warnings(selected_formats: list):
    """Пустая функция для совместимости"""
    pass

def check_dependencies(selected_formats: list):
    """Проверка зависимостей с установкой при необходимости"""
    print("\n🔍 ПРОВЕРКА ЗАВИСИМОСТЕЙ...")
    
    # Основные зависимости
    required_deps = get_required_dependencies()
    
    # Опциональные зависимости
    optional_deps = get_optional_dependencies()
    
    missing = []
    optional_missing = []
    
    print("📦 Основные зависимости:")
    missing = check_required_dependencies(required_deps, missing)
    
    print("\n📦 Опциональные зависимости:")
    optional_missing = check_optional_dependencies(optional_deps, selected_formats, optional_missing)
    
    # Обработка отсутствующих зависимостей
    if missing:
        return handle_missing_dependencies(missing)
    
    if optional_missing:
        return handle_optional_dependencies(optional_missing, selected_formats)
    
    print("\n✅ Все зависимости готовы!")
    return True

def get_required_dependencies() -> Dict[str, str]:
    """Получить список обязательных зависимостей"""
    return {
        'aiohttp': 'pip install aiohttp',
        'aiofiles': 'pip install aiofiles',
        'numpy': 'pip install numpy',
        'Pillow': 'pip install Pillow',
        'opencv-python': 'pip install opencv-python',
        'psutil': 'pip install psutil'
    }

def get_optional_dependencies() -> Dict[str, tuple]:
    """Получить список опциональных зависимостей"""
    return {
        'reportlab': ('PDF', 'pip install reportlab'),
        'openpyxl': ('Excel', 'pip install openpyxl'),
    }

def check_required_dependencies(required_deps: Dict[str, str], missing: list) -> list:
    """Проверить обязательные зависимости"""
    for lib, cmd in required_deps.items():
        try:
            if lib == 'Pillow':
                __import__('PIL.Image')
            elif lib == 'opencv-python':
                __import__('cv2')
            else:
                __import__(lib)
            print(f"   ✅ {lib}")
        except ImportError:
            print(f"   ❌ {lib}")
            missing.append(cmd)
    
    return missing

def check_optional_dependencies(optional_deps: Dict[str, tuple], selected_formats: list, optional_missing: list) -> list:
    """Проверить опциональные зависимости"""
    for lib, (format_name, cmd) in optional_deps.items():
        if format_name in selected_formats:
            try:
                __import__(lib)
                print(f"   ✅ {lib} (для {format_name})")
            except ImportError:
                print(f"   ❌ {lib} (для {format_name})")
                optional_missing.append((format_name, cmd))
        else:
            print(f"   ⚪ {lib} (не требуется)")
    
    return optional_missing

def handle_missing_dependencies(missing: list) -> bool:
    """Обработать отсутствующие обязательные зависимости"""
    print(f"\n❌ Отсутствуют необходимые зависимости!")
    print("Установите командой:")
    for cmd in set(missing):
        print(f"   {cmd}")
    
    # Предлагаем установить автоматически
    if platform.system() == "Windows":
        return offer_automatic_installation(missing)
    
    return False

def offer_automatic_installation(missing: list) -> bool:
    """Предложить автоматическую установку зависимостей"""
    confirm = input("\n👉 Установить зависимости автоматически? (y/N): ").strip().lower()
    if confirm == 'y':
        print("Установка зависимостей...")
        for cmd in set(missing):
            print(f"Устанавливаю: {cmd}")
            os.system(cmd)
        
        # Повторная проверка
        print("\nПовторная проверка...")
        required_deps = get_required_dependencies()
        for lib, cmd in required_deps.items():
            try:
                if lib == 'Pillow':
                    __import__('PIL.Image')
                elif lib == 'opencv-python':
                    __import__('cv2')
                else:
                    __import__(lib)
            except ImportError:
                print(f"Не удалось установить {lib}. Установите вручную.")
                return False
        
        print("✅ Все зависимости успешно установлены!")
        return True
    else:
        return False

def handle_optional_dependencies(optional_missing: list, selected_formats: list) -> bool:
    """Обработать отсутствующие опциональные зависимости"""
    print(f"\n⚠️  Отсутствуют зависимости для некоторых форматов отчетов:")
    for format_name, cmd in optional_missing:
        print(f"   • {format_name}: {cmd}")
    
    confirm = input("\n👉 Продолжить без этих форматов? (y/N): ").strip().lower()
    if confirm != 'y':
        return False
    
    # Удаляем форматы, для которых нет зависимостей
    for format_name, _ in optional_missing:
        if format_name in selected_formats:
            selected_formats.remove(format_name)
            print(f"   📌 Формат {format_name} исключен из обработки")
    
    return True

def get_available_memory_info() -> Dict[str, float]:
    """Получить информацию о доступной памяти"""
    try:
        memory = psutil.virtual_memory()
        return {
            'total_gb': memory.total / 1024**3,
            'available_gb': memory.available / 1024**3,
            'used_gb': memory.used / 1024**3,
            'percent': memory.percent,
            'free_gb': memory.free / 1024**3
        }
    except:
        return {
            'total_gb': 0,
            'available_gb': 0,
            'used_gb': 0,
            'percent': 0,
            'free_gb': 0
        }

def get_disk_space_info() -> Dict[str, float]:
    """Получить информацию о свободном месте на диске"""
    try:
        project_path = Config.BASE_DIR
        disk_usage = shutil.disk_usage(project_path)
        
        return {
            'total_gb': disk_usage.total / 1024**3,
            'used_gb': disk_usage.used / 1024**3,
            'free_gb': disk_usage.free / 1024**3,
            'percent': (disk_usage.used / disk_usage.total) * 100
        }
    except:
        return {
            'total_gb': 0,
            'used_gb': 0,
            'free_gb': 0,
            'percent': 0
        }

def check_system_resources() -> bool:
    """Проверить системные ресурсы"""
    print("\n🔍 ПРОВЕРКА СИСТЕМНЫХ РЕСУРСОВ...")
    
    # Память
    memory_info = get_available_memory_info()
    print(f"   Память: {memory_info['total_gb']:.1f} GB всего")
    print(f"           {memory_info['available_gb']:.1f} GB доступно ({memory_info['percent']:.1f}% используется)")
    
    if memory_info['percent'] > 90:
        print("   ⚠️  Очень высокое использование памяти!")
        print("   Рекомендуется закрыть другие программы")
        return False
    elif memory_info['available_gb'] < 2:
        print("   ⚠️  Мало свободной памяти (<2 GB)")
        print("   Работа с большими файлами может быть медленной")
    
    # Диск
    disk_info = get_disk_space_info()
    print(f"   Диск: {disk_info['total_gb']:.1f} GB всего")
    print(f"         {disk_info['free_gb']:.1f} GB свободно ({disk_info['percent']:.1f}% используется)")
    
    if disk_info['free_gb'] < 10:
        print("   ⚠️  Мало свободного места на диске (<10 GB)")
        print("   Рекомендуется освободить место")
    
    # CPU
    try:
        cpu_percent = psutil.cpu_percent(interval=1)
        cpu_cores = psutil.cpu_count()
        print(f"   CPU: {cpu_cores} ядер, нагрузка: {cpu_percent:.1f}%")
        
        if cpu_percent > 90:
            print("   ⚠️  Высокая нагрузка на CPU!")
            print("   Работа может быть медленной")
    except:
        print("   CPU: информация недоступна")
    
    print("✅ Системные ресурсы в порядке")
    return True

def get_user_confirmation(prompt: str, default: str = 'n') -> bool:
    """Получить подтверждение от пользователя"""
    options = {'y': True, 'n': False}
    default_option = default.lower()
    
    while True:
        choice = input(f"{prompt} ({'Y/n' if default_option == 'y' else 'y/N'}): ").strip().lower()
        
        if choice == '':
            return options[default_option]
        elif choice in options:
            return options[choice]
        
        print("❌ Неверный выбор. Введите 'y' или 'n'")

def format_file_size(size_bytes: int) -> str:
    """Форматировать размер файла в читаемом виде"""
    if size_bytes >= 1024**3:
        return f"{size_bytes / 1024**3:.2f} GB"
    elif size_bytes >= 1024**2:
        return f"{size_bytes / 1024**2:.2f} MB"
    elif size_bytes >= 1024:
        return f"{size_bytes / 1024:.2f} KB"
    else:
        return f"{size_bytes} B"

def estimate_processing_time(file_size_bytes: int) -> str:
    """Оценить время обработки файла"""
    # Ориентировочная скорость: 1000 записей/сек
    # Средний размер записи: 500 байт
    try:
        estimated_records = file_size_bytes / 500
        estimated_seconds = estimated_records / 1000
        
        if estimated_seconds > 3600:
            hours = estimated_seconds / 3600
            return f"{hours:.1f} часов"
        elif estimated_seconds > 60:
            minutes = estimated_seconds / 60
            return f"{minutes:.1f} минут"
        else:
            return f"{estimated_seconds:.0f} секунд"
    except:
        return "неизвестно"

def show_processing_tips():
    """Пустая функция для совместимости"""
    pass

def cleanup_old_results(max_age_days: int = 7):
    """Очистка старых результатов"""
    output_dir = Config.get_output_dir()
    if not os.path.exists(output_dir):
        return
    
    current_time = time.time()
    
    for item in os.listdir(output_dir):
        item_path = os.path.join(output_dir, item)
        if os.path.isdir(item_path) and item.startswith("results_"):
            try:
                item_age = current_time - os.path.getmtime(item_path)
                if item_age > max_age_days * 24 * 3600:
                    size_mb = get_directory_size(item_path) / 1024**2
                    
                    confirm = get_user_confirmation(
                        f"Найдена старая папка результатов: {item} ({size_mb:.1f} MB). Удалить?",
                        default='n'
                    )
                    
                    if confirm:
                        shutil.rmtree(item_path)
                        print(f"✅ Удалено: {item}")
            except Exception as e:
                print(f"⚠️  Ошибка при проверке {item}: {e}")

def get_directory_size(directory: str) -> int:
    """Получить размер директории в байтах"""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(directory):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.exists(fp):
                total_size += os.path.getsize(fp)
    return total_size

def validate_file_path(file_path: str) -> Tuple[bool, str]:
    """Проверить валидность пути к файлу"""
    if not os.path.exists(file_path):
        return False, "Файл не существует"
    
    if not os.path.isfile(file_path):
        return False, "Указанный путь не является файлом"
    
    if os.path.getsize(file_path) == 0:
        return False, "Файл пуст"
    
    # Проверяем расширение файла
    valid_extensions = ['.json', '.jsonl', '.txt']
    file_ext = os.path.splitext(file_path)[1].lower()
    
    if file_ext not in valid_extensions:
        return False, f"Неподдерживаемое расширение файла: {file_ext}"
    
    return True, "Файл валиден"