#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Face Recognition Analytics Suite v13.0
Optimized version for large files with performance optimization
"""

import os
import sys
import platform
import asyncio
import argparse
import traceback
import time
import psutil

# Add modules path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# Import modules
from core.config import Config
from core.optimizer import (
    run_comprehensive_optimization,
    optimize_for_file_size,
    get_system_optimizer,
    get_memory_optimizer
)
from utils.logger import setup_logging
from utils.helpers import (
    print_banner,
    select_file,
    select_formats,
    check_dependencies,
    ensure_directories,
    get_available_memory_info,
    get_disk_space_info,
    validate_file_path,
    cleanup_old_results
)
from core.processor import FaceRecognitionProcessor
from core.optimized_processor import get_optimized_processor

# Setup logging
logger = setup_logging()

def parse_arguments():
    """Parse command line arguments with extended options"""
    parser = argparse.ArgumentParser(
        description=f'Face Recognition Analytics Suite v{Config.VERSION}',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python src/main.py                    # Normal run with interface
  python src/main.py --resume           # Resume interrupted processing
  python src/main.py --file data.json   # Process specific file
  python src/main.py --formats html,excel --batch-size 10000
  python src/main.py --skip-optimization # Skip optimization
  python src/main.py --max-workers 20 --memory-limit 90
        """
    )
    
    parser.add_argument('--resume', action='store_true',
                       help='Resume interrupted processing')
    parser.add_argument('--file', type=str,
                       help='Path to file for processing')
    parser.add_argument('--formats', type=str,
                       help='Report formats separated by comma (html,pdf,excel,json)')
    parser.add_argument('--batch-size', type=int,
                       help='Batch size for processing (max: 50000)')
    parser.add_argument('--max-workers', type=int,
                       help='Maximum number of parallel workers (max: 30)')
    parser.add_argument('--memory-limit', type=int,
                       help='Maximum memory usage in percentage (10-95)')
    parser.add_argument('--skip-optimization', action='store_true',
                       help='Skip system optimization')
    parser.add_argument('--cleanup-old', action='store_true',
                       help='Clean old results before running')
    parser.add_argument('--no-interactive', action='store_true',
                       help='Don\'t ask questions, use default values')
    parser.add_argument('--output-dir', type=str,
                       help='Custom results folder')
    parser.add_argument('--log-level', type=str, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='INFO', help='Log level')
    parser.add_argument('--benchmark', action='store_true',
                       help='Run performance test')
    parser.add_argument('--optimize-only', action='store_true',
                       help='Only optimize system')
    parser.add_argument('--new-processing', action='store_true',
                       help='Start new file processing (for batch file)')
    parser.add_argument('--show-menu', action='store_true',
                       help='Force show menu (default when no other arguments)')
    
    return parser.parse_args()

def print_system_info():
    """Print detailed system information"""
    memory_info = get_available_memory_info()
    disk_info = get_disk_space_info()
    
    print("📊 SYSTEM INFORMATION:")
    print(f"   • OS: {platform.system()} {platform.release()}")
    print(f"   • Architecture: {platform.architecture()[0]}")
    print(f"   • Python: {platform.python_version()} ({platform.python_implementation()})")
    
    # Safely get CPU information
    try:
        cpu_count = psutil.cpu_count(logical=False)
        cpu_logical = psutil.cpu_count(logical=True)
        cpu_percent = psutil.cpu_percent(interval=0.5)
        print(f"   • CPU Cores: {cpu_count} physical, {cpu_logical} logical")
        print(f"   • CPU Load: {cpu_percent:.1f}%")
    except:
        print("   • CPU: information unavailable")
    
    print(f"   • Memory: {memory_info['total_gb']:.1f} GB total")
    print(f"   • Available: {memory_info['available_gb']:.1f} GB ({memory_info['percent']:.1f}% used)")
    print(f"   • Disk: {disk_info['total_gb']:.1f} GB total")
    print(f"   • Free: {disk_info['free_gb']:.1f} GB")
    
    if memory_info['percent'] > 80:
        print("   ⚠️  Warning: high memory usage!")
    
    print()


def get_adaptive_config():
    """Get adaptive configuration based on system resources"""
    memory_info = get_available_memory_info()
    cpu_percent = psutil.cpu_percent(interval=0.5)
    
    config = {
        'batch_size': Config.INITIAL_BATCH_SIZE,
        'max_workers': Config.MAX_WORKERS,
        'memory_limit': Config.MAX_MEMORY_PERCENT
    }
    
    # Adapt based on memory load
    if memory_info['percent'] > 85:
        config['memory_limit'] = 70  # Reduce limit when high load
        config['batch_size'] = max(Config.INITIAL_BATCH_SIZE // 2, 500)  # Reduce batch
        config['max_workers'] = max(Config.MAX_WORKERS // 2, 4)  # Reduce workers
        print("   ⚠️  High memory usage - reducing load")
    elif memory_info['percent'] > 70:
        config['memory_limit'] = 80
        config['batch_size'] = max(Config.INITIAL_BATCH_SIZE // 1.5, 1000)
        config['max_workers'] = max(Config.MAX_WORKERS // 1.5, 6)
        print("   ⚠️  Medium memory load - moderate load reduction")
    else:
        # If system is idle, use more resources
        if cpu_percent < 20 and memory_info['available_gb'] > 4:
            config['batch_size'] = min(Config.INITIAL_BATCH_SIZE * 1.5, 15000)
            config['max_workers'] = min(Config.MAX_WORKERS * 1.2, 20)
            print("   ⚡ System is idle - increasing performance")
    
    # Adapt based on available memory
    if memory_info['available_gb'] < 2:
        config['batch_size'] = max(Config.INITIAL_BATCH_SIZE // 3, 500)
        config['max_workers'] = max(Config.MAX_WORKERS // 3, 2)
        print("   ⚠️  Low available memory - minimal load")
    
    return config

def find_resume_file() -> str:
    """Find file to resume processing"""
    output_dir = Config.get_output_dir()
    if not os.path.exists(output_dir):
        return ""
    
    # Look for results folders
    result_dirs = []
    for item in os.listdir(output_dir):
        item_path = os.path.join(output_dir, item)
        if os.path.isdir(item_path) and item.startswith("results_"):
            result_dirs.append(item_path)
    
    if not result_dirs:
        return ""
    
    # Sort by modification time (most recent first)
    result_dirs.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    
    # Check for checkpoint in the latest folder
    latest_dir = result_dirs[0]
    checkpoint_file = os.path.join(latest_dir, "checkpoint.json")
    
    if os.path.exists(checkpoint_file):
        return checkpoint_file
    
    return ""

async def interactive_setup(args):
    """Interactive setup of processing parameters"""
    print("\n" + "="*80)
    print("⚙️  PROCESSING PARAMETERS SETUP")
    print("="*80)
    
    # File selection
    if args.file:
        input_file = args.file
        if not os.path.exists(input_file):
            print(f"❌ File not found: {input_file}")
            return None
    else:
        input_file = select_file()
        if not input_file:
            print("❌ File not selected. Exiting.")
            return None
    
    # File validation
    is_valid, message = validate_file_path(input_file)
    if not is_valid:
        print(f"❌ {message}")
        return None
    
    # Format selection
    if args.formats:
        selected_formats = [f.strip().upper() for f in args.formats.split(',')]
        valid_formats = ['HTML', 'PDF', 'EXCEL', 'JSON']
        selected_formats = [f for f in selected_formats if f in valid_formats]
        if not selected_formats:
            print("❌ Invalid formats. Use: html, pdf, excel, json")
            return None
    else:
        selected_formats = select_formats()
        if not selected_formats:
            print("❌ Formats not selected. Exiting.")
            return None
    
    # Performance settings
    if not args.no_interactive:
        print("\n⚡ PERFORMANCE SETTINGS")
        print("-"*80)
        
        # Batch size
        if args.batch_size:
            batch_size = args.batch_size
        else:
            file_size_gb = os.path.getsize(input_file) / (1024**3)
            if file_size_gb > 10:
                suggested_batch = 1000
            elif file_size_gb > 5:
                suggested_batch = 2000
            elif file_size_gb > 1:
                suggested_batch = 4000
            else:
                suggested_batch = 8000
            
            try:
                batch_input = input(f"Batch size [{suggested_batch}]: ").strip()
                batch_size = int(batch_input) if batch_input else suggested_batch
            except:
                batch_size = suggested_batch
        
        # Max workers
        if args.max_workers:
            max_workers = args.max_workers
        else:
            memory_gb = psutil.virtual_memory().total / (1024**3)
            if memory_gb < 4:
                suggested_workers = 4
            elif memory_gb < 8:
                suggested_workers = 8
            elif memory_gb < 16:
                suggested_workers = 12
            else:
                suggested_workers = 16
            
            try:
                workers_input = input(f"Max parallel tasks [{suggested_workers}]: ").strip()
                max_workers = int(workers_input) if workers_input else suggested_workers
            except:
                max_workers = suggested_workers
        
        # Memory limit
        if args.memory_limit:
            memory_limit = args.memory_limit
        else:
            suggested_limit = 85
            try:
                limit_input = input(f"Max memory usage % [{suggested_limit}]: ").strip()
                memory_limit = int(limit_input) if limit_input else suggested_limit
            except:
                memory_limit = suggested_limit
        
        # Apply settings
        if 100 <= batch_size <= 50000:
            Config.INITIAL_BATCH_SIZE = batch_size
        if 1 <= max_workers <= 30:
            Config.MAX_WORKERS = max_workers
        if 10 <= memory_limit <= 95:
            Config.MAX_MEMORY_PERCENT = memory_limit
        
        print(f"✅ Set: Batch={batch_size}, Workers={max_workers}, Memory={memory_limit}%")
    else:
        # Automatic adaptive configuration based on system resources
        print("\n⚡ AUTOMATIC CONFIGURATION BASED ON SYSTEM RESOURCES")
        print("-"*80)
        
        adaptive_config = get_adaptive_config()
        
        # Apply adaptive settings
        Config.INITIAL_BATCH_SIZE = adaptive_config['batch_size']
        Config.MAX_WORKERS = adaptive_config['max_workers']
        Config.MAX_MEMORY_PERCENT = adaptive_config['memory_limit']
        
        print(f"✅ Automatically configured:")
        print(f"   • Batch size: {Config.INITIAL_BATCH_SIZE}")
        print(f"   • Max workers: {Config.MAX_WORKERS}")
        print(f"   • Memory limit: {Config.MAX_MEMORY_PERCENT}%")
    
    return {
        'input_file': input_file,
        'formats': selected_formats,
        'batch_size': Config.INITIAL_BATCH_SIZE,
        'max_workers': Config.MAX_WORKERS,
        'memory_limit': Config.MAX_MEMORY_PERCENT
    }

async def show_main_menu():
    """Show main menu"""
    while True:
        print("\n" + "="*80)
        print("🎭 MAIN MENU")
        print("="*80)
        print("1. 🚀 Start new file processing")
        print("2. 🔄 Continue interrupted processing")
        print("3. 🧹 Clean old results")
        print("4. ❌ Exit")
        print("="*80)
        
        choice = input("\n👉 Select action (1-4): ").strip()
        
        if choice == "1":
            return "new"
        elif choice == "2":
            return "resume"
        elif choice == "3":
            return "cleanup"
        elif choice == "4":
            print("Goodbye!")
            sys.exit(0)
        else:
            print("❌ Invalid choice. Please select 1-4.")


async def main():
    """Main function"""
    args = parse_arguments()
    
    # Setup logging level
    import logging
    log_level = getattr(logging, args.log_level)
    logger.setLevel(log_level)
    
    # Print system information
    print_banner()
    print_system_info()
    
    # Create required directories
    ensure_directories()
    
    # Check existence of main folders and create if needed
    input_dir = Config.get_input_dir()
    output_dir = Config.get_output_dir()
    
    missing_dirs = []
    if not os.path.exists(input_dir):
        missing_dirs.append(f"Input data folder: {input_dir}")
    if not os.path.exists(output_dir):
        missing_dirs.append(f"Results folder: {output_dir}")
    
    if missing_dirs:
        print("❌ Missing folders detected:")
        for missing_dir in missing_dirs:
            print(f"   • {missing_dir}")
        print()
        print("🔄 Creating missing folders...")
        
        try:
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(output_dir, exist_ok=True)
            print("✅ All required folders created successfully!")
        except Exception as e:
            print(f"❌ Error creating folders: {e}")
            print("⚠️  Please create folders manually and restart the program")
            input("Press Enter to exit...")
            sys.exit(1)
    
    # Determine working mode
    mode = None
    if args.resume:
        mode = "resume"
    elif args.cleanup_old:
        print("🧹 Cleaning old results...")
        cleanup_old_results()
        return
    elif args.benchmark or args.optimize_only:
        print("❌ This function is no longer supported")
        return
    elif args.new_processing:
        mode = "new"
    elif args.show_menu or (not any([args.resume, args.cleanup_old, args.benchmark, args.optimize_only, args.new_processing])):
        mode = await show_main_menu()
    else:
        # If arguments are passed but no specific task is specified, show menu
        mode = await show_main_menu()
    
    # Обработка выбранного режима
    if mode == "new":
        # Интерактивная настройка
        setup_result = await interactive_setup(args)
        if not setup_result:
            return
        
        input_file = setup_result['input_file']
        selected_formats = setup_result['formats']
        
        # Получаем информацию о файле
        file_size = os.path.getsize(input_file)
        file_size_gb = file_size / (1024**3)
        
        # Оптимизация под размер файла
        if not args.skip_optimization:
            print(f"\n🔄 Оптимизация под файл {file_size_gb:.2f} GB...")
            file_optimization = await optimize_for_file_size(file_size_gb)
            
            # Комплексная оптимизация системы
            print("🔄 Комплексная оптимизация системы...")
            optimization_results = await run_comprehensive_optimization()
        else:
            print("⏭️  Пропуск оптимизации системы")
        
        # Проверка зависимостей
        if not check_dependencies(selected_formats):
            if not args.no_interactive:
                input("\nНажмите Enter для выхода...")
            sys.exit(1)
        
        # Вывод финальной конфигурации
        print("\n" + "="*80)
        print("🚀 ФИНАЛЬНАЯ КОНФИГУРАЦИЯ")
        print("="*80)
        print(f"📂 Файл: {os.path.basename(input_file)}")
        print(f"📦 Размер: {file_size_gb:.2f} GB")
        print(f"📄 Форматы: {', '.join(selected_formats)}")
        print(f"🔄 Режим: НОВАЯ ОБРАБОТКА")
        print(f"⚡ Производительность:")
        print(f"   • Размер батча: {Config.INITIAL_BATCH_SIZE:,} записей")
        print(f"   • Макс. рабочих: {Config.MAX_WORKERS}")
        print(f"   • Лимит памяти: {Config.MAX_MEMORY_PERCENT}%")
        print(f"   • Таймаут: {Config.REQUEST_TIMEOUT} сек")
        print(f"   • Попыток: {Config.REQUEST_RETRIES}")
        
        # Оценка времени
        if file_size_gb > 10:
            time_estimate = "несколько часов"
        elif file_size_gb > 5:
            time_estimate = "1-2 часа"
        elif file_size_gb > 1:
            time_estimate = "30-60 минут"
        else:
            time_estimate = "5-30 минут"
        
        print(f"⏱️  Ориентировочное время: {time_estimate}")
        print("="*80)
        
        # Подтверждение запуска
        if not args.no_interactive:
            confirm = input("\n👉 Начать обработку? (y/N): ").strip().lower()
            if confirm != 'y':
                print("❌ Обработка отменена.")
                return
        
        print("\n⏳ Начало обработки...")
        print("   • Используется до 85% оперативной памяти")
        print("   • Прогресс сохраняется каждые 100,000 записей")
        print("   • При прерывании используйте --resume для продолжения")
        print("   • Размер батча будет динамически настраиваться")
        print("─" * 80)
        
    elif mode == "resume":
        print("\n🔄 РЕЖИМ ВОЗОБНОВЛЕНИЯ ОБРАБОТКИ")
        print("="*80)
        
        # Найти последний прерванный файл
        resume_file = find_resume_file()
        if not resume_file:
            print("❌ Не найден файл для возобновления обработки")
            print("💡 Убедитесь, что в папке output_results есть прерванный процесс")
            return
        
        print(f"📁 Найден файл для возобновления: {resume_file}")
        confirm = input("👉 Продолжить обработку? (y/N): ").strip().lower()
        if confirm != 'y':
            print("❌ Возобновление отменено.")
            return
        
        # Запуск процесса возобновления
        try:
            processor = FaceRecognitionProcessor([], resume=True)
            success = await processor.resume_processing(resume_file)
            
            if success:
                print("\n✅ Обработка успешно завершена!")
            else:
                print("\n⚠️  Обработка завершена с ошибками")
                
        except Exception as e:
            print(f"\n❌ Ошибка при возобновлении: {e}")
            traceback.print_exc()
        
        return
    
    elif mode == "cleanup":
        print("\n🗑️  ОЧИСТКА СТАРЫХ РЕЗУЛЬТАТОВ")
        print("="*80)
        cleanup_old_results()
        print("✅ Очистка завершена!")
        input("\nНажмите Enter для возврата в меню...")
        return
    
    # Создание и запуск процессора
    try:
        # Используем оптимизированный процессор для максимальной производительности
        if selected_formats == ["HTML"]:  # Если только HTML, используем оптимизированный процессор
            processor = get_optimized_processor(selected_formats, resume=args.resume)
        else:
            processor = FaceRecognitionProcessor(selected_formats, resume=args.resume)
        
        # Мониторинг памяти перед запуском
        memory_optimizer = get_memory_optimizer()
        memory_optimizer.monitor_memory_usage()
        
        success = await processor.process_file(input_file)
        
        if success:
            # Показать инструкции
            html_report = os.path.join(processor.output_dir, Config.REPORTS_FOLDER, Config.HTML_REPORT)
            if os.path.exists(html_report) and "HTML" in selected_formats:
                print("\n" + "="*80)
                print("📌 ИНСТРУКЦИЯ ПО ИСПОЛЬЗОВАНИЮ ОТЧЕТОВ")
                print("="*80)
                print("1. 📁 Откройте папку с результатами")
                print(f"   Путь: {processor.output_dir}")
                print("2. 🌐 Откройте HTML отчет в браузере")
                print("   • Используйте фильтры для поиска")
                print("   • Нажимайте на фото для увеличения")
                print("   • Экспортируйте в PDF или Excel")
                print("3. 📊 Для анализа используйте Excel отчет")
                print("4. 💾 Для повторной обработки используйте:")
                print(f"   python src/main.py --file \"{input_file}\" --resume")
                print("="*80)
                
                if not args.no_interactive:
                    choice = input("\n👉 Открыть HTML отчет в браузере? (y/N): ").strip().lower()
                    if choice == 'y':
                        try:
                            if platform.system() == "Windows":
                                os.startfile(html_report)
                            elif platform.system() == "Darwin":
                                os.system(f"open {html_report}")
                            else:
                                os.system(f"xdg-open {html_report}")
                            print("✅ Отчет открывается в браузере...")
                        except:
                            print(f"📎 Отчет находится здесь: {html_report}")
        
        # Отчет о производительности
        print("\n" + "="*80)
        print("📊 ОТЧЕТ О ПРОИЗВОДИТЕЛЬНОСТИ")
        print("="*80)
        
        performance_report = processor.get_performance_report()
        
        print(f"⏱️  Общее время обработки: {performance_report['processing']['processing_time_seconds']:.1f} сек")
        print(f"⚡ Средняя скорость: {performance_report['processing']['records_per_second']:.0f} записей/сек")
        print(f"📦 Обработано батчей: {performance_report['processing']['batches_processed']}")
        print(f"📊 Финальный размер батча: {performance_report['processing']['final_batch_size']}")
        print(f"🧠 Пиковое использование памяти: {performance_report['memory']['peak_memory_mb']:.1f} MB")
        print(f"🖼️  Успешных фото: {performance_report['images']['valid']:,}")
        print(f"📈 Успешность: {performance_report['images']['success_rate']:.1f}%")
        print("="*80)
        
        print("\n" + "="*80)
        print("✨ ОБРАБОТКА ЗАВЕРШЕНА!")
        print("="*80)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Обработка прервана пользователем")
        print("💡 Для продолжения запустите программу с ключом --resume")
        if not args.no_interactive:
            input("\nНажмите Enter для выхода...")
    except Exception as e:
        print(f"\n💥 Критическая ошибка: {e}")
        traceback.print_exc()
        if not args.no_interactive:
            input("\nНажмите Enter для выхода...")

def setup_asyncio_for_platform():
    """Настройка asyncio для разных платформ"""
    if platform.system() == "Windows":
        if sys.version_info >= (3, 8):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        else:
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    else:
        # Для Unix-систем используем uvloop если установлен
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            logger.info("Используется uvloop для улучшения производительности")
        except ImportError:
            pass

if __name__ == "__main__":
    try:
        # Настройка asyncio
        setup_asyncio_for_platform()
        
        # Check if help is requested first to avoid memory check
        if len(sys.argv) == 2 and sys.argv[1] in ['-h', '--help']:
            # Parse arguments to show help without memory check
            parse_arguments()
            # Exit immediately without going to finally block
            sys.exit(0)
        
        # Проверка версии Python
        if sys.version_info < (3, 7):
            print("❌ Требуется Python 3.7 или выше")
            sys.exit(1)
        
        # Проверка доступной памяти
        memory_info = get_available_memory_info()
        # Уменьшаем требования к памяти для тестирования
        if memory_info['available_gb'] < 0.1:  # Было 0.5
            print("❌ Недостаточно свободной памяти (<0.1 GB)")
            print("   Закройте другие программы и попробуйте снова")
            sys.exit(1)
        
        # Запуск
        asyncio.run(main())
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Программа прервана пользователем")
        sys.exit(0)
    except Exception as e:
        print(f"\n💥 Необработанная ошибка: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Очистка временных файлов
        try:
            import tempfile
            import glob
            
            # Удаляем временные файлы benchmark если есть
            for temp_file in glob.glob('benchmark_temp*'):
                try:
                    os.remove(temp_file)
                except:
                    pass
        except:
            pass
        
        if not sys.flags.interactive and not ('--help' in sys.argv or '-h' in sys.argv):
            input("\nНажмите Enter для выхода...")