"""
Clean and improved main application module with proper error handling
"""
import argparse
import asyncio
import os
import sys
import time
import traceback
import platform
from pathlib import Path
from typing import Optional, List, Dict, Any
import json

# Import core modules
from .core.config import config as app_config
from .core.data_parser import DataParser, ParserMetrics
from .core.checkpoint_manager import CheckpointManager, create_checkpoint_manager
from .processing.image_processor import ImageProcessorWithEmbedding, process_images_batch
from .utils.helpers import (
    ensure_directories,
    print_banner,
    select_file,
    select_formats,
    check_dependencies,
    check_system_resources,
    format_file_size,
    format_number,
    get_system_info
)
from .utils.logger import get_global_logger, setup_logging, log_system_info

# Setup logging
logger = get_global_logger()


class FaceRecognitionProcessor:
    """Main processor class with improved error handling and performance"""
    
    def __init__(self, output_dir: str, selected_formats: List[str]):
        self.output_dir = output_dir
        self.selected_formats = selected_formats
        self.checkpoint_manager = create_checkpoint_manager(output_dir)
        self.parser = DataParser()
        self.metrics = ParserMetrics()
        self.processing_stats = {
            'total_lines': 0,
            'processed_lines': 0,
            'valid_images': 0,
            'failed_images': 0,
            'json_errors': 0,
            'cached_images': 0,
            'network_errors': 0,
            'timeout_errors': 0,
            'duplicate_records': 0,
            'last_position': 0,
            'batch_size': app_config.initial_batch_size,
            'records_processed': [],
            'unique_users': set(),
            'unique_devices': set(),
            'unique_companies': set(),
            'unique_ips': set(),
            'start_time': time.time(),
            'last_update_time': time.time()
        }
        
        # Processing state
        self.is_processing = False
        self.resume_mode = False
        self.interrupted = False
    
    async def process_file(self, file_path: str):
        """Process the entire file with checkpoint support"""
        logger.info(f"Starting processing of {file_path}")
        
        # Load checkpoint if exists
        checkpoint = self.checkpoint_manager.load_checkpoint()
        if checkpoint:
            is_valid, error_msg = self.checkpoint_manager.validate_checkpoint(file_path)
            if is_valid:
                logger.info(f"Resuming from checkpoint: {checkpoint.processed_lines:,} records")
                self.resume_mode = True
                self._restore_from_checkpoint(checkpoint)
            else:
                logger.warning(f"Invalid checkpoint: {error_msg}")
        
        # Prepare output directories
        app_config.setup_directories(self.output_dir)
        
        # Get total file size
        total_size = os.path.getsize(file_path)
        logger.info(f"File size: {format_file_size(total_size)}")
        
        # Initialize image processor
        async with ImageProcessorWithEmbedding(self.output_dir) as image_processor:
            await self._process_file_with_image_processor(file_path, image_processor)
        
        # Generate reports
        await self._generate_reports()
        
        # Clean up
        self.checkpoint_manager.clear_checkpoint()
    
    def _restore_from_checkpoint(self, checkpoint):
        """Restore processing state from checkpoint"""
        self.processing_stats.update({
            'processed_lines': checkpoint.processed_lines,
            'valid_images': checkpoint.valid_images,
            'failed_images': checkpoint.failed_images,
            'json_errors': checkpoint.json_errors,
            'cached_images': checkpoint.cached_images,
            'network_errors': checkpoint.network_errors,
            'timeout_errors': checkpoint.timeout_errors,
            'duplicate_records': checkpoint.duplicate_records,
            'last_position': checkpoint.last_position,
            'batch_size': checkpoint.batch_size,
            'records_processed': checkpoint.records_processed,
            'unique_users': set(checkpoint.unique_users),
            'unique_devices': set(checkpoint.unique_devices),
            'unique_companies': set(checkpoint.unique_companies),
            'unique_ips': set(checkpoint.unique_ips),
        })
    
    async def _process_file_with_image_processor(self, file_path: str, image_processor: ImageProcessorWithEmbedding):
        """Process file with image processor"""
        # Get total lines
        self.processing_stats['total_lines'] = self._count_lines(file_path)
        logger.info(f"Total records to process: {format_number(self.processing_stats['total_lines'])}")
        
        # Open file
        with open(file_path, 'r', encoding='utf-8') as f:
            # If resuming, seek to checkpoint position
            if self.resume_mode and self.processing_stats['last_position'] > 0:
                f.seek(self.processing_stats['last_position'])
            
            batch = []
            batch_num = 0
            
            for line_num, line in enumerate(f, 1):
                if self.interrupted:
                    logger.info("Processing interrupted by user")
                    break
                
                # Skip already processed lines in resume mode
                if self.resume_mode and line_num <= self.processing_stats['processed_lines']:
                    continue
                
                line = line.strip()
                if not line:
                    continue
                
                batch.append(line)
                
                # Process batch when full
                if len(batch) >= self.processing_stats['batch_size']:
                    await self._process_batch(batch, image_processor)
                    batch = []
                    batch_num += 1
                    
                    # Update checkpoint
                    self._update_checkpoint(file_path)
                    
                    # Update progress
                    self._update_progress()
            
            # Process remaining batch
            if batch:
                await self._process_batch(batch, image_processor)
                self._update_checkpoint(file_path)
                self._update_progress()
    
    def _count_lines(self, file_path: str) -> int:
        """Count total lines in file"""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                return sum(1 for _ in f if _.strip())
        except Exception as e:
            logger.error(f"Error counting lines: {e}")
            return 0
    
    async def _process_batch(self, batch: List[str], image_processor: ImageProcessorWithEmbedding):
        """Process a batch of records"""
        # Parse records
        parsed_records = self.parser.parse_batch(batch, self.metrics)
        
        # Extract image URLs
        image_urls = []
        for record in parsed_records:
            if record.get('image_url'):
                image_urls.append(record['image_url'])
        
        # Process images if any
        if image_urls:
            try:
                image_results = await process_images_batch(image_processor, image_urls, self.metrics)
                
                # Update stats
                for result in image_results:
                    if result.filepath and result.base64_str:
                        self.processing_stats['valid_images'] += 1
                    else:
                        self.processing_stats['failed_images'] += 1
            except Exception as e:
                logger.error(f"Error processing images: {e}")
                self.processing_stats['network_errors'] += len(image_urls)
        
        # Update other stats
        self.processing_stats['processed_lines'] += len(batch)
        self.processing_stats['json_errors'] += len(batch) - len(parsed_records)
        
        # Update unique collections
        for record in parsed_records:
            if record.get('user_name'):
                self.processing_stats['unique_users'].add(record['user_name'])
            if record.get('device_id'):
                self.processing_stats['unique_devices'].add(record['device_id'])
            if record.get('company_id'):
                self.processing_stats['unique_companies'].add(record['company_id'])
            if record.get('ip_address'):
                self.processing_stats['unique_ips'].add(record['ip_address'])
    
    def _update_checkpoint(self, file_path: str):
        """Update checkpoint"""
        if not self.checkpoint_manager.should_save_checkpoint(
            self.processing_stats['processed_lines'] - 
            (self.checkpoint_manager.state.processed_lines if self.checkpoint_manager.state else 0)
        ):
            return
        
        success = self.checkpoint_manager.save_checkpoint(
            file_name=os.path.basename(file_path),
            total_lines=self.processing_stats['total_lines'],
            processed_lines=self.processing_stats['processed_lines'],
            valid_images=self.processing_stats['valid_images'],
            failed_images=self.processing_stats['failed_images'],
            json_errors=self.processing_stats['json_errors'],
            cached_images=self.processing_stats['cached_images'],
            network_errors=self.processing_stats['network_errors'],
            timeout_errors=self.processing_stats['timeout_errors'],
            duplicate_records=self.processing_stats['duplicate_records'],
            last_position=0,  # We're not tracking file position in this simplified version
            batch_size=self.processing_stats['batch_size'],
            records_processed=self.processing_stats['records_processed'],
            unique_users=list(self.processing_stats['unique_users']),
            unique_devices=list(self.processing_stats['unique_devices']),
            unique_companies=list(self.processing_stats['unique_companies']),
            unique_ips=list(self.processing_stats['unique_ips'])
        )
        
        if success:
            logger.debug(f"Checkpoint saved at {self.processing_stats['processed_lines']:,} records")
    
    def _update_progress(self):
        """Update progress information"""
        elapsed_time = time.time() - self.processing_stats['start_time']
        processed = self.processing_stats['processed_lines']
        total = self.processing_stats['total_lines']
        
        if total > 0:
            progress_percent = (processed / total) * 100
            records_per_sec = processed / elapsed_time if elapsed_time > 0 else 0
            remaining = total - processed
            eta_seconds = remaining / records_per_sec if records_per_sec > 0 else 0
            
            # Update every 5 seconds or every 1000 records
            current_time = time.time()
            if (current_time - self.processing_stats['last_update_time'] >= 5 or 
                processed % 1000 == 0):
                
                print(f"\r📊 Прогресс: {progress_percent:.1f}% ({format_number(processed):>8s}/{format_number(total):>8s}) "
                      f"⚡ {records_per_sec:.1f} rec/s "
                      f"⏱️ ETA: {self._format_time(eta_seconds):>8s}", end='', flush=True)
                
                self.processing_stats['last_update_time'] = current_time
    
    def _format_time(self, seconds: float) -> str:
        """Format time in human readable format"""
        if seconds < 0:
            return "N/A"
        elif seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}m"
        else:
            hours = seconds / 3600
            return f"{hours:.1f}h"
    
    async def _generate_reports(self):
        """Generate output reports"""
        logger.info("Generating reports...")
        
        # Create summary report
        summary = {
            'processing_summary': {
                'total_records': self.processing_stats['processed_lines'],
                'valid_images': self.processing_stats['valid_images'],
                'failed_images': self.processing_stats['failed_images'],
                'json_errors': self.processing_stats['json_errors'],
                'unique_users': len(self.processing_stats['unique_users']),
                'unique_devices': len(self.processing_stats['unique_devices']),
                'unique_companies': len(self.processing_stats['unique_companies']),
                'processing_time_seconds': time.time() - self.processing_stats['start_time'],
                'records_per_second': (
                    self.processing_stats['processed_lines'] / 
                    (time.time() - self.processing_stats['start_time'])
                    if time.time() - self.processing_stats['start_time'] > 0 else 0
                )
            },
            'selected_formats': self.selected_formats,
            'config': app_config.get_optimal_settings()
        }
        
        # Save summary
        summary_file = os.path.join(self.output_dir, app_config.summary_report)
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Summary report saved to: {summary_file}")
        
        # Generate format-specific reports based on selection
        for fmt in self.selected_formats:
            if fmt == "HTML":
                await self._generate_html_report()
            elif fmt == "PDF":
                await self._generate_pdf_report()
            elif fmt == "Excel":
                await self._generate_excel_report()
            elif fmt == "JSON":
                await self._generate_json_report()
    
    async def _generate_html_report(self):
        """Generate HTML report"""
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Face Recognition Report</title>
    <meta charset="utf-8">
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .thumbnail {{ width: 120px; height: 120px; object-fit: cover; }}
        .stats {{ background-color: #f9f9f9; padding: 15px; margin: 10px 0; border-radius: 5px; }}
    </style>
</head>
<body>
    <h1>Face Recognition Report</h1>
    
    <div class="stats">
        <h2>Processing Statistics</h2>
        <p>Total Records: {format_number(self.processing_stats['processed_lines'])}</p>
        <p>Valid Images: {format_number(self.processing_stats['valid_images'])}</p>
        <p>Failed Images: {format_number(self.processing_stats['failed_images'])}</p>
        <p>Unique Users: {format_number(len(self.processing_stats['unique_users']))}</p>
        <p>Unique Devices: {format_number(len(self.processing_stats['unique_devices']))}</p>
    </div>
    
    <h2>Sample Data</h2>
    <table>
        <thead>
            <tr>
                <th>Timestamp</th>
                <th>Device ID</th>
                <th>User Name</th>
                <th>Gender</th>
                <th>Age</th>
                <th>Score</th>
                <th>Image</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>2024-01-01 10:00:00</td>
                <td>CAM001</td>
                <td>Иван Иванов</td>
                <td>Мужской</td>
                <td>30</td>
                <td>95.5%</td>
                <td><img src="data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAYEBQYFBAYGBQYHBwYIChAKCgkJChQODwwQFxQYGBcUFhYaHSUfGhsjHBYWICwgIyYnKSopGR8tMC0oMCUoKSj/2wBDAQcHBwoIChMKChMoGhYaKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCj/wAARCAABAAEDASIAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAv/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/8QAFQEBAQAAAAAAAAAAAAAAAAAAAAX/xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oADAMBAAIRAxEAPwAfgA==" class="thumbnail" alt="Sample Image"></td>
            </tr>
        </tbody>
    </table>
</body>
</html>
"""
        
        html_file = os.path.join(self.output_dir, app_config.reports_folder, app_config.html_report)
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"HTML report saved to: {html_file}")
    
    async def _generate_pdf_report(self):
        """Generate PDF report placeholder"""
        logger.info("PDF report generation skipped (requires reportlab)")
    
    async def _generate_excel_report(self):
        """Generate Excel report placeholder"""
        logger.info("Excel report generation skipped (requires openpyxl)")
    
    async def _generate_json_report(self):
        """Generate JSON report with statistics"""
        json_data = {
            'metadata': {
                'generated_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                'total_records': self.processing_stats['processed_lines'],
                'valid_images': self.processing_stats['valid_images'],
                'failed_images': self.processing_stats['failed_images']
            },
            'statistics': {
                'unique_users': len(self.processing_stats['unique_users']),
                'unique_devices': len(self.processing_stats['unique_devices']),
                'unique_companies': len(self.processing_stats['unique_companies']),
                'processing_time_seconds': time.time() - self.processing_stats['start_time']
            }
        }
        
        json_file = os.path.join(self.output_dir, app_config.reports_folder, 'statistics.json')
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"JSON report saved to: {json_file}")


async def run_processing(input_file: str, selected_formats: List[str], output_dir: str):
    """Run the main processing workflow"""
    logger.info("Starting face recognition processing...")
    
    # Create processor
    processor = FaceRecognitionProcessor(output_dir, selected_formats)
    
    try:
        # Process file
        await processor.process_file(input_file)
        
        # Print final statistics
        print("\n" + "="*80)
        print("📊 ОКОНЧАТЕЛЬНАЯ СТАТИСТИКА")
        print("="*80)
        print(f"Обработано записей: {format_number(processor.processing_stats['processed_lines']):>15s}")
        print(f"Успешных фото:     {format_number(processor.processing_stats['valid_images']):>15s}")
        print(f"Ошибок фото:       {format_number(processor.processing_stats['failed_images']):>15s}")
        print(f"Ошибок JSON:       {format_number(processor.processing_stats['json_errors']):>15s}")
        print(f"Уникальных юзеров: {format_number(len(processor.processing_stats['unique_users'])):>15s}")
        print(f"Уникальных устр.:  {format_number(len(processor.processing_stats['unique_devices'])):>15s}")
        print("="*80)
        
        logger.info("Processing completed successfully")
        return True
        
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
        print("\n⚠️  Обработка прервана пользователем")
        return False
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        traceback.print_exc()
        print(f"\n❌ Ошибка обработки: {e}")
        return False


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Face Recognition Analytics Suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python main.py --file data.json --format HTML
  python main.py --file data.json --format HTML,Excel --output results_2024
  python main.py --resume
        """
    )
    
    parser.add_argument(
        '--file',
        type=str,
        help='Путь к входному файлу JSON/JSONL'
    )
    
    parser.add_argument(
        '--format',
        type=str,
        help='Форматы отчетов (HTML,PDF,Excel,JSON) через запятую'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Папка для результатов (по умолчанию генерируется автоматически)'
    )
    
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Продолжить прерванную обработку'
    )
    
    parser.add_argument(
        '--no-interaction',
        action='store_true',
        help='Режим без интерактивных запросов'
    )
    
    parser.add_argument(
        '--no-check',
        action='store_true',
        help='Пропустить проверку зависимостей'
    )
    
    return parser.parse_args()


def setup_asyncio_for_platform():
    """Setup asyncio for different platforms"""
    if platform.system() == "Windows":
        if sys.version_info >= (3, 8):
            import asyncio
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    else:
        # For Unix systems, try to use uvloop if available
        try:
            import uvloop
            uvloop.install()
        except ImportError:
            pass


async def main():
    """Main application entry point"""
    print_banner()
    
    # Parse arguments
    args = parse_arguments()
    
    # Setup asyncio
    setup_asyncio_for_platform()
    
    # Check Python version
    if sys.version_info < (3, 7):
        print("❌ Требуется Python 3.7 или выше")
        return False
    
    # Ensure directories exist
    ensure_directories()
    
    # Check system resources
    if not check_system_resources():
        print("⚠️  Системные ресурсы не подходят для обработки")
        if not args.no_interaction:
            input("\nНажмите Enter для продолжения...")
        return False
    
    # Get input file
    if args.file:
        input_file = args.file
    else:
        input_file = select_file()
        if not input_file:
            print("❌ Файл не выбран")
            return False
    
    # Validate file
    is_valid, error_msg = validate_file_path(input_file)
    if not is_valid:
        print(f"❌ Ошибка валидации файла: {error_msg}")
        return False
    
    # Get selected formats
    if args.format:
        selected_formats = [fmt.strip().upper() for fmt in args.format.split(',')]
    else:
        selected_formats = select_formats()
        if not selected_formats:
            print("❌ Форматы не выбраны")
            return False
    
    # Check dependencies if not skipped
    if not args.no_check:
        if not check_dependencies(selected_formats):
            print("❌ Проверка зависимостей не пройдена")
            return False
    
    # Get output directory
    if args.output:
        output_dir = args.output
    else:
        output_dir = app_config.get_output_subdir()
    
    print(f"\n📁 Папка результатов: {output_dir}")
    
    # Confirm before starting
    if not args.no_interaction:
        confirm = input("\n👉 Начать обработку? (y/N): ").strip().lower()
        if confirm != 'y':
            print("❌ Обработка отменена")
            return False
    
    # Run processing
    success = await run_processing(input_file, selected_formats, output_dir)
    
    if success:
        print(f"\n✅ Обработка завершена успешно!")
        print(f"📁 Результаты сохранены в: {output_dir}")
        
        # Show results location
        reports_dir = os.path.join(output_dir, app_config.reports_folder)
        if os.path.exists(reports_dir):
            print(f"📊 Отчеты: {reports_dir}")
        
        images_dir = os.path.join(output_dir, app_config.image_folder)
        if os.path.exists(images_dir):
            print(f"🖼️  Фото: {images_dir}")
    else:
        print(f"\n⚠️  Обработка завершена с ошибками")
        print(f"📁 Результаты могут быть частично сохранены в: {output_dir}")
    
    return success


def validate_file_path(file_path: str) -> tuple[bool, str]:
    """Validate file path"""
    if not os.path.exists(file_path):
        return False, "Файл не существует"
    
    if not os.path.isfile(file_path):
        return False, "Указанный путь не является файлом"
    
    if os.path.getsize(file_path) == 0:
        return False, "Файл пуст"
    
    # Check file extension
    valid_extensions = ['.json', '.jsonl', '.txt']
    file_ext = os.path.splitext(file_path)[1].lower()
    
    if file_ext not in valid_extensions:
        return False, f"Неподдерживаемое расширение файла: {file_ext}"
    
    return True, "Файл валиден"


if __name__ == "__main__":
    try:
        # Run main function
        success = asyncio.run(main())
        
        if not success:
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Программа прервана пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Необработанная ошибка: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        if 'main' in sys.argv or '--help' not in sys.argv:
            input("\nНажмите Enter для выхода...")