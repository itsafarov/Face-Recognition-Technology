"""
Clean and improved configuration system
"""
import os
import psutil
import platform
from datetime import datetime
from typing import Dict, Any, Optional
try:
    from pydantic import BaseSettings
    from pydantic import Field
except ImportError:
    from pydantic_settings import BaseSettings
    from pydantic import Field


class AppConfig(BaseSettings):
    """Pydantic-based configuration with validation and type hints"""
    version: str = Field(default="13.0", description="Application version")
    thumbnail_size: tuple = Field(default=(120, 120), description="Thumbnail size for HTML reports")
    preview_size: tuple = Field(default=(300, 300), description="Preview size for viewing")
    image_quality: int = Field(default=85, ge=10, le=100, description="Image quality percentage")
    
    # Performance settings
    initial_batch_size: int = Field(default=1000, ge=100, le=50000, description="Initial batch size")
    max_workers: int = Field(default=15, ge=1, le=50, description="Max concurrent workers")
    request_timeout: int = Field(default=30, ge=5, le=120, description="Request timeout in seconds")
    request_retries: int = Field(default=2, ge=0, le=5, description="Request retry attempts")
    chunk_size: int = Field(default=2 * 1024 * 1024, ge=1024, le=100*1024*1024, description="File chunk size in bytes")
    
    # Checkpoint settings
    checkpoint_interval: int = Field(default=100000, ge=1000, le=1000000, description="Records between checkpoints")
    checkpoint_file: str = Field(default="processing_checkpoint.json", description="Checkpoint filename")
    
    # Memory settings
    max_memory_percent: int = Field(default=85, ge=50, le=95, description="Max memory usage percentage")
    memory_check_interval: int = Field(default=500, ge=100, le=10000, description="Memory check interval")
    
    # File size limits
    max_image_size_mb: int = Field(default=10, ge=1, le=50, description="Max image size in MB")
    max_cache_size_mb: int = Field(default=800, ge=50, le=2000, description="Max cache size in MB")
    
    # Directory structure
    base_dir: str = Field(default_factory=lambda: os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    input_folder: str = Field(default="input_data", description="Input data folder")
    output_folder: str = Field(default="output_results", description="Output results folder")
    image_folder: str = Field(default="photos", description="Image storage folder")
    reports_folder: str = Field(default="reports", description="Reports folder")
    cache_folder: str = Field(default="image_cache", description="Image cache folder")
    temp_folder: str = Field(default="temp", description="Temporary files folder")
    
    # Report settings
    html_report: str = Field(default="face_recognition_report.html", description="HTML report filename")
    pdf_report: str = Field(default="face_recognition_report.pdf", description="PDF report filename")
    excel_report: str = Field(default="face_recognition_data.xlsx", description="Excel report filename")
    summary_report: str = Field(default="processing_summary.json", description="Summary report filename")
    
    # Progress settings
    progress_update_interval: int = Field(default=1000, ge=100, le=10000, description="Progress update interval")
    detailed_progress_interval: int = Field(default=10000, ge=1000, le=100000, description="Detailed progress interval")
    
    # Platform-specific settings
    windows_max_workers: int = Field(default=12, ge=1, le=30)
    linux_max_workers: int = Field(default=25, ge=1, le=50)
    macos_max_workers: int = Field(default=20, ge=1, le=30)
    
    class Config:
        env_prefix = "FACE_RECOGNITION_"
        case_sensitive = False

    def get_optimal_settings(self) -> Dict[str, Any]:
        """Get optimal settings based on system resources"""
        memory_gb = psutil.virtual_memory().total / (1024**3)
        cpu_count = psutil.cpu_count(logical=True) or 4
        
        settings = {
            'max_workers': self.max_workers,
            'initial_batch_size': self.initial_batch_size,
            'max_cache_size_mb': self.max_cache_size_mb,
        }
        
        # Adjust based on available memory
        if memory_gb < 4:
            settings['max_workers'] = max(4, self.max_workers // 2)
            settings['initial_batch_size'] = min(2000, self.initial_batch_size)
            settings['max_cache_size_mb'] = min(200, self.max_cache_size_mb)
        elif memory_gb < 8:
            settings['max_workers'] = min(8, self.max_workers)
            settings['initial_batch_size'] = min(4000, self.initial_batch_size)
            settings['max_cache_size_mb'] = min(400, self.max_cache_size_mb)
        elif memory_gb >= 16:
            settings['max_workers'] = min(30, self.max_workers + 5)
            settings['initial_batch_size'] = min(15000, self.initial_batch_size * 2)
            settings['max_cache_size_mb'] = min(1200, self.max_cache_size_mb)
        
        # Adjust based on CPU cores
        if cpu_count < 4:
            settings['max_workers'] = max(2, settings['max_workers'] // 2)
        elif cpu_count > 16:
            settings['max_workers'] = min(25, settings['max_workers'] + 5)
        
        return settings

    def get_input_dir(self) -> str:
        """Get input directory path"""
        return os.path.join(self.base_dir, self.input_folder)

    def get_output_dir(self) -> str:
        """Get output directory path"""
        return os.path.join(self.base_dir, self.output_folder)

    def get_output_subdir(self, timestamp: Optional[str] = None) -> str:
        """Get output subdirectory with timestamp"""
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return os.path.join(self.get_output_dir(), f"results_{timestamp}")

    def setup_directories(self, output_path: Optional[str] = None) -> str:
        """Create directory structure"""
        if output_path is None:
            output_path = self.get_output_subdir()
        
        folders = [
            os.path.join(output_path, self.image_folder),
            os.path.join(output_path, self.reports_folder),
            os.path.join(output_path, self.cache_folder),
            os.path.join(output_path, self.temp_folder),
        ]
        
        for folder in folders:
            os.makedirs(folder, exist_ok=True)
        
        return output_path

    def ensure_base_directories(self):
        """Create base directories"""
        base_folders = [
            self.get_input_dir(),
            self.get_output_dir()
        ]
        
        for folder in base_folders:
            os.makedirs(folder, exist_ok=True)

    def get_system_info(self) -> Dict[str, Any]:
        """Get comprehensive system information"""
        memory = psutil.virtual_memory()
        disk_usage = psutil.disk_usage(self.base_dir)
        
        return {
            'os': platform.system(),
            'os_version': platform.release(),
            'python_version': platform.python_version(),
            'cpu_count': psutil.cpu_count(),
            'cpu_logical_count': psutil.cpu_count(logical=True),
            'memory_total_gb': memory.total / (1024**3),
            'memory_available_gb': memory.available / (1024**3),
            'memory_used_gb': memory.used / (1024**3),
            'memory_percent': memory.percent,
            'disk_total_gb': disk_usage.total / (1024**3),
            'disk_free_gb': disk_usage.free / (1024**3),
            'disk_used_gb': disk_usage.used / (1024**3),
            'disk_percent': disk_usage.percent
        }

    def print_config_summary(self):
        """Print configuration summary"""
        system_info = self.get_system_info()
        optimal_settings = self.get_optimal_settings()
        
        print("\n" + "="*80)
        print("⚙️  CONFIGURATION SUMMARY")
        print("="*80)
        
        print(f"📊 System: {system_info['os']} {system_info['os_version']}")
        print(f"🐍 Python: {system_info['python_version']}")
        print(f"💾 Memory: {system_info['memory_total_gb']:.1f} GB total")
        print(f"   Available: {system_info['memory_available_gb']:.1f} GB")
        print(f"   Used: {system_info['memory_percent']:.1f}%")
        print(f"💿 Disk: {system_info['disk_total_gb']:.1f} GB total")
        print(f"   Free: {system_info['disk_free_gb']:.1f} GB")
        print("─" * 80)
        
        print("⚡ Performance:")
        print(f"   Max workers: {optimal_settings['max_workers']}")
        print(f"   Initial batch size: {optimal_settings['initial_batch_size']:,}")
        print(f"   Max cache size: {optimal_settings['max_cache_size_mb']} MB")
        print(f"   Request timeout: {self.request_timeout} sec")
        print(f"   Request retries: {self.request_retries}")
        print("─" * 80)
        
        print("🔒 Security:")
        print(f"   Max memory usage: {self.max_memory_percent}%")
        print(f"   Checkpoint interval: {self.checkpoint_interval:,} records")
        print(f"   Max image size: {self.max_image_size_mb} MB")
        
        print("="*80)


# Global configuration instance
config = AppConfig()