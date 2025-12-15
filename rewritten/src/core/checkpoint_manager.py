"""
Clean and improved checkpoint manager with enhanced reliability
"""
import os
import json
import time
import shutil
import hashlib
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path

# Import configuration
from .config import config as app_config

# Setup logging
logger = logging.getLogger(__name__)


@dataclass
class CheckpointState:
    """Checkpoint state with validation"""
    file_name: str = ""
    total_lines: int = 0
    processed_lines: int = 0
    valid_images: int = 0
    failed_images: int = 0
    json_errors: int = 0
    cached_images: int = 0
    network_errors: int = 0
    timeout_errors: int = 0
    duplicate_records: int = 0
    last_position: int = 0  # File position in bytes
    timestamp: float = field(default_factory=time.time)
    batch_size: int = field(default_factory=lambda: app_config.initial_batch_size)
    records_processed: List[str] = field(default_factory=list)
    unique_users: List[str] = field(default_factory=list)
    unique_devices: List[str] = field(default_factory=list)
    unique_companies: List[str] = field(default_factory=list)
    unique_ips: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        """Post initialization validation"""
        self._normalize_numeric_fields()
        self._validate_data()
    
    def _normalize_numeric_fields(self):
        """Normalize numeric fields"""
        # Convert batch_size to int and check bounds
        try:
            self.batch_size = int(self.batch_size)
            if self.batch_size < 100:
                self.batch_size = app_config.initial_batch_size
            elif self.batch_size > 50000:
                self.batch_size = 50000
        except (ValueError, TypeError):
            self.batch_size = app_config.initial_batch_size
        
        # Ensure other numeric fields are correct
        numeric_fields = [
            'total_lines', 'processed_lines', 'valid_images', 
            'failed_images', 'json_errors', 'cached_images',
            'network_errors', 'timeout_errors', 'duplicate_records',
            'last_position'
        ]
        
        for field_name in numeric_fields:
            value = getattr(self, field_name, 0)
            try:
                setattr(self, field_name, int(float(value)))
            except (ValueError, TypeError):
                setattr(self, field_name, 0)
        
        # Ensure timestamp is float
        try:
            self.timestamp = float(self.timestamp)
        except (ValueError, TypeError):
            self.timestamp = time.time()
    
    def _validate_data(self):
        """Validate data integrity"""
        # Check integrity
        if self.processed_lines > self.total_lines > 0:
            logger.warning(f"Processed lines ({self.processed_lines:,}) > total lines ({self.total_lines:,})")
            self.processed_lines = min(self.processed_lines, self.total_lines)
        
        if self.last_position < 0:
            logger.warning(f"Invalid position: {self.last_position:,}")
            self.last_position = 0
        
        # Check image statistics
        total_images = self.valid_images + self.failed_images
        if total_images > self.processed_lines:
            logger.warning(f"Image count ({total_images}) > processed lines ({self.processed_lines})")
    
    @property
    def progress_percent(self) -> float:
        """Completion percentage"""
        if self.total_lines == 0:
            return 0.0
        return (self.processed_lines / self.total_lines) * 100
    
    @property
    def age_seconds(self) -> float:
        """Checkpoint age in seconds"""
        return time.time() - self.timestamp
    
    @property
    def age_hours(self) -> float:
        """Checkpoint age in hours"""
        return self.age_seconds / 3600
    
    def is_expired(self, max_age_hours: float = 168) -> bool:
        """Check if checkpoint is expired (default 7 days)"""
        return self.age_hours > max_age_hours
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        data = asdict(self)
        
        # Add computed fields
        data['progress_percent'] = self.progress_percent
        data['age_seconds'] = self.age_seconds
        data['age_hours'] = self.age_hours
        data['is_expired'] = self.is_expired()
        
        # Format timestamps
        if self.timestamp > 0:
            dt = datetime.fromtimestamp(self.timestamp)
            data['timestamp_human'] = dt.strftime("%Y-%m-%d %H:%M:%S")
            data['timestamp_iso'] = dt.isoformat()
        
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CheckpointState':
        """Create object from dictionary"""
        # Filter only dataclass fields
        field_names = {f.name for f in cls.__dataclass_fields__}
        filtered_data = {k: v for k, v in data.items() if k in field_names}
        return cls(**filtered_data)


class CheckpointManager:
    """
    Checkpoint management for resuming processing with enhanced reliability
    
    Features:
    - Atomic save operations
    - Data integrity checks
    - Automatic recovery from backups
    - Version control for format
    - Caching for fast access
    """
    
    CHECKPOINT_VERSION = 1
    
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.checkpoint_file = os.path.join(output_dir, app_config.checkpoint_file)
        self.checkpoint_temp = f"{self.checkpoint_file}.tmp"
        self.checkpoint_backup = f"{self.checkpoint_file}.backup"
        self.checkpoint_archive = f"{self.checkpoint_file}.archive"
        
        self.state: Optional[CheckpointState] = None
        self.last_save = 0.0
        self.save_count = 0
        self.checksum: Optional[str] = None
        
        # Cache for fast access
        self._cache: Dict[str, tuple] = {}
        self._cache_ttl = 60  # seconds
        
        # Stats
        self.stats = {
            'loads': 0,
            'saves': 0,
            'backup_restores': 0,
            'integrity_errors': 0,
            'last_operation': None
        }
        
        # Auto-create directory if needed
        os.makedirs(os.path.dirname(self.checkpoint_file), exist_ok=True)
        
        logger.info(f"Initialized CheckpointManager: {self.checkpoint_file}")
    
    def _update_stats(self, operation: str):
        """Update operation statistics"""
        self.stats['last_operation'] = operation
        key = f'{operation}s'
        self.stats[key] = self.stats.get(key, 0) + 1
    
    def _generate_checksum(self, data: Dict[str, Any]) -> str:
        """Generate checksum for data"""
        try:
            data_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
            return hashlib.sha256(data_str.encode()).hexdigest()[:32]
        except Exception as e:
            logger.error(f"Error generating checksum: {e}")
            return "0" * 32
    
    def _calculate_file_checksum(self, filepath: str) -> Optional[str]:
        """Calculate file checksum"""
        if not os.path.exists(filepath):
            return None
        
        try:
            with open(filepath, 'rb') as f:
                file_hash = hashlib.sha256()
                for chunk in iter(lambda: f.read(4096), b""):
                    file_hash.update(chunk)
                return file_hash.hexdigest()
        except Exception as e:
            logger.warning(f"Error calculating file checksum {filepath}: {e}")
            return None
    
    def validate_checkpoint_integrity(self, checkpoint_data: Dict[str, Any]) -> bool:
        """Validate checkpoint data integrity"""
        try:
            required_fields = {
                'file_name', 'total_lines', 'processed_lines', 
                'last_position', 'timestamp', 'batch_size'
            }
            
            # Check for required fields
            missing_fields = required_fields - set(checkpoint_data.keys())
            if missing_fields:
                logger.warning(f"Checkpoint missing required fields: {missing_fields}")
                return False
            
            # Check data types
            type_checks = [
                ('processed_lines', (int, float)),
                ('total_lines', (int, float)),
                ('last_position', (int, float)),
                ('batch_size', (int, float)),
                ('timestamp', (int, float)),
            ]
            
            for field_name, expected_types in type_checks:
                value = checkpoint_data.get(field_name)
                if not isinstance(value, expected_types):
                    logger.warning(f"Invalid type {field_name}: {type(value)}")
                    return False
            
            # Check logical integrity
            if checkpoint_data['processed_lines'] > checkpoint_data['total_lines']:
                logger.warning(f"Processed lines ({checkpoint_data['processed_lines']:,}) > total lines ({checkpoint_data['total_lines']:,})")
                return False
            
            if checkpoint_data['last_position'] < 0:
                logger.warning(f"Invalid position: {checkpoint_data['last_position']:,}")
                return False
            
            # Check age (warn for old checkpoints)
            checkpoint_age = time.time() - checkpoint_data['timestamp']
            if checkpoint_age > 30 * 24 * 3600:  # 30 days
                logger.warning(f"Checkpoint is very old: {checkpoint_age/3600/24:.1f} days")
            elif checkpoint_age > 7 * 24 * 3600:  # 7 days
                logger.info(f"Checkpoint is old: {checkpoint_age/3600/24:.1f} days")
            
            # Check batch size in reasonable range
            try:
                batch_size = int(checkpoint_data['batch_size'])
                if not (100 <= batch_size <= 50000):
                    logger.warning(f"Invalid batch size: {batch_size:,}")
                    return False
            except (ValueError, TypeError):
                logger.warning(f"Invalid batch size type")
                return False
            
            # Check checksum if present
            if 'checksum' in checkpoint_data:
                data_copy = checkpoint_data.copy()
                saved_checksum = data_copy.pop('checksum')
                calculated_checksum = self._generate_checksum(data_copy)
                
                if saved_checksum != calculated_checksum:
                    logger.warning("Checksum mismatch")
                    self.stats['integrity_errors'] += 1
                    return False
            
            logger.debug("Checkpoint passed integrity check")
            return True
            
        except (TypeError, KeyError, ValueError) as e:
            logger.warning(f"Checkpoint validation error: {e}")
            self.stats['integrity_errors'] += 1
            return False
    
    def _safe_json_load(self, filepath: str) -> Optional[Dict[str, Any]]:
        """Safe JSON loading with error handling"""
        if not os.path.exists(filepath):
            return None
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in file {filepath}: {e}")
            
            # Try to recover file
            try:
                backup_content = self._try_recover_json(filepath)
                if backup_content:
                    logger.info(f"Successfully recovered JSON from {filepath}")
                    return backup_content
            except Exception as recovery_error:
                logger.error(f"JSON recovery error: {recovery_error}")
            
            return None
        except UnicodeDecodeError as e:
            logger.error(f"Encoding error in file {filepath}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error loading {filepath}: {e}")
            return None
    
    def _try_recover_json(self, filepath: str) -> Optional[Dict[str, Any]]:
        """Attempt to recover corrupted JSON file"""
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Try to find and extract JSON
            start_idx = content.find('{')
            end_idx = content.rfind('}')
            
            if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                json_str = content[start_idx:end_idx + 1]
                return json.loads(json_str)
            
            return None
        except Exception as e:
            logger.debug(f"JSON recovery failed: {e}")
            return None
    
    def load_checkpoint(self) -> Optional[CheckpointState]:
        """Load checkpoint state"""
        cache_key = f"checkpoint_{self.checkpoint_file}"
        current_time = time.time()
        
        # Check cache
        if cache_key in self._cache:
            cached_data, timestamp = self._cache[cache_key]
            if current_time - timestamp < self._cache_ttl:
                logger.debug("Loaded from cache")
                self.state = cached_data
                self._update_stats('load')
                return self.state
        
        # Clear cache
        self._cache.clear()
        
        logger.info(f"Loading checkpoint from {self.checkpoint_file}")
        
        # Try to load from main file
        if os.path.exists(self.checkpoint_file):
            try:
                data = self._safe_json_load(self.checkpoint_file)
                if data is None:
                    logger.warning(f"Failed to load main file, trying backup")
                    return self._load_backup_checkpoint()
                
                # Check format version
                if data.get('version', 0) != self.CHECKPOINT_VERSION:
                    logger.warning(f"Incompatible checkpoint version: {data.get('version')}")
                    return self._load_backup_checkpoint()
                
                # Validate data integrity
                if not self.validate_checkpoint_integrity(data):
                    logger.warning("Checkpoint data integrity check failed")
                    return self._load_backup_checkpoint()
                
                # Create state object
                self.state = CheckpointState.from_dict(data)
                
                # Save checksum
                self.checksum = data.get('checksum')
                
                # Save to cache
                self._cache[cache_key] = (self.state, current_time)
                
                # Update stats
                self._update_stats('load')
                
                # Log successful load
                logger.info(f"Loaded checkpoint: processed {self.state.processed_lines:,} of {self.state.total_lines:,} records")
                logger.info(f"Last position: {self.state.last_position:,} bytes")
                logger.info(f"Batch size: {self.state.batch_size:,}")
                logger.info(f"Progress: {self.state.progress_percent:.1f}%")
                
                # Check expiration
                if self.state.is_expired():
                    logger.warning(f"Checkpoint expired: {self.state.age_hours:.1f} hours")
                
                return self.state
                
            except Exception as e:
                logger.error(f"Error loading main checkpoint: {e}")
                return self._load_backup_checkpoint()
        
        # File doesn't exist
        logger.info("Checkpoint not found")
        return None
    
    def _load_backup_checkpoint(self) -> Optional[CheckpointState]:
        """Load checkpoint from backup"""
        if not os.path.exists(self.checkpoint_backup):
            logger.info("Backup checkpoint not found")
            return None
        
        logger.info(f"Loading backup checkpoint from {self.checkpoint_backup}")
        
        try:
            data = self._safe_json_load(self.checkpoint_backup)
            if data is None:
                logger.error("Failed to load backup")
                return None
            
            # Validate data integrity
            if not self.validate_checkpoint_integrity(data):
                logger.warning("Backup integrity check failed")
                return None
            
            # Create state object
            self.state = CheckpointState.from_dict(data)
            self.checksum = data.get('checksum')
            
            # Update stats
            self.stats['backup_restores'] += 1
            self._update_stats('load')
            
            logger.info(f"Loaded backup checkpoint: processed {self.state.processed_lines:,} records")
            
            # Restore main file from backup
            try:
                self._atomic_save(self.checkpoint_backup, self.checkpoint_file)
                logger.info("Main checkpoint file restored from backup")
            except Exception as e:
                logger.error(f"Failed to restore main checkpoint file: {e}")
            
            return self.state
            
        except Exception as e:
            logger.error(f"Error loading backup checkpoint: {e}")
            return None
    
    def _atomic_save(self, source: str, destination: str):
        """Atomic file save"""
        try:
            # Create temp file
            temp_file = f"{destination}.atomic.tmp"
            
            # Copy file
            shutil.copy2(source, temp_file)
            
            # Verify file was copied correctly
            if os.path.exists(temp_file):
                dest_size = os.path.getsize(temp_file)
                src_size = os.path.getsize(source)
                
                if dest_size == src_size:
                    # Atomic rename
                    os.replace(temp_file, destination)
                    logger.debug(f"File saved successfully: {dest_size} bytes")
                else:
                    os.remove(temp_file)
                    raise IOError(f"File sizes don't match: {src_size} != {dest_size}")
            else:
                raise IOError("Temp file not created")
        
        except Exception as e:
            logger.error(f"Atomic save error {source} -> {destination}: {e}")
            raise
    
    def save_checkpoint(self,
                       file_name: str,
                       total_lines: int,
                       processed_lines: int,
                       valid_images: int,
                       failed_images: int,
                       json_errors: int,
                       cached_images: int,
                       network_errors: int,
                       timeout_errors: int,
                       duplicate_records: int,
                       last_position: int,
                       batch_size: int,
                       records_processed: list,
                       unique_users: list,
                       unique_devices: list,
                       unique_companies: list,
                       unique_ips: list) -> bool:
        """
        Save checkpoint state
        
        Saves checkpoint when one of these conditions is met:
        1. Every 60 seconds
        2. Every CHECKPOINT_INTERVAL records
        3. At processing completion
        
        Returns:
            bool: True if save was successful, False on error
        """
        current_time = time.time()
        
        # Check if we should save
        time_condition = current_time - self.last_save >= 60
        records_condition = False
        
        if self.state:
            records_since_last = processed_lines - self.state.processed_lines
            records_condition = records_since_last >= app_config.checkpoint_interval
        
        completion_condition = processed_lines >= total_lines and total_lines > 0
        
        if not (time_condition or records_condition or completion_condition):
            return False
        
        # Prepare data
        checkpoint_data = {
            'version': self.CHECKPOINT_VERSION,
            'file_name': file_name,
            'total_lines': total_lines,
            'processed_lines': processed_lines,
            'valid_images': valid_images,
            'failed_images': failed_images,
            'json_errors': json_errors,
            'cached_images': cached_images,
            'network_errors': network_errors,
            'timeout_errors': timeout_errors,
            'duplicate_records': duplicate_records,
            'last_position': last_position,
            'timestamp': current_time,
            'batch_size': batch_size,
            'records_processed': records_processed,
            'unique_users': unique_users,
            'unique_devices': unique_devices,
            'unique_companies': unique_companies,
            'unique_ips': unique_ips,
        }
        
        # Add checksum
        checksum = self._generate_checksum(checkpoint_data)
        checkpoint_data['checksum'] = checksum
        
        try:
            # Step 1: Save to temp file
            with open(self.checkpoint_temp, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f, indent=2, ensure_ascii=False, default=str)
            
            # Step 2: Create backup of current checkpoint (if exists)
            if os.path.exists(self.checkpoint_file):
                try:
                    # Create archive of previous checkpoint
                    if os.path.exists(self.checkpoint_backup):
                        try:
                            shutil.copy2(self.checkpoint_backup, self.checkpoint_archive)
                        except Exception:
                            pass
                    
                    # Update backup
                    shutil.copy2(self.checkpoint_file, self.checkpoint_backup)
                    logger.debug("Created checkpoint backup")
                except Exception as e:
                    logger.warning(f"Failed to create backup: {e}")
            
            # Step 3: Atomic move temp file to main
            self._atomic_save(self.checkpoint_temp, self.checkpoint_file)
            
            # Step 4: Clean up temp file
            if os.path.exists(self.checkpoint_temp):
                try:
                    os.remove(self.checkpoint_temp)
                except Exception:
                    pass
            
            # Update state
            self.state = CheckpointState.from_dict(checkpoint_data)
            self.checksum = checksum
            self.last_save = current_time
            self.save_count += 1
            
            # Clear cache
            self._cache.clear()
            
            # Update stats
            self._update_stats('save')
            
            # Log save
            if completion_condition:
                logger.info(f"💾 Final checkpoint saved: {processed_lines:,} of {total_lines:,} records")
            elif records_condition:
                logger.info(f"💾 Checkpoint saved (every {app_config.checkpoint_interval:,}): {processed_lines:,} records")
            elif time_condition:
                logger.debug(f"💾 Auto-save (every 60 sec): {processed_lines:,} records")
            
            return True
            
        except Exception as e:
            logger.error(f"Checkpoint save error: {e}")
            
            # Clean up temp file on error
            if os.path.exists(self.checkpoint_temp):
                try:
                    os.remove(self.checkpoint_temp)
                except Exception:
                    pass
            
            return False
    
    def clear_checkpoint(self) -> int:
        """Clear all checkpoint files"""
        files_to_remove = [
            self.checkpoint_file,
            self.checkpoint_backup,
            self.checkpoint_temp,
            self.checkpoint_archive
        ]
        
        removed_count = 0
        for file_path in files_to_remove:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    removed_count += 1
                    logger.debug(f"Removed checkpoint file: {file_path}")
                except Exception as e:
                    logger.error(f"Failed to remove file {file_path}: {e}")
        
        # Reset state
        self.state = None
        self.checksum = None
        self.last_save = 0.0
        self._cache.clear()
        
        if removed_count > 0:
            logger.info(f"Cleaned up {removed_count} checkpoint files")
        
        return removed_count
    
    def should_save_checkpoint(self, processed_since_last: int) -> bool:
        """Check if checkpoint should be saved"""
        return processed_since_last >= app_config.checkpoint_interval
    
    def get_checkpoint_info(self) -> Dict[str, Any]:
        """Get detailed checkpoint information"""
        if not self.state:
            return {
                "exists": False,
                "file_path": self.checkpoint_file,
                "backup_exists": os.path.exists(self.checkpoint_backup)
            }
        
        info = self.state.to_dict()
        info["exists"] = True
        info["file_path"] = self.checkpoint_file
        info["backup_exists"] = os.path.exists(self.checkpoint_backup)
        info["save_count"] = self.save_count
        info["checksum"] = self.checksum
        
        # Add file info
        if os.path.exists(self.checkpoint_file):
            try:
                info["file_size"] = os.path.getsize(self.checkpoint_file)
                info["file_mtime"] = os.path.getmtime(self.checkpoint_file)
                info["file_ctime"] = os.path.getctime(self.checkpoint_file)
            except Exception:
                pass
        
        return info
    
    def get_progress_info(self) -> Dict[str, Any]:
        """Get processing progress information"""
        if not self.state:
            return {"has_checkpoint": False}
        
        info = self.get_checkpoint_info()
        info["has_checkpoint"] = True
        
        # Add speed info
        if self.state.timestamp > 0 and self.state.processed_lines > 0:
            elapsed_hours = self.state.age_hours
            
            if elapsed_hours > 0:
                records_per_hour = self.state.processed_lines / elapsed_hours
                info["records_per_hour"] = int(records_per_hour)
                info["elapsed_hours"] = round(elapsed_hours, 1)
                
                # Estimated time to completion
                if self.state.total_lines > 0:
                    remaining = self.state.total_lines - self.state.processed_lines
                    if records_per_hour > 0:
                        hours_remaining = remaining / records_per_hour
                        info["hours_remaining"] = round(hours_remaining, 1)
                        info["eta_timestamp"] = time.time() + hours_remaining * 3600
        
        return info
    
    def validate_checkpoint(self, input_file: str) -> tuple[bool, str]:
        """
        Validate checkpoint for current file
        
        Returns:
            tuple[bool, str]: (Is checkpoint valid, Error message)
        """
        if not self.state:
            return False, "Checkpoint not loaded"
        
        # Check if checkpoint is for the same file
        if self.state.file_name != os.path.basename(input_file):
            message = f"Checkpoint for different file: {self.state.file_name} != {os.path.basename(input_file)}"
            logger.warning(message)
            return False, message
        
        # Check if file exists
        if not os.path.exists(input_file):
            message = "Input file does not exist"
            logger.warning(message)
            return False, message
        
        # Check file position
        try:
            file_size = os.path.getsize(input_file)
            
            # Allow small position error (1KB)
            if self.state.last_position > file_size + 1024:
                message = f"Invalid checkpoint position: {self.state.last_position:,} > {file_size:,}"
                logger.warning(message)
                return False, message
            
            # If position is near end of file, consider processing complete
            if file_size - self.state.last_position < 1024:  # Less than 1KB remaining
                logger.info(f"File almost completely processed, position: {self.state.last_position:,} of {file_size:,}")
        
        except OSError as e:
            message = f"File size check error: {e}"
            logger.warning(message)
            return False, message
        
        # Check line counts
        if self.state.total_lines < self.state.processed_lines:
            message = f"Invalid line count: {self.state.processed_lines:,} > {self.state.total_lines:,}"
            logger.warning(message)
            return False, message
        
        # Check expiration
        if self.state.is_expired(max_age_hours=168):  # 7 days
            message = f"Checkpoint expired: {self.state.age_hours:.1f} hours"
            logger.warning(message)
            return False, message
        
        logger.info(f"Checkpoint valid for file {input_file}")
        return True, "Checkpoint is valid"
    
    def archive_old_checkpoint(self, max_age_days: int = 30) -> bool:
        """Archive old checkpoint"""
        if not self.state:
            return False
        
        if self.state.age_hours <= max_age_days * 24:
            return False
        
        try:
            archive_dir = os.path.join(self.output_dir, "checkpoint_archive")
            os.makedirs(archive_dir, exist_ok=True)
            
            timestamp = datetime.fromtimestamp(self.state.timestamp).strftime("%Y%m%d_%H%M%S")
            archive_name = f"checkpoint_{self.state.file_name}_{timestamp}.json"
            archive_path = os.path.join(archive_dir, archive_name)
            
            # Copy checkpoint to archive
            if os.path.exists(self.checkpoint_file):
                shutil.copy2(self.checkpoint_file, archive_path)
                logger.info(f"Checkpoint archived: {archive_path}")
                return True
        
        except Exception as e:
            logger.error(f"Checkpoint archiving error: {e}")
        
        return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get checkpoint manager statistics"""
        stats = self.stats.copy()
        
        # Add file info
        stats['checkpoint_exists'] = os.path.exists(self.checkpoint_file)
        stats['backup_exists'] = os.path.exists(self.checkpoint_backup)
        stats['archive_exists'] = os.path.exists(self.checkpoint_archive)
        
        if os.path.exists(self.checkpoint_file):
            try:
                stats['checkpoint_size'] = os.path.getsize(self.checkpoint_file)
                stats['checkpoint_mtime'] = os.path.getmtime(self.checkpoint_file)
            except Exception:
                pass
        
        # Add current state info
        if self.state:
            stats['current_state'] = {
                'processed_lines': self.state.processed_lines,
                'total_lines': self.state.total_lines,
                'progress_percent': self.state.progress_percent,
                'age_hours': self.state.age_hours,
                'batch_size': self.state.batch_size
            }
        
        stats['save_count'] = self.save_count
        stats['cache_size'] = len(self._cache)
        
        return stats
    
    def cleanup_temp_files(self):
        """Clean up temporary files"""
        temp_files = [self.checkpoint_temp]
        
        for file_path in temp_files:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    logger.debug(f"Cleaned temp file: {file_path}")
                except Exception as e:
                    logger.debug(f"Failed to clean temp file {file_path}: {e}")


# Utility class for checkpoint operations
class CheckpointUtils:
    """Utilities for checkpoint operations"""
    
    @staticmethod
    def scan_for_checkpoints(directory: str) -> List[Dict[str, Any]]:
        """Scan directory for checkpoints"""
        checkpoints = []
        
        try:
            for root, dirs, files in os.walk(directory):
                for file in files:
                    if file == app_config.checkpoint_file:
                        filepath = os.path.join(root, file)
                        try:
                            data = CheckpointUtils._safe_read_json(filepath)
                            if data:
                                checkpoints.append({
                                    'path': filepath,
                                    'directory': root,
                                    'file_name': data.get('file_name', ''),
                                    'processed_lines': data.get('processed_lines', 0),
                                    'total_lines': data.get('total_lines', 0),
                                    'timestamp': data.get('timestamp', 0),
                                    'progress_percent': (data.get('processed_lines', 0) / data.get('total_lines', 1) * 100) if data.get('total_lines', 0) > 0 else 0
                                })
                        except Exception as e:
                            logger.debug(f"Error reading checkpoint {filepath}: {e}")
        
        except Exception as e:
            logger.error(f"Checkpoint scan error: {e}")
        
        return checkpoints
    
    @staticmethod
    def _safe_read_json(filepath: str) -> Optional[Dict[str, Any]]:
        """Safe JSON file reading"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return None


def create_checkpoint_manager(output_dir: str) -> CheckpointManager:
    """Create checkpoint manager with pre-check"""
    # Create directory if needed
    os.makedirs(output_dir, exist_ok=True)
    
    # Create manager
    manager = CheckpointManager(output_dir)
    
    # Scan for existing checkpoints
    checkpoints = CheckpointUtils.scan_for_checkpoints(output_dir)
    if len(checkpoints) > 1:
        logger.info(f"Found {len(checkpoints)} checkpoints in directory")
    
    return manager