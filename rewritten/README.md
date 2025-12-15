# Face Recognition Analytics Suite

Professional data processing and analytics tool for face recognition systems. This application processes JSON/JSONL files containing face recognition data, downloads associated images, and generates comprehensive reports in multiple formats.

## Features

- **High Performance**: Optimized for processing large JSON/JSONL files (up to 1+ GB)
- **Multi-format Reports**: HTML, PDF, Excel, and JSON reports
- **Image Processing**: Download and process images with caching
- **Checkpoint Support**: Resume interrupted processing
- **Memory Efficient**: Smart caching and memory management
- **Cross-platform**: Works on Windows, Linux, and macOS
- **Progress Tracking**: Real-time progress with ETA

## Requirements

- Python 3.7 or higher
- At least 4GB RAM (8GB+ recommended for large files)
- Sufficient disk space for output files

## Installation

```bash
pip install -r requirements.txt
```

Or install directly:

```bash
pip install face-recognition-analytics
```

## Usage

### Interactive Mode
```bash
python -m src.main
```

### Command Line Options
```bash
# Process a specific file with HTML output
python -m src.main --file data.json --format HTML

# Process with multiple output formats
python -m src.main --file data.json --format HTML,Excel,JSON --output my_results

# Resume interrupted processing
python -m src.main --resume

# Non-interactive mode
python -m src.main --file data.json --format HTML --no-interaction --no-check
```

## Input File Format

The application accepts JSON/JSONL files where each line is a valid JSON object. Supported fields include:

```json
{
  "timestamp": {"$date": "2024-01-01T10:00:00Z"},
  "device_id": "CAM001",
  "user_name": "Иван Иванов",
  "eva_sex": "male",
  "sex": 1,
  "comp_score": 95.5,
  "eva_age": 30,
  "image": "http://example.com/photo.jpg",
  "face_id": "face_123",
  "company_id": "company_456",
  "event_type": "recognition",
  "user_list": "list_789",
  "IP": "192.168.1.100"
}
```

## Output Structure

```
output_results/
├── results_YYYYMMDD_HHMMSS/
│   ├── photos/           # Downloaded images
│   ├── reports/          # Generated reports
│   │   ├── face_recognition_report.html
│   │   ├── face_recognition_report.pdf
│   │   ├── face_recognition_data.xlsx
│   │   └── statistics.json
│   ├── image_cache/      # Cached images
│   └── temp/             # Temporary files
```

## Configuration

The application automatically adapts to system resources. You can also configure behavior through environment variables:

- `FACE_RECOGNITION_MAX_WORKERS`: Maximum concurrent workers
- `FACE_RECOGNITION_REQUEST_TIMEOUT`: Request timeout in seconds
- `FACE_RECOGNITION_CHECKPOINT_INTERVAL`: Records between checkpoints
- `FACE_RECOGNITION_MAX_IMAGE_SIZE_MB`: Maximum image size in MB

## Performance Tips

1. **Large Files**: For files >1GB, use HTML format only (more efficient)
2. **Memory**: Close other applications when processing large files
3. **Network**: Ensure stable internet connection for image downloads
4. **Storage**: Ensure sufficient disk space (2-3x input file size recommended)

## Troubleshooting

- **Memory Issues**: Reduce `max_workers` in configuration
- **Slow Processing**: Check network connection for image downloads
- **JSON Errors**: Verify input file format and encoding
- **Missing Dependencies**: Run `pip install -r requirements.txt`

## License

MIT License - see LICENSE file for details.