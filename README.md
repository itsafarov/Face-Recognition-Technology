# Face Recognition Analytics Suite

Professional face recognition data processing system with advanced optimization for large files.

## Features

- **High Performance**: Optimized for processing large datasets with adaptive batch processing
- **Multiple Output Formats**: HTML, PDF, Excel, and JSON reports
- **Memory Management**: Intelligent memory usage control with checkpointing
- **Cross-Platform**: Works on Windows, Linux, and macOS
- **Resumable Processing**: Can resume from checkpoints if interrupted

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Quick Start
```bash
python run.py
```

### Command Line Options
- `--resume`: Resume interrupted processing
- `--file <path>`: Process specific file
- `--formats <format>`: Output formats (html,pdf,excel,json)
- `--batch-size <number>`: Batch size for processing
- `--max-workers <number>`: Max parallel workers
- `--memory-limit <percent>`: Max memory usage percentage
- `--skip-optimization`: Skip system optimization
- `--help`: Show all options

## Project Structure

```
/workspace/
├── run.py                 # Main launcher script
├── requirements.txt       # Dependencies
├── README.md             # This file
└── src/                  # Source code
    ├── main.py           # Main application
    ├── core/             # Core modules
    ├── processing/       # Processing modules
    └── utils/            # Utility functions
```

## Input Format

Supports JSON, JSONL, and TXT files with records in the following format:
```json
{"timestamp": {"$date": "2024-01-01T10:00:00Z"}, "device_id": "CAM001", "user_name": "John Doe", "image": "http://example.com/photo.jpg"}
```

## Output

Processed results are saved in `output_results/` directory in the following structure:
```
output_results/
└── results_YYYYMMDD_HHMMSS/
    ├── photos/          # Processed images
    ├── reports/         # Generated reports
    └── processing_checkpoint.json  # Checkpoint file
```

## System Requirements

- Python 3.7 or higher
- At least 2GB free RAM (more for larger files)
- Sufficient disk space for output files
