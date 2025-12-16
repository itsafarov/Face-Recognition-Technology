#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main launcher script for Face Recognition Analytics Suite
This script provides an entry point to run the application without directly calling main.py
"""

import os
import sys
import subprocess
from pathlib import Path

def main():
    """Main entry point that launches the application"""
    # Get the directory where this script is located
    script_dir = Path(__file__).parent.absolute()
    src_dir = script_dir / "src"
    
    # Check if src directory exists
    if not src_dir.exists():
        print("❌ Error: 'src' directory not found!")
        print(f"Current directory: {script_dir}")
        sys.exit(1)
    
    # Path to main.py
    main_py = src_dir / "main.py"
    if not main_py.exists():
        print("❌ Error: 'src/main.py' not found!")
        sys.exit(1)
    
    # Prepare the command to run main.py with all arguments
    cmd = [sys.executable, str(main_py)] + sys.argv[1:]
    
    try:
        # Run the main application
        result = subprocess.run(cmd, check=True)
        sys.exit(result.returncode)
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running the application: {e}")
        sys.exit(e.returncode)
    except FileNotFoundError:
        print("❌ Python interpreter not found!")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n⚠️  Application interrupted by user")
        sys.exit(1)

if __name__ == "__main__":
    main()