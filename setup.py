#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Setup script for Face Recognition Analytics Suite
"""

from setuptools import setup, find_packages
import os

# Read the contents of README file
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Read the requirements from requirements.txt
with open(os.path.join(this_directory, 'requirements.txt'), encoding='utf-8') as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name="face-recognition-analytics-suite",
    version="13.0.0",
    description="Professional face recognition data processing system with advanced optimization for large files",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Face Recognition Analytics Team",
    author_email="",
    url="https://github.com/your-username/face-recognition-analytics-suite",
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'face-recognition-suite=src.main:main',
        ],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Image Recognition",
        "Topic :: Utilities",
    ],
    python_requires=">=3.7",
    keywords="face-recognition, image-processing, data-analytics, computer-vision",
    project_urls={
        'Source': 'https://github.com/your-username/face-recognition-analytics-suite',
        'Tracker': 'https://github.com/your-username/face-recognition-analytics-suite/issues',
    },
)