"""
Setup script for Face Recognition Analytics Suite
"""
from setuptools import setup, find_packages
import os

# Read the contents of README file
def read_readme():
    readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
    if os.path.exists(readme_path):
        with open(readme_path, 'r', encoding='utf-8') as f:
            return f.read()
    return "Face Recognition Analytics Suite - Professional data processing tool"

# Read requirements from requirements.txt
def read_requirements():
    requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
    if os.path.exists(requirements_path):
        with open(requirements_path, 'r', encoding='utf-8') as f:
            requirements = []
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    requirements.append(line)
            return requirements
    return []

setup(
    name="face-recognition-analytics",
    version="13.0.0",
    author="Professional Analytics Team",
    author_email="analytics@example.com",
    description="Professional face recognition data processing and analytics suite",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/example/face-recognition-analytics",
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
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
        "Topic :: Scientific/Engineering :: Image Recognition",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.7",
    install_requires=read_requirements(),
    entry_points={
        'console_scripts': [
            'face-recognition-analyze=src.main:main',
        ],
    },
    keywords=[
        "face recognition", "data processing", "json parsing", 
        "image processing", "analytics", "reporting", "checkpoint"
    ],
    project_urls={
        'Source': 'https://github.com/example/face-recognition-analytics',
        'Tracker': 'https://github.com/example/face-recognition-analytics/issues',
    },
)