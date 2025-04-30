"""
Setup script for the KDIFF package.
"""

import os
from setuptools import setup, find_packages

# Read version from package __init__.py
with open(os.path.join('kdiff', '__init__.py'), 'r') as f:
    for line in f:
        if line.startswith('__version__'):
            version = line.split('=')[1].strip().strip('"\'')
            break

# Read requirements from requirements.txt
with open('requirements.txt', 'r') as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]

# Read long description from README if it exists
long_description = ""
if os.path.exists('README.md'):
    with open('README.md', 'r') as f:
        long_description = f.read()

setup(
    name="kdiff",
    version=version,
    description="A tool for comparing the content of two Kafka topics",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Kafka Tools Team",
    author_email="team@example.com",
    url="https://github.com/example/kdiff",
    packages=find_packages(),
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'kdiff=kdiff.cli.main:main',
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Software Development :: Libraries",
        "Topic :: System :: Systems Administration",
    ],
    python_requires=">=3.8",
    include_package_data=True,
    zip_safe=False,
)
