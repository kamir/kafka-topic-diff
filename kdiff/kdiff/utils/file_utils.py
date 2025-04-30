"""
File utility functions for KDIFF.

This module contains utility functions for file operations used across
the KDIFF tool.
"""

import os
import pathlib
from typing import Union, Optional


def expand_path(path: str) -> str:
    """
    Expand user home directory and environment variables in a path.
    
    Args:
        path: The path to expand
        
    Returns:
        The expanded path as a string
    """
    expanded = os.path.expanduser(os.path.expandvars(path))
    return expanded


def ensure_dir_exists(path: Union[str, pathlib.Path]) -> pathlib.Path:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        path: The directory path to check/create
        
    Returns:
        A Path object for the directory
    """
    if isinstance(path, str):
        path = pathlib.Path(expand_path(path))
    
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
    elif not path.is_dir():
        raise NotADirectoryError(f"Path exists but is not a directory: {path}")
        
    return path


def find_file(filename: str, search_dirs: list[str], default_path: Optional[str] = None) -> Optional[str]:
    """
    Search for a file in the given directories.
    
    Args:
        filename: The filename to search for
        search_dirs: A list of directories to search in
        default_path: A default path to return if the file is not found
        
    Returns:
        The full path to the found file, the default_path if provided and 
        file not found, or None if the file is not found and no default is provided
    """
    for directory in search_dirs:
        filepath = os.path.join(expand_path(directory), filename)
        if os.path.isfile(filepath):
            return filepath
            
    return default_path
