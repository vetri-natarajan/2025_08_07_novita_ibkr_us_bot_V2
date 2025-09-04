# -*- coding: utf-8 -*-
"""
utils/make_path
"""
from pathlib import Path 
import os

def make_path(*path_parts):
    """
    Create an OS-independent path from given path parts.

    Args:
      *path_parts: str components of the path.

    Returns:
      pathlib.Path or str: Joined path using the selected method.
    """
    return Path(*path_parts)