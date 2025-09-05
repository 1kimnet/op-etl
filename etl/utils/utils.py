"""Small ETL utilities used by staging and other modules.

Contains essential ArcPy-safe helper functions.
"""
import logging
import re
import sys
import unicodedata
from pathlib import Path
from typing import List, Optional


def make_arcpy_safe_name(name: str, max_length: int = 100) -> str:
    """Create ArcPy-safe feature class names that always work.

    Handles Swedish characters, Unicode normalization, and ArcPy naming restrictions.

    Args:
        name: Input name that may contain special characters
        max_length: Maximum length for the output name

    Returns:
        Clean name safe for use as ArcPy feature class name
    """
    if not name:
        return "unnamed_fc"

    # Normalize unicode and remove all accents
    normalized = unicodedata.normalize('NFD', name)
    ascii_name = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')

    # Convert to ASCII, handling any remaining issues
    try:
        ascii_name = ascii_name.encode('ascii', 'ignore').decode('ascii')
    except Exception:
        ascii_name = "converted_name"

    # Clean for ArcPy rules
    clean = ascii_name.lower().strip()

    # Replace any non-alphanumeric with underscore
    clean = re.sub(r'[^a-z0-9]', '_', clean)

    # Remove multiple underscores
    clean = re.sub(r'_+', '_', clean)

    # Remove leading/trailing underscores
    clean = clean.strip('_')

    # Ensure it starts with a letter (ArcPy requirement)
    if clean and clean[0].isdigit():
        clean = f"fc_{clean}"

    # Handle empty/invalid results
    if not clean or len(clean) < 1:
        clean = "default_fc"

    # Truncate to max length
    clean = clean[:max_length]

    # Handle reserved words (Windows/ArcPy conflicts)
    reserved = {
        'con', 'prn', 'aux', 'nul', 'com1', 'com2', 'com3', 'com4',
        'com5', 'com6', 'com7', 'com8', 'com9', 'lpt1', 'lpt2',
        'lpt3', 'lpt4', 'lpt5', 'lpt6', 'lpt7', 'lpt8', 'lpt9'
    }
    if clean.lower() in reserved:
        clean = f"{clean}_data"

    return clean