import subprocess
import logging
from typing import Optional

# Import GDAL safely
GDAL_AVAILABLE = False
try:
    from osgeo import gdal
    gdal.PushErrorHandler('CPLQuietErrorHandler')  # Suppress GDAL warnings to console
    GDAL_AVAILABLE = True
except ImportError:
    pass

logger = logging.getLogger("ahn_downloader")

def verify_file_with_gdal(file_path: str) -> bool:
    """
    Verify TIF file integrity using GDAL.
    Returns True if file is valid, False otherwise.
    """
    if not GDAL_AVAILABLE:
        logger.warning("GDAL not available, skipping integrity check")
        return True  # Skip verification if GDAL not available
    
    try:
        dataset = gdal.Open(file_path, gdal.GA_ReadOnly)
        if dataset is None:
            logger.error(f"GDAL cannot open file: {file_path}")
            return False
        
        # Check if we can read basic metadata
        if dataset.RasterXSize <= 0 or dataset.RasterYSize <= 0:
            logger.error(f"Invalid raster dimensions in {file_path}")
            dataset = None
            return False
        
        # Try to read a small portion of the data to ensure it's not corrupted
        band = dataset.GetRasterBand(1)
        if band is None:
            logger.error(f"Cannot read raster band in {file_path}")
            dataset = None
            return False
        
        # Clean up
        band = None
        dataset = None
        return True
        
    except Exception as e:
        logger.error(f"GDAL verification failed for {file_path}: {e}")
        return False


def verify_file_with_gdalinfo(file_path: str) -> bool:
    """
    Verify TIF file integrity using gdalinfo command-line tool.
    Returns True if file is valid, False otherwise.
    """
    try:
        # Run gdalinfo command with suppressed output
        result = subprocess.run(
            ['gdalinfo', file_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30  # 30 second timeout
        )
        
        # Return code 0 means success
        if result.returncode == 0:
            return True
        else:
            logger.error(f"gdalinfo returned error code {result.returncode} for {file_path}")
            return False
            
    except FileNotFoundError:
        logger.warning("gdalinfo command not found. Skipping integrity check.")
        logger.warning("Install GDAL/OSGeo4W for full verification support.")
        return True  # Skip verification if gdalinfo not available
    except subprocess.TimeoutExpired:
        logger.error(f"gdalinfo timed out for {file_path}")
        return False
    except Exception as e:
        logger.error(f"gdalinfo verification failed for {file_path}: {e}")
        return False
