import os
import platform
import logging

logger = logging.getLogger("ahn_downloader")

def setup_osgeo4w():
    """
    Auto-detect and configure OSGEO4W on Windows.
    Returns True if successful, False otherwise.
    """
    if platform.system() != 'Windows':
        return True  # Not needed on non-Windows systems
    
    # Common OSGEO4W installation paths
    possible_paths = [
        r'Z:\GEO\OSGeo4W',  # User's custom installation
        r'C:\OSGeo4W64',
        r'C:\OSGeo4W',
        r'C:\Program Files\QGIS 3.28',
        r'C:\Program Files\QGIS 3.30',
        r'C:\Program Files\QGIS 3.32',
        r'C:\Program Files\QGIS 3.34',
    ]
    
    osgeo_root = None
    for path in possible_paths:
        if os.path.exists(path):
            osgeo_root = path
            logger.info(f"Found OSGEO4W installation at: {path}")
            break
    
    if not osgeo_root:
        logger.warning("OSGEO4W not found in common locations. Attempting to use system GDAL...")
        return False
    
    # Set up environment variables
    bin_path = os.path.join(osgeo_root, 'bin')
    apps_path = os.path.join(osgeo_root, 'apps')
    
    # Add to PATH
    if bin_path not in os.environ['PATH']:
        os.environ['PATH'] = bin_path + os.pathsep + os.environ['PATH']
    
    # Set GDAL_DATA
    gdal_data_paths = [
        os.path.join(apps_path, 'gdal', 'share', 'gdal'),
        os.path.join(osgeo_root, 'share', 'gdal'),
    ]
    for gdal_data in gdal_data_paths:
        if os.path.exists(gdal_data):
            os.environ['GDAL_DATA'] = gdal_data
            logger.info(f"Set GDAL_DATA to: {gdal_data}")
            break
    
    # Set PROJ_LIB
    proj_lib_paths = [
        os.path.join(apps_path, 'proj', 'share', 'proj'),
        os.path.join(osgeo_root, 'share', 'proj'),
    ]
    for proj_lib in proj_lib_paths:
        if os.path.exists(proj_lib):
            os.environ['PROJ_LIB'] = proj_lib
            logger.info(f"Set PROJ_LIB to: {proj_lib}")
            break
    
    return True

def sanitize_filename(filename: str) -> str:
    """Sanitize filename to prevent directory traversal."""
    # Get just the basename to prevent path traversal
    return os.path.basename(filename)
