import os
import sys
import json
import logging
import subprocess
import re
import concurrent.futures
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from tqdm import tqdm

from ..config import DOWNLOAD_DIR, WGS84_OUTPUT_DIR, ERROR_LOG, CONVERSION_WORKERS

logger = logging.getLogger("ahn_downloader")

TARGET_CRS = "EPSG:4326"
RESAMPLING_METHOD = "bilinear"

def check_gdal_availability():
    """
    Check if GDAL tools (gdalwarp, gdalinfo) are accessible.
    Returns True if available, False otherwise.
    """
    try:
        result = subprocess.run(
            ['gdalinfo', '--version'],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            logger.info(f"GDAL found: {result.stdout.strip()}")
            return True
        else:
            logger.error("gdalinfo command failed")
            return False
    except FileNotFoundError:
        logger.error("gdalinfo not found in PATH. Please ensure GDAL is installed and PATH is set.")
        return False
    except Exception as e:
        logger.error(f"Error checking GDAL availability: {e}")
        return False


def get_tif_files(directory: str) -> List[str]:
    """
    Get all .tif files from the specified directory.
    Returns list of filenames (not full paths).
    """
    path = Path(directory)
    if not path.exists():
        logger.error(f"Directory does not exist: {directory}")
        return []
    
    tif_files = [f.name for f in path.glob("*.tif")]
    return sorted(tif_files)


def get_raster_info(file_path: str) -> Optional[Dict]:
    """
    Extract raster information using gdalinfo.
    Returns dict with size_x, size_y, bands, datatype, crs info.
    """
    try:
        result = subprocess.run(
            ['gdalinfo', '-json', file_path],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            logger.error(f"gdalinfo failed for {file_path}: {result.stderr}")
            return None
        
        info = json.loads(result.stdout)
        
        # Extract key information
        size = info.get('size', [0, 0])
        bands = len(info.get('bands', []))
        
        # Get data type from first band
        datatype = None
        if bands > 0:
            datatype = info['bands'][0].get('type')
        
        # Extract CRS information
        crs_wkt = info.get('coordinateSystem', {}).get('wkt', '')
        
        return {
            'size_x': size[0],
            'size_y': size[1],
            'bands': bands,
            'datatype': datatype,
            'crs_wkt': crs_wkt
        }
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse gdalinfo JSON for {file_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error getting raster info for {file_path}: {e}")
        return None


def detect_source_crs(file_path: str) -> Optional[str]:
    """
    Auto-detect source CRS from TIF file.
    Returns CRS string (e.g., 'EPSG:28992') or None.
    """
    info = get_raster_info(file_path)
    if not info:
        return None
    
    crs_wkt = info.get('crs_wkt', '')
    
    # Try to find EPSG code in the WKT
    epsg_match = re.search(r'EPSG["\',\s]*(\d+)', crs_wkt, re.IGNORECASE)
    if epsg_match:
        epsg_code = epsg_match.group(1)
        return f"EPSG:{epsg_code}"
    
    # Check for RD New / Amersfoort
    if 'Amersfoort' in crs_wkt or '28992' in crs_wkt:
        return "EPSG:28992"
    
    logger.warning(f"Could not determine EPSG code for {file_path}")
    return None


def convert_to_wgs84(input_path: str, output_path: str) -> bool:
    """
    Convert a TIF file to WGS84 using gdalwarp.
    Returns True if successful, False otherwise.
    """
    try:
        cmd = [
            'gdalwarp',
            '-t_srs', TARGET_CRS,
            '-r', RESAMPLING_METHOD,
            '-overwrite',
            input_path,
            output_path
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600  # 10 minutes timeout for large files
        )
        
        if result.returncode != 0:
            logger.error(f"gdalwarp failed: {result.stderr}")
            return False
        
        return True
        
    except subprocess.TimeoutExpired:
        logger.error(f"Conversion timeout for {input_path}")
        return False
    except Exception as e:
        logger.error(f"Error during conversion: {e}")
        return False


def verify_conversion(source_path: str, output_path: str) -> Tuple[bool, str]:
    """
    Verify the conversion was successful by checking integrity.
    Compares dimensions, bands, datatype, and CRS.
    Returns (success: bool, message: str)
    """
    source_info = get_raster_info(source_path)
    output_info = get_raster_info(output_path)
    
    if not source_info:
        return False, "Could not read source file info"
    
    if not output_info:
        return False, "Could not read output file info"
    
    # Check dimensions are reasonable
    if output_info['size_x'] == 0 or output_info['size_y'] == 0:
        return False, "Output has zero dimensions"
    
    # Check band count matches
    if source_info['bands'] != output_info['bands']:
        return False, f"Band count mismatch: {source_info['bands']} vs {output_info['bands']}"
    
    # Check data type matches or is compatible
    if source_info['datatype'] != output_info['datatype']:
        logger.warning(f"Data type changed: {source_info['datatype']} -> {output_info['datatype']}")
    
    # Verify output is in WGS84
    crs_wkt = output_info['crs_wkt']
    if 'WGS 84' not in crs_wkt and '4326' not in crs_wkt:
        return False, "Output CRS is not WGS84"
    
    # Check dimensions haven't changed drastically (allowing some variation due to reprojection)
    size_ratio_x = output_info['size_x'] / source_info['size_x']
    size_ratio_y = output_info['size_y'] / source_info['size_y']
    
    if size_ratio_x < 0.5 or size_ratio_x > 2.0 or size_ratio_y < 0.5 or size_ratio_y > 2.0:
        return False, f"Dimensions changed drastically: {source_info['size_x']}x{source_info['size_y']} -> {output_info['size_x']}x{output_info['size_y']}"
    
    return True, "Integrity check passed"


def prompt_user_for_existing_file(filename: str) -> str:
    """
    Prompt user what to do with existing file.
    Returns 'skip', 'overwrite', or 'cancel'.
    """
    print(f"\nFile {filename} already exists in {WGS84_OUTPUT_DIR}/")
    while True:
        response = input("[S]kip / [O]verwrite / [C]ancel all? ").strip().lower()
        if response in ['s', 'skip']:
            return 'skip'
        elif response in ['o', 'overwrite']:
            return 'overwrite'
        elif response in ['c', 'cancel']:
            return 'cancel'
        else:
            print("Invalid response. Please enter S, O, or C.")


def conversion_worker(filename: str) -> Tuple[str, bool, str]:
    """
    Worker function for parallel conversion.
    Returns (filename, success, message)
    """
    input_path = os.path.join(DOWNLOAD_DIR, filename)
    output_path = os.path.join(WGS84_OUTPUT_DIR, filename)
    
    # Detect source CRS
    source_crs = detect_source_crs(input_path)
    if source_crs:
        logger.info(f"Processing {filename} (Source CRS: {source_crs})")
    else:
        logger.info(f"Processing {filename} (Source CRS: auto-detect)")
    
    # Convert
    if not convert_to_wgs84(input_path, output_path):
        logger.error(f"Failed to convert: {filename}")
        return filename, False, "Conversion failed"
    
    # Verify
    success, message = verify_conversion(input_path, output_path)
    if success:
        logger.info(f"Successfully converted: {filename}")
        return filename, True, "Success"
    else:
        logger.error(f"Integrity check failed for {filename}: {message}")
        return filename, False, message


def check_existing_files_integrity(existing_files: List[str]) -> List[str]:
    """
    Check integrity of existing converted files.
    Returns list of invalid files that need re-conversion.
    """
    if not existing_files:
        return []
        
    print(f"\nChecking integrity of {len(existing_files)} existing converted files...")
    invalid_files = []
    
    for filename in tqdm(existing_files, desc="Verifying existing"):
        input_path = os.path.join(DOWNLOAD_DIR, filename)
        output_path = os.path.join(WGS84_OUTPUT_DIR, filename)
        
        # If source file doesn't exist, we can't verify conversion relative to it
        # But we can check if output file is valid GDAL file
        if not os.path.exists(input_path):
            info = get_raster_info(output_path)
            if not info or info['size_x'] == 0:
                logger.warning(f"Existing file {filename} is corrupt (source missing)")
                invalid_files.append(filename)
            continue
            
        # Full verification against source
        success, message = verify_conversion(input_path, output_path)
        if not success:
            logger.warning(f"Existing file {filename} is invalid: {message}")
            invalid_files.append(filename)
            
    if invalid_files:
        print(f"Found {len(invalid_files)} invalid/corrupt files that need re-conversion.")
    else:
        print("All existing files verified successfully.")
        
    return invalid_files


def run_conversion():
    """
    Main execution function for conversion.
    """
    print("\n" + "="*60)
    print("GDAL TIF to WGS84 Converter")
    print("="*60)

    # Check GDAL availability
    if not check_gdal_availability():
        logger.error("GDAL tools are not available. Exiting.")
        return
    
    # Get source files
    source_files = get_tif_files(DOWNLOAD_DIR)
    total_files = len(source_files)
    
    # Get already converted files
    Path(WGS84_OUTPUT_DIR).mkdir(exist_ok=True)
    existing_files = get_tif_files(WGS84_OUTPUT_DIR)
    
    # Check integrity of existing files
    invalid_files = check_existing_files_integrity(existing_files)
    
    # Calculate files to process (new + invalid)
    valid_existing = set(existing_files) - set(invalid_files)
    to_process = [f for f in source_files if f not in valid_existing]
    
    already_done = len(valid_existing)
    remaining = len(to_process)
    
    print(f"\nTotal TIF files in {DOWNLOAD_DIR}/: {total_files}")
    print(f"Valid reprojected files in {WGS84_OUTPUT_DIR}/: {already_done}")
    print(f"Files to process (new + invalid): {remaining}")
    
    if remaining > 0:
        print(f"\nFiles that need conversion:")
        for i, f in enumerate(to_process[:10], 1):
            print(f"  {i}. {f}")
        if len(to_process) > 10:
            print(f"  ... and {len(to_process) - 10} more")
    
    print("="*60 + "\n")
    
    if not to_process:
        print("All files have already been converted and verified!")
        input("Press Enter to return to menu...")
        return
    
    confirm = input("Start conversion? [Y/n] ").strip().lower()
    if confirm == 'n':
        return

    # Statistics
    stats = {
        'successful': 0,
        'failed': 0,
        'skipped': 0
    }
    failed_files = []
    
    print(f"\nStarting parallel conversion with {CONVERSION_WORKERS} workers...")
    
    # Use ThreadPoolExecutor for parallel processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONVERSION_WORKERS) as executor:
        # Submit all tasks
        future_to_file = {executor.submit(conversion_worker, filename): filename for filename in to_process}
        
        try:
            # Process results as they complete
            for future in tqdm(concurrent.futures.as_completed(future_to_file), total=len(to_process), desc="Converting"):
                filename = future_to_file[future]
                try:
                    _, success, message = future.result()
                    if success:
                        stats['successful'] += 1
                    else:
                        stats['failed'] += 1
                        failed_files.append((filename, message))
                except Exception as e:
                    stats['failed'] += 1
                    failed_files.append((filename, f"Exception: {str(e)}"))
                    logger.error(f"Exception converting {filename}: {e}")
                    
        except KeyboardInterrupt:
            print("\n\nCancelling remaining tasks...")
            # Cancel all pending futures
            for future in future_to_file:
                future.cancel()
            print("Waiting for running tasks to finish (this might take a moment)...")
            raise  # Re-raise to exit the function/menu
    
    # Display summary
    print("\n" + "="*60)
    print("CONVERSION SUMMARY")
    print("="*60)
    print(f"Total files processed: {len(to_process)}")
    print(f"Successful conversions: {stats['successful']}")
    print(f"Failed conversions: {stats['failed']}")
    
    if failed_files:
        print(f"\nFailed files:")
        for filename, reason in failed_files:
            print(f"  - {filename}: {reason}")
    
    print("="*60 + "\n")
    input("Press Enter to return to menu...")
