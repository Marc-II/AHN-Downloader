#!/usr/bin/env python3
"""
AHN (Actueel Hoogtebestand Nederland) Downloader
Downloads .tif files referenced in kaartbladindex.json with resume capability.
Uses parallel download/verification pipeline for optimal performance.
"""

import json
import os
import sys
import logging
import threading
import queue
import platform
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime

import requests
from tqdm import tqdm

# Configuration
KAARTBLAD_INDEX = "kaartbladindex.json"
DOWNLOAD_DIR = "downloads"
PROGRESS_LOG = "download_progress.json"
ERROR_LOG = "download_errors.log"
CHUNK_SIZE = 8192  # 8KB chunks for downloading
VERIFICATION_QUEUE_SIZE = 3  # Number of files to buffer for verification

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(ERROR_LOG),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


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


# Set up OSGEO4W before importing GDAL
setup_osgeo4w()

# Now import GDAL
try:
    from osgeo import gdal
    gdal.PushErrorHandler('CPLQuietErrorHandler')  # Suppress GDAL warnings to console
    GDAL_AVAILABLE = True
except ImportError as e:
    logger.error(f"Failed to import GDAL: {e}")
    GDAL_AVAILABLE = False


class DownloadProgress:
    """Manages download progress tracking and persistence (thread-safe)."""
    
    def __init__(self, progress_file: str):
        self.progress_file = progress_file
        self.data = self._load_progress()
        self.lock = threading.Lock()  # Thread-safe access
    
    def _load_progress(self) -> Dict:
        """Load existing progress or create new tracking data."""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)
                    logger.info(f"Loaded progress: {len(data.get('completed', []))} completed, "
                              f"{len(data.get('failed', []))} failed")
                    return data
            except Exception as e:
                logger.error(f"Error loading progress file: {e}")
                return self._new_progress()
        return self._new_progress()
    
    def _new_progress(self) -> Dict:
        """Create new progress tracking structure."""
        return {
            "completed": [],  # List of successfully downloaded kaartbladNr
            "failed": [],     # List of failed kaartbladNr with error info
            "last_updated": None,
            "stats": {
                "total_files": 0,
                "completed_count": 0,
                "failed_count": 0,
                "total_bytes_downloaded": 0
            }
        }
    
    def save(self):
        """Persist progress to disk (thread-safe)."""
        with self.lock:
            self.data["last_updated"] = datetime.now().isoformat()
            try:
                with open(self.progress_file, 'w') as f:
                    json.dump(self.data, f, indent=2)
            except Exception as e:
                logger.error(f"Error saving progress file: {e}")
    
    def is_completed(self, kaartblad_nr: str) -> bool:
        """Check if a file has already been successfully downloaded (thread-safe)."""
        with self.lock:
            return kaartblad_nr in self.data["completed"]
    
    def mark_completed(self, kaartblad_nr: str, file_size: int):
        """Mark a file as successfully downloaded (thread-safe)."""
        with self.lock:
            if kaartblad_nr not in self.data["completed"]:
                self.data["completed"].append(kaartblad_nr)
                self.data["stats"]["completed_count"] = len(self.data["completed"])
                self.data["stats"]["total_bytes_downloaded"] += file_size
        self.save()
    
    def mark_failed(self, kaartblad_nr: str, error: str):
        """Mark a file as failed with error message (thread-safe)."""
        with self.lock:
            failed_entry = {
                "kaartbladNr": kaartblad_nr,
                "error": error,
                "timestamp": datetime.now().isoformat()
            }
            self.data["failed"].append(failed_entry)
            self.data["stats"]["failed_count"] = len(self.data["failed"])
        self.save()


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


def download_file(url: str, destination: str, expected_size: Optional[int] = None) -> bool:
    """
    Download a file from URL to destination.
    Returns True if successful, False otherwise.
    """
    try:
        # Start download with streaming
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        
        # Get actual file size from headers
        total_size = int(response.headers.get('content-length', 0))
        
        # Verify expected size if provided
        if expected_size and total_size != expected_size:
            logger.warning(f"Size mismatch: expected {expected_size}, got {total_size}")
        
        # Download with progress bar
        with open(destination, 'wb') as f:
            with tqdm(
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                desc=f"  Downloading",
                leave=False
            ) as pbar:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
        
        # Verify downloaded file size
        actual_size = os.path.getsize(destination)
        if total_size > 0 and actual_size != total_size:
            logger.error(f"Downloaded file size mismatch: expected {total_size}, got {actual_size}")
            return False
        
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Download error for {url}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error downloading {url}: {e}")
        return False


def load_kaartblad_index(index_file: str) -> List[Dict]:
    """Load and parse the kaartbladindex.json file."""
    try:
        with open(index_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        features = data.get('features', [])
        logger.info(f"Loaded {len(features)} features from {index_file}")
        return features
        
    except Exception as e:
        logger.error(f"Error loading {index_file}: {e}")
        sys.exit(1)


def verify_all_downloads(features: List[Dict], progress: DownloadProgress) -> Dict:
    """
    Verify all files marked as completed in kaartbladindex.json.
    Checks existence, size, and GDAL integrity.
    Removes failed files from completed list and deletes corrupted files.
    Returns statistics dictionary.
    """
    print("\n" + "=" * 70)
    print("PRE-DOWNLOAD VERIFICATION PHASE")
    print("Checking integrity of all completed downloads...")
    print("=" * 70)
    
    stats = {
        'total_checked': 0,
        'verified_ok': 0,
        'missing': 0,
        'size_mismatch': 0,
        'corrupt': 0
    }
    
    # Build a lookup map of kaartbladNr -> feature for efficient access
    features_map = {}
    for feature in features:
        properties = feature.get('properties', {})
        kaartblad_nr = properties.get('kaartbladNr', 'unknown')
        features_map[kaartblad_nr] = feature
    
    # Get list of completed items to check
    completed_list = progress.data["completed"].copy()
    stats['total_checked'] = len(completed_list)
    
    if stats['total_checked'] == 0:
        print("No files to verify (no completed downloads yet).\n")
        return stats
    
    print(f"Verifying {stats['total_checked']} completed files...\n")
    
    files_to_redownload = []
    
    # Progress bar for verification
    with tqdm(total=stats['total_checked'], desc="Verifying", unit="file") as pbar:
        for kaartblad_nr in completed_list:
            # Get feature details
            if kaartblad_nr not in features_map:
                logger.warning(f"KaartbladNr {kaartblad_nr} not found in index, skipping")
                pbar.update(1)
                continue
            
            feature = features_map[kaartblad_nr]
            properties = feature.get('properties', {})
            filename = sanitize_filename(properties.get('name', ''))
            expected_size = int(properties.get('length', 0))
            file_path = os.path.join(DOWNLOAD_DIR, filename)
            
            # Check 1: File exists
            if not os.path.exists(file_path):
                logger.warning(f"Missing file: {filename} (kaartblad: {kaartblad_nr})")
                stats['missing'] += 1
                files_to_redownload.append(kaartblad_nr)
                pbar.update(1)
                continue
            
            # Check 2: File size matches
            actual_size = os.path.getsize(file_path)
            if actual_size != expected_size:
                logger.warning(f"Size mismatch for {filename}: expected {expected_size}, got {actual_size}")
                stats['size_mismatch'] += 1
                files_to_redownload.append(kaartblad_nr)
                # Delete corrupted file
                try:
                    os.remove(file_path)
                    logger.info(f"Deleted corrupted file: {filename}")
                except Exception as e:
                    logger.error(f"Failed to delete {filename}: {e}")
                pbar.update(1)
                continue
            
            # Check 3: GDAL integrity check
            if not verify_file_with_gdalinfo(file_path):
                logger.warning(f"GDAL integrity check failed for {filename}")
                stats['corrupt'] += 1
                files_to_redownload.append(kaartblad_nr)
                # Delete corrupted file
                try:
                    os.remove(file_path)
                    logger.info(f"Deleted corrupted file: {filename}")
                except Exception as e:
                    logger.error(f"Failed to delete {filename}: {e}")
                pbar.update(1)
                continue
            
            # All checks passed
            stats['verified_ok'] += 1
            pbar.update(1)
    
    # Remove failed files from completed list
    if files_to_redownload:
        print(f"\nRemoving {len(files_to_redownload)} files from completed list (will be re-downloaded)...")
        with progress.lock:
            for kaartblad_nr in files_to_redownload:
                if kaartblad_nr in progress.data["completed"]:
                    progress.data["completed"].remove(kaartblad_nr)
            progress.data["stats"]["completed_count"] = len(progress.data["completed"])
        progress.save()
    
    # Print summary
    print("\n" + "-" * 70)
    print("VERIFICATION SUMMARY")
    print("-" * 70)
    print(f"Total files checked:     {stats['total_checked']}")
    print(f"✓ Verified OK:           {stats['verified_ok']}")
    print(f"✗ Missing files:         {stats['missing']}")
    print(f"✗ Size mismatches:       {stats['size_mismatch']}")
    print(f"✗ Corrupt files (GDAL):  {stats['corrupt']}")
    
    failed_total = stats['missing'] + stats['size_mismatch'] + stats['corrupt']
    if failed_total > 0:
        print(f"\n⚠ {failed_total} files will be re-downloaded")
    print("-" * 70)
    print()
    
    return stats


def sanitize_filename(filename: str) -> str:
    """Sanitize filename to prevent directory traversal."""
    # Get just the basename to prevent path traversal
    return os.path.basename(filename)


def download_worker(features: List[Dict], progress: DownloadProgress, 
                   verification_queue: queue.Queue, stop_event: threading.Event,
                   total_features: int):
    """
    Worker thread that downloads files and puts them in verification queue.
    """
    for idx, feature in enumerate(features, 1):
        if stop_event.is_set():
            break
        
        properties = feature.get('properties', {})
        kaartblad_nr = properties.get('kaartbladNr', 'unknown')
        url = properties.get('url', '')
        filename = sanitize_filename(properties.get('name', ''))
        expected_size = int(properties.get('length', 0))
        
        # Skip if already completed
        if progress.is_completed(kaartblad_nr):
            continue
        
        # Prepare destination path
        destination = os.path.join(DOWNLOAD_DIR, filename)
        
        print(f"\n[{idx}/{total_features}] DOWNLOADING: {kaartblad_nr} - {filename}")
        print(f"  Size: {expected_size / (1024*1024):.2f} MB")
        
        # Skip if file exists and has correct size (will be verified by verification thread)
        if os.path.exists(destination):
            file_size = os.path.getsize(destination)
            if file_size == expected_size:
                print(f"  ✓ File exists with correct size, queuing for verification...")
                verification_queue.put((kaartblad_nr, destination, expected_size, idx, total_features))
                continue
            else:
                print(f"  ⚠ File exists but size mismatch ({file_size} vs {expected_size}), re-downloading...")
                try:
                    os.remove(destination)
                except:
                    pass
        
        # Download file
        success = download_file(url, destination, expected_size)
        
        if not success:
            logger.error(f"Failed to download {kaartblad_nr}")
            progress.mark_failed(kaartblad_nr, "Download failed")
            # Clean up partial download
            if os.path.exists(destination):
                try:
                    os.remove(destination)
                except:
                    pass
            continue
        
        # Put downloaded file in verification queue
        print(f"  ✓ Download complete, queuing for verification...")
        verification_queue.put((kaartblad_nr, destination, expected_size, idx, total_features))
    
    # Signal verification thread that download is complete
    verification_queue.put(None)


def verification_worker(progress: DownloadProgress, verification_queue: queue.Queue, 
                       stop_event: threading.Event):
    """
    Worker thread that verifies files from the queue.
    """
    while not stop_event.is_set():
        try:
            item = verification_queue.get(timeout=1)
            
            # None signals end of downloads
            if item is None:
                break
            
            kaartblad_nr, destination, expected_size, idx, total_features = item
            
            print(f"  [{idx}/{total_features}] VERIFYING: {kaartblad_nr}...")
            
            # Verify with gdalinfo
            if verify_file_with_gdalinfo(destination):
                print(f"  ✓ Verification successful for {kaartblad_nr}")
                progress.mark_completed(kaartblad_nr, expected_size)
            else:
                print(f"  ✗ GDAL verification failed for {kaartblad_nr}")
                logger.error(f"GDAL verification failed for {kaartblad_nr}")
                progress.mark_failed(kaartblad_nr, "GDAL verification failed")
                # Remove corrupted file
                if os.path.exists(destination):
                    try:
                        os.remove(destination)
                    except:
                        pass
            
            verification_queue.task_done()
            
        except queue.Empty:
            continue


def main():
    """Main download orchestration with parallel download/verification pipeline."""
    print("=" * 70)
    print("AHN Downloader - Actueel Hoogtebestand Nederland")
    print("Parallel Pipeline: Download + Verification")
    print("=" * 70)
    print()
    
    # Check if GDAL is available
    if GDAL_AVAILABLE:
        try:
            gdal_version = gdal.__version__
            print(f"✓ GDAL version: {gdal_version}")
        except:
            print("⚠ GDAL found but version unavailable")
    else:
        print("⚠ GDAL not available - integrity checks will be skipped")
        print("  Install OSGEO4W or GDAL for full verification")
    
    # Check if index file exists
    if not os.path.exists(KAARTBLAD_INDEX):
        logger.error(f"Index file not found: {KAARTBLAD_INDEX}")
        sys.exit(1)
    
    # Create download directory
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    print(f"✓ Download directory: {DOWNLOAD_DIR}")
    
    # Load progress
    progress = DownloadProgress(PROGRESS_LOG)
    print(f"✓ Progress tracking: {PROGRESS_LOG}")
    print()
    
    # Load kaartblad index
    features = load_kaartblad_index(KAARTBLAD_INDEX)
    total_features = len(features)
    progress.data["stats"]["total_files"] = total_features
    
    # Run pre-download verification phase
    verify_all_downloads(features, progress)
    
    # Calculate what needs to be downloaded (after verification)
    completed_count = len(progress.data["completed"])
    failed_count = len(progress.data["failed"])
    remaining_count = total_features - completed_count
    
    print(f"Total files: {total_features}")
    print(f"Already completed: {completed_count}")
    print(f"Previously failed: {failed_count}")
    print(f"Remaining: {remaining_count}")
    print()
    
    if remaining_count == 0:
        print("All files have been processed!")
        return
    
    # Set up parallel pipeline
    print("Starting parallel download/verification pipeline...")
    print("⚡ Downloads and verification happen simultaneously")
    print("-" * 70)
    print()
    
    # Create queue and threading events
    verification_queue = queue.Queue(maxsize=VERIFICATION_QUEUE_SIZE)
    stop_event = threading.Event()
    
    # Start verification thread
    verification_thread = threading.Thread(
        target=verification_worker,
        args=(progress, verification_queue, stop_event),
        daemon=True
    )
    verification_thread.start()
    
    # Start download thread (runs in main thread for better control)
    try:
        download_worker(features, progress, verification_queue, stop_event, total_features)
        
        # Wait for verification queue to be processed
        print("\n⏳ Waiting for remaining verifications to complete...")
        verification_thread.join(timeout=300)  # 5 minute timeout
        
    except KeyboardInterrupt:
        print("\n\n⚠ Interrupting download pipeline...")
        stop_event.set()
        verification_thread.join(timeout=10)
        raise
    
    # Final summary
    print()
    print("=" * 70)
    print("Download Summary")
    print("=" * 70)
    print(f"Total files: {progress.data['stats']['total_files']}")
    print(f"Completed: {progress.data['stats']['completed_count']}")
    print(f"Failed: {progress.data['stats']['failed_count']}")
    total_gb = progress.data['stats']['total_bytes_downloaded'] / (1024**3)
    print(f"Total downloaded: {total_gb:.2f} GB")
    print()
    print(f"Progress saved to: {PROGRESS_LOG}")
    print(f"Errors logged to: {ERROR_LOG}")
    print()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nDownload interrupted by user.")
        print("Progress has been saved. Run the script again to resume.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

