import os
import requests
import logging
import queue
import threading
from typing import Optional, List, Dict
from tqdm import tqdm

from ..config import CHUNK_SIZE, DOWNLOAD_DIR
from ..utils.os_utils import sanitize_filename
from .progress import DownloadProgress
from .verifier import verify_file_with_gdalinfo

logger = logging.getLogger("ahn_downloader")

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
