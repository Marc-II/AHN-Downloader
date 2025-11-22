import sys
import os
import logging
import threading
import queue
from typing import List, Dict

from .config import KAARTBLAD_INDEX, DOWNLOAD_DIR, PROGRESS_LOG, VERIFICATION_QUEUE_SIZE, ERROR_LOG
from .utils.logging_setup import setup_logging
from .utils.os_utils import setup_osgeo4w
from .core.progress import DownloadProgress
from .core.downloader import download_worker, verification_worker, verify_all_downloads
from .core.converter import run_conversion

# Initialize logging
logger = setup_logging()

def load_kaartblad_index(index_file: str) -> List[Dict]:
    """Load and parse the kaartbladindex.json file."""
    import json
    try:
        with open(index_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        features = data.get('features', [])
        logger.info(f"Loaded {len(features)} features from {index_file}")
        return features
        
    except Exception as e:
        logger.error(f"Error loading {index_file}: {e}")
        sys.exit(1)

def run_downloader():
    """Main download orchestration with parallel download/verification pipeline."""
    print("=" * 70)
    print("AHN Downloader - Actueel Hoogtebestand Nederland")
    print("Parallel Pipeline: Download + Verification")
    print("=" * 70)
    print()
    
    # Check if GDAL is available
    try:
        from osgeo import gdal
        gdal_version = gdal.__version__
        print(f"✓ GDAL version: {gdal_version}")
    except ImportError:
        print("⚠ GDAL not available - integrity checks will be skipped")
        print("  Install OSGEO4W or GDAL for full verification")
    except Exception:
        print("⚠ GDAL found but version unavailable")
    
    # Check if index file exists
    if not os.path.exists(KAARTBLAD_INDEX):
        logger.error(f"Index file not found: {KAARTBLAD_INDEX}")
        print(f"Error: Index file '{KAARTBLAD_INDEX}' not found.")
        input("Press Enter to return to menu...")
        return
    
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
        input("Press Enter to return to menu...")
        return
    
    confirm = input("Start download? [Y/n] ").strip().lower()
    if confirm == 'n':
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
    input("Press Enter to return to menu...")

def verify_only():
    """Run verification on existing downloads."""
    print("=" * 70)
    print("Verification Only Mode")
    print("=" * 70)
    
    if not os.path.exists(KAARTBLAD_INDEX):
        print(f"Error: Index file '{KAARTBLAD_INDEX}' not found.")
        input("Press Enter to return to menu...")
        return

    progress = DownloadProgress(PROGRESS_LOG)
    features = load_kaartblad_index(KAARTBLAD_INDEX)
    
    verify_all_downloads(features, progress)
    input("Press Enter to return to menu...")

def main_menu():
    while True:
        # Clear screen (optional, but nice)
        os.system('cls' if os.name == 'nt' else 'clear')
        
        print("=" * 50)
        print("AHN Downloader - Main Menu")
        print("=" * 50)
        print("1. Start/Resume Download")
        print("2. Verify Existing Downloads")
        print("3. Convert Downloads to WGS84")
        print("4. Exit")
        print("=" * 50)
        
        choice = input("Select an option (1-4): ").strip()
        
        if choice == '1':
            run_downloader()
        elif choice == '2':
            verify_only()
        elif choice == '3':
            run_conversion()
        elif choice == '4':
            print("Exiting...")
            sys.exit(0)
        else:
            input("Invalid option. Press Enter to try again...")

def main():
    # Setup OSGeo4W first
    setup_osgeo4w()
    
    try:
        main_menu()
    except KeyboardInterrupt:
        print("\n\nExiting...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        print(f"\nAn unexpected error occurred: {e}")
        print(f"Check {ERROR_LOG} for details.")
        sys.exit(1)

if __name__ == "__main__":
    main()
