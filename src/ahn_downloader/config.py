import os

# Configuration
KAARTBLAD_INDEX = "kaartbladindex.json"
DOWNLOAD_DIR = "downloads"
WGS84_OUTPUT_DIR = "downloads_wgs84"
PROGRESS_LOG = "download_progress.json"
ERROR_LOG = "download_errors.log"
CHUNK_SIZE = 8192  # 8KB chunks for downloading
VERIFICATION_QUEUE_SIZE = 3  # Number of files to buffer for verification
CONVERSION_WORKERS = 4  # Number of parallel conversion threads
