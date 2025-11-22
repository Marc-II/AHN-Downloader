import os
import json
import threading
import logging
from datetime import datetime
from typing import Dict

logger = logging.getLogger("ahn_downloader")

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
