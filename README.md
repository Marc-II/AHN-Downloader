# AHN Downloader

A robust Python script to download all .tif files referenced in the kaartbladindex.json from the Dutch AHN (Actueel Hoogtebestand Nederland) dataset.

## Features

- ⚡ **Parallel Pipeline**: Downloads and verification happen simultaneously for optimal performance
- ✓ **Pre-Download Verification**: Automatically checks all completed files before starting downloads
- ✓ **Auto OSGEO4W Detection**: Automatically finds and configures OSGEO4W on Windows
- ✓ **Resume Capability**: Automatically resumes from where it left off if interrupted
- ✓ **File Verification**: Validates file size and runs GDAL integrity checks using gdalinfo
- ✓ **Progress Tracking**: Shows console progress bar and logs to file
- ✓ **Error Handling**: Continues with next file if one fails, logs all errors
- ✓ **Smart Skip**: Skips already downloaded and verified files
- ✓ **Auto-Repair**: Automatically re-downloads corrupted or missing files
- ✓ **Thread-Safe**: Uses proper locking for concurrent operations

## Prerequisites

### Install GDAL

GDAL is required for verifying TIF file integrity. Installation varies by platform:

#### Windows
The script **automatically detects and configures OSGEO4W** from these locations:
- `Z:\GEO\OSGeo4W` (your current installation)
- `C:\OSGeo4W64`
- `C:\OSGeo4W`
- QGIS installations in `C:\Program Files\`

**If you already have OSGEO4W installed at `Z:\GEO\OSGeo4W`, you're all set!** The script will find it automatically.

Otherwise, install using:
```powershell
# Option 1: Using OSGeo4W (Recommended)
# Download and install OSGeo4W from: https://trac.osgeo.org/osgeo4w/

# Option 2: Using Conda
conda install -c conda-forge gdal

# Option 3: Using pip with pre-built wheels
pip install GDAL
```

#### Linux (Ubuntu/Debian)
```bash
sudo apt-get update
sudo apt-get install gdal-bin python3-gdal
```

#### macOS
```bash
brew install gdal
pip install GDAL==$(gdal-config --version)
```

## Installation

1. Clone or navigate to the repository:
```bash
cd "Z:\GEO\AHN Downloader"
```

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Simply run the script:

```bash
python ahn_downloader.py
```

The script will:
1. Auto-detect and configure OSGEO4W (on Windows)
2. Load the kaartbladindex.json file
3. Check for existing progress in download_progress.json
4. Create a `downloads/` directory if it doesn't exist
5. **Run pre-download verification phase**:
   - Check all files marked as completed in progress
   - Verify file existence, correct size, and GDAL integrity
   - Automatically remove corrupted/missing files from completed list
   - Queue failed files for re-download
6. Start parallel download/verification pipeline:
   - **Download Thread**: Downloads files and queues them for verification
   - **Verification Thread**: Verifies files with GDAL while next file downloads
7. Save progress after each successful verification

### How the Parallel Pipeline Works

```
Download Thread          Verification Thread
     │                          │
     ├─ Download File 1 ────────┤
     │                          ├─ Verify File 1
     ├─ Download File 2 ────────┤
     │                          ├─ Verify File 2
     ├─ Download File 3 ────────┤
     │                          ├─ Verify File 3
     ⋮                          ⋮
```

This means while File N+1 is downloading, File N is being verified with GDAL - **saving significant time!**

### Pre-Download Verification Phase

Every time the script runs, it automatically performs a comprehensive verification of all previously completed downloads **before** starting any new downloads:

**What it checks:**
1. **File Existence**: Ensures the file exists in the `downloads/` directory
2. **File Size**: Verifies the file size matches the expected size from kaartbladindex.json
3. **GDAL Integrity**: Runs `gdalinfo` command to validate the TIF file structure

**Auto-Repair Feature:**
- Any files that fail verification are automatically:
  - Removed from the completed list
  - Deleted from disk (if corrupted)
  - Queued for re-download

This ensures your dataset is always complete and intact, automatically fixing any corruption that may have occurred due to disk errors, interrupted transfers, or other issues.

**Example output:**
```
======================================================================
PRE-DOWNLOAD VERIFICATION PHASE
Checking integrity of all completed downloads...
======================================================================
Verifying 150 completed files...

----------------------------------------------------------------------
VERIFICATION SUMMARY
----------------------------------------------------------------------
Total files checked:     150
✓ Verified OK:           148
✗ Missing files:         1
✗ Size mismatches:       0
✗ Corrupt files (GDAL):  1

⚠ 2 files will be re-downloaded
----------------------------------------------------------------------
```

### Resuming Downloads

If the download is interrupted (Ctrl+C, network failure, etc.), simply run the script again:

```bash
python ahn_downloader.py
```

It will automatically skip already downloaded files and continue from where it left off.

### Output Files

- `downloads/` - Directory containing all downloaded .tif files
- `download_progress.json` - Progress tracking (completed/failed files)
- `download_errors.log` - Detailed error log

## Progress Tracking

The script maintains a `download_progress.json` file that tracks:
- Successfully downloaded files
- Failed downloads with error messages
- Statistics (total files, bytes downloaded, etc.)
- Last update timestamp

## File Verification

The script performs comprehensive verification at two stages:

### During Download (Real-time)
Each downloaded file undergoes two verification steps:
1. **Size Check**: Compares downloaded file size with expected size from index
2. **GDAL Check**: Runs `gdalinfo` command to verify TIF integrity

Only files that pass both checks are marked as complete.

### Pre-Download Phase (On Each Run)
Before starting downloads, all previously completed files are verified:
1. **Existence Check**: Confirms file exists in downloads directory
2. **Size Check**: Verifies file size matches expected size
3. **GDAL Integrity Check**: Validates TIF structure using `gdalinfo`

Files that fail any check are automatically queued for re-download.

## Error Handling

- Network errors: Logged and skipped, can be retried by deleting the failed entry from progress file
- Corrupted downloads: Automatically deleted and marked as failed
- Partial downloads: Cleaned up and can be resumed
- All errors logged to `download_errors.log`

## Estimated Download Size

Based on the sample data:
- Average file size: ~20-40 MB
- Total files: ~10,000-15,000 (estimated)
- **Total dataset size: ~200-600 GB (estimated)**

Ensure you have sufficient disk space before starting!

## Troubleshooting

### "GDAL not found" error
Make sure GDAL is installed and accessible to Python. Test with:
```python
python -c "from osgeo import gdal; print(gdal.__version__)"
```

### Network timeouts
The script has a 60-second timeout per request. If you have slow internet, you may need to increase this in the code.

### Disk space
Check available space regularly:
```bash
# Windows PowerShell
Get-PSDrive Z

# Linux/macOS
df -h
```

## License

This script is for downloading publicly available Dutch government data from PDOK (Publieke Dienstverlening Op de Kaart).

