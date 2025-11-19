# Quick Start Guide

## Prerequisites Check

✅ OSGEO4W installed at: `Z:\GEO\OSGeo4W`  
✅ Python 3.7+ installed  
✅ `kaartbladindex.json` in current directory

## Installation (One-time setup)

```powershell
# Navigate to the AHN Downloader directory
cd "Z:\GEO\AHN Downloader"

# Install Python dependencies
pip install -r requirements.txt
```

That's it! Only `requests` and `tqdm` need to be installed. GDAL will be auto-detected from your OSGEO4W installation.

## Running the Downloader

```powershell
python ahn_downloader.py
```

## What You'll See

```
======================================================================
AHN Downloader - Actueel Hoogtebestand Nederland
Parallel Pipeline: Download + Verification
======================================================================

Found OSGEO4W installation at: Z:\GEO\OSGeo4W
Set GDAL_DATA to: Z:\GEO\OSGeo4W\share\gdal
Set PROJ_LIB to: Z:\GEO\OSGeo4W\share\proj
✓ GDAL version: 3.x.x
✓ Download directory: downloads
✓ Progress tracking: download_progress.json

Total files: 6289
Already completed: 0
Previously failed: 0
Remaining: 6289

Starting parallel download/verification pipeline...
⚡ Downloads and verification happen simultaneously
----------------------------------------------------------------------

[1/6289] DOWNLOADING: R_01HZ2 - R_01HZ2.tif
  Size: 43.89 MB
  Downloading: 100%|████████████████| 43.9M/43.9M [00:15<00:00]
  ✓ Download complete, queuing for verification...
  [1/6289] VERIFYING: R_01HZ2...
  ✓ Verification successful for R_01HZ2

[2/6289] DOWNLOADING: R_01GZ2 - R_01GZ2.tif
  Size: 11.22 MB
  ...
```

## Interrupting and Resuming

**To pause**: Press `Ctrl+C`

The script will:
- Stop downloading new files
- Wait for current verification to complete
- Save progress
- Exit gracefully

**To resume**: Just run the script again!

```powershell
python ahn_downloader.py
```

It will:
- Load previous progress
- Skip already completed files
- Continue from where it left off

## Checking Progress

Your progress is saved in `download_progress.json`:

```json
{
  "completed": ["R_01HZ2", "R_01GZ2", ...],
  "failed": [],
  "stats": {
    "total_files": 6289,
    "completed_count": 2,
    "failed_count": 0,
    "total_bytes_downloaded": 58000000
  }
}
```

## Troubleshooting

### OSGEO4W Not Found
If you see "OSGEO4W not found in common locations":
1. Check if OSGEO4W is installed at `Z:\GEO\OSGeo4W`
2. If it's elsewhere, edit line 52 in `ahn_downloader.py` to add your path

### Network Errors
- Network failures are logged but won't stop the script
- Failed files are tracked and can be retried
- Check `download_errors.log` for details

### Disk Space
Monitor your disk space! With 6289 files averaging 30-40MB each, you need:
- **Estimated total**: ~250-300 GB

Check space:
```powershell
Get-PSDrive Z
```

## Output Files

After running:
```
Z:\GEO\AHN Downloader\
├── downloads/              ← Downloaded .tif files
│   ├── R_01HZ2.tif
│   ├── R_01GZ2.tif
│   └── ...
├── download_progress.json  ← Resume tracking
└── download_errors.log     ← Error details
```

## Performance

With the parallel pipeline:
- **Download** and **verification** happen simultaneously
- Typical speed: ~15-25 files per hour (depending on network)
- Total time for 6289 files: ~10-20 days of continuous running
- Can be paused and resumed anytime!

## Tips

1. **Run overnight**: This is a long-running process
2. **Check progress**: Look at `download_progress.json` periodically
3. **Monitor disk space**: Keep at least 50GB free
4. **Network stability**: Use wired connection if possible
5. **Resume freely**: Don't worry about interruptions, just restart!

Happy downloading! 🚀





