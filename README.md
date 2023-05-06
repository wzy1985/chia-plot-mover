# Chia Plot Mover

Automatically moves Chia plots to a specified location.

If you have multiple hard drives, this little tool will automatically handle all plot files and move them to your specified destination.

## Features

* Supports multiple source directories 
* Supports multiple destination directories 
* Supports resuming from breakpoints 
* Parallel processing of multiple destination directories 
* Supports new plot replacement

## Requirements

Requires Python 3.7 or later versions.

## Install

```bash
pip3 install -r requirements.txt
```

## Usage

```bash
python3 mover.py --config=<config_file>
```

## Config file

```json
{
  "main": {
    // Optional
    // File to store runtime context data.
    "session_file": "/tmp/chia-plot-mover.session",
    
    // Optional
    "lock_file": "/tmp/chia-plot-mover.lock",
    
    // Optional
    // Once set, the script will automatically delete files older than
    // this time when the destination disk capacity is insufficient.
    "dest_file_expiration_time": "2022-01-01 00:00:00"
  },
  "source": [
    {
      // Required
      // Name of the source.
      "name": "ssd0",
      
      // Required
      // Absolute path of the source.
      "dir": "/mnt/ssd0/plot"
    },
    {
      "name": "ssd1",
      "dir": "/mnt/ssd1/plot"
    }
  ],
  "dest": [
    {
      // Required
      // Name of the destination.
      "name": "hdd0",
      
      // Required
      // Absolute path of the destination.
      "dir": "/mnt/hdd0/plot"
    },
    {
      "name": "hdd1",
      "dir": "/mnt/hdd1/plot"
    },
    {
      "name": "hdd2",
      "dir": "/mnt/hdd2/plot"
    },
    {
      "name": "hdd3",
      "dir": "/mnt/hdd3/plot"
    }
  ]
}
```
