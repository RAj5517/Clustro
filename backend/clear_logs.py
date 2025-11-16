"""
Clear Log Files Utility

Clears log files from the logs directory.
Optionally clears rotated backup logs as well.
"""

import sys
from pathlib import Path
import glob

def clear_logs(clear_backups: bool = False):
    """
    Clear log files.
    
    Args:
        clear_backups: If True, also clears rotated backup log files
    """
    # Get logs directory
    script_dir = Path(__file__).parent
    logs_dir = script_dir / 'logs'
    
    if not logs_dir.exists():
        print(f"Logs directory does not exist: {logs_dir}")
        return
    
    # Clear main log file
    main_log = logs_dir / 'auraverse_backend.log'
    if main_log.exists():
        main_log.write_text('')  # Clear file
        print(f"✓ Cleared: {main_log.name}")
    
    # Clear backup logs if requested
    if clear_backups:
        backup_pattern = str(logs_dir / 'auraverse_backend.log.*')
        backup_files = glob.glob(backup_pattern)
        
        for backup_file in backup_files:
            backup_path = Path(backup_file)
            backup_path.unlink()
            print(f"✓ Deleted: {backup_path.name}")
    
    print(f"\nLog files cleared successfully!")
    if not clear_backups:
        print("Note: Rotated backup logs were not cleared. Use --all to clear them too.")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Clear log files')
    parser.add_argument('--all', action='store_true',
                       help='Also clear rotated backup log files')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Clear Log Files")
    print("=" * 60)
    print()
    
    clear_logs(clear_backups=args.all)

