"""
Prefect Flow: QuestDB Filesystem Backup

Directly tars the QuestDB data volume to a .tar.gz file.
Safe for live databases — historical partitions are immutable in QuestDB.

Restore on another machine:
  tar -xzf questdb_backup_2026-05-28_160000.tar.gz
  docker run -p 9001:9000 -p 8812:8812 \\
    -v $(pwd)/questdb-data:/var/lib/questdb \\
    questdb/questdb:7.3.10
"""

import tarfile
from datetime import datetime
from pathlib import Path

from prefect import flow, task, get_run_logger

QUESTDB_DATA_PATH = Path("/questdb-data")
BACKUP_DIR = Path("/backup")


@task(name="tar-questdb-data")
def create_backup(backup_name: str) -> Path:
    logger = get_run_logger()
    archive_path = BACKUP_DIR / f"{backup_name}.tar.gz"

    files = [f for f in QUESTDB_DATA_PATH.rglob("*") if f.is_file()]
    total_files = len(files)
    total_bytes = sum(f.stat().st_size for f in files)
    logger.info(f"Archiving {total_bytes/1e9:.2f} GB ({total_files:,} files) → {archive_path.name}")

    done_bytes = [0]
    last_pct = [0]

    def progress_filter(tarinfo):
        if tarinfo.isfile():
            done_bytes[0] += tarinfo.size
            pct = int(done_bytes[0] / total_bytes * 100) if total_bytes else 100
            if pct >= last_pct[0] + 10:
                last_pct[0] = pct - (pct % 10)
                logger.info(f"  {last_pct[0]}% — {done_bytes[0]/1e9:.2f} / {total_bytes/1e9:.2f} GB")
        return tarinfo

    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(QUESTDB_DATA_PATH, arcname="questdb-data", filter=progress_filter)

    compressed_gb = archive_path.stat().st_size / 1e9
    logger.info(f"Archive complete: {archive_path.name} ({compressed_gb:.2f} GB compressed)")
    return archive_path


@task(name="cleanup-old-backups")
def cleanup_old_backups(keep: int):
    logger = get_run_logger()
    archives = sorted(BACKUP_DIR.glob("questdb_backup_*.tar.gz"))
    for old in archives[:-keep]:
        old.unlink()
        logger.info(f"Deleted {old.name}")
    logger.info(f"Keeping {min(len(archives), keep)} backup(s) in {BACKUP_DIR}")


@flow(name="questdb-local-backup", log_prints=True)
def questdb_backup_flow(keep_local_backups: int = 3):
    logger = get_run_logger()
    backup_name = f"questdb_backup_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}"
    logger.info(f"Starting backup: {backup_name}")

    archive_path = create_backup(backup_name)
    cleanup_old_backups(keep_local_backups)

    logger.info(f"Done — copy to laptop: scp <host>:{archive_path} .")
