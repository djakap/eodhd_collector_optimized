#!/bin/bash
# Copy QuestDB backup files from prefect-worker container to local backups/ folder.
# Needed because Docker Desktop + WSL2 bind mounts are not directly accessible from WSL.
#
# Usage:
#   ./scripts/copy_backup.sh            # copy all missing backups
#   ./scripts/copy_backup.sh --latest   # copy only the latest backup

set -e

CONTAINER="prefect-worker"
BACKUP_DIR="$(cd "$(dirname "$0")/.." && pwd)/backups"
mkdir -p "$BACKUP_DIR"

echo "=== QuestDB Backup Copy ==="
echo "Container : $CONTAINER"
echo "Destination: $BACKUP_DIR"
echo ""

# Check container is running
if ! docker inspect --format '{{.State.Running}}' "$CONTAINER" 2>/dev/null | grep -q true; then
    echo "ERROR: Container '$CONTAINER' is not running."
    exit 1
fi

# List backup files in container
FILES=$(docker exec "$CONTAINER" sh -c 'ls /backup/questdb_backup_*.tar.gz 2>/dev/null | sort' 2>/dev/null)
if [ -z "$FILES" ]; then
    echo "No backup files found in container at /backup/"
    exit 1
fi

# If --latest flag, only take the last file
if [ "$1" = "--latest" ]; then
    FILES=$(echo "$FILES" | tail -1)
fi

COPIED=0
SKIPPED=0

while IFS= read -r FILEPATH; do
    FILENAME=$(basename "$FILEPATH")
    DEST="$BACKUP_DIR/$FILENAME"

    if [ -f "$DEST" ]; then
        echo "SKIP  $FILENAME (already exists)"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    echo -n "COPY  $FILENAME ... "
    docker exec "$CONTAINER" cat "$FILEPATH" > "$DEST"
    SIZE=$(du -h "$DEST" | cut -f1)
    echo "done ($SIZE)"
    COPIED=$((COPIED + 1))
done <<< "$FILES"

echo ""
echo "Done. Copied: $COPIED | Skipped (already exist): $SKIPPED"
echo "Files in $BACKUP_DIR:"
ls -lh "$BACKUP_DIR"/*.tar.gz 2>/dev/null || echo "  (none)"
