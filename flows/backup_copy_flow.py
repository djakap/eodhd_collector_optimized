"""
Prefect Flow: Copy QuestDB backup from container to host filesystem.

Runs on the HOST worker (host-pool), not inside the Docker container,
because docker exec needs direct access to the Docker socket.
"""

import subprocess
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, get_run_logger

SCRIPT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts", "copy_backup.sh")


@flow(name="backup-copy", log_prints=True)
def backup_copy_flow(latest_only: bool = True):
    log = get_run_logger()
    log.info(f"Running: {SCRIPT} {'--latest' if latest_only else '(all)'}")

    args = [SCRIPT]
    if latest_only:
        args.append("--latest")

    result = subprocess.run(args, capture_output=True, text=True)

    if result.stdout:
        for line in result.stdout.strip().splitlines():
            log.info(line)
    if result.stderr:
        for line in result.stderr.strip().splitlines():
            log.warning(line)

    if result.returncode != 0:
        raise RuntimeError(f"copy_backup.sh exited with code {result.returncode}")

    log.info("Done.")


if __name__ == "__main__":
    backup_copy_flow()
