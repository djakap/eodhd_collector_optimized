#!/usr/bin/env bash
# =============================================================================
# One-time setup: configure Google Drive backup for QuestDB
#
# What this script does:
#   1. Guides you to create a GCP service account
#   2. Generates the rclone config that uses that service account
#   3. Creates secrets/ directory with the config files
#
# Run this ONCE on the host before starting docker-compose.
# =============================================================================

set -e

SECRETS_DIR="$(dirname "$0")/../secrets"
mkdir -p "$SECRETS_DIR"

echo ""
echo "============================================================"
echo "  QuestDB → Google Drive Backup Setup"
echo "============================================================"
echo ""
echo "STEP 1: Create a Google Cloud Service Account"
echo "----------------------------------------------"
echo "  1. Go to: https://console.cloud.google.com/iam-admin/serviceaccounts"
echo "  2. Select or create a project"
echo "  3. Click 'Create Service Account'"
echo "     - Name: questdb-backup"
echo "     - Click 'Create and Continue' → 'Done'"
echo "  4. Click the service account → 'Keys' tab → 'Add Key' → 'JSON'"
echo "  5. Download the JSON file"
echo ""
read -rp "Paste the full path to the downloaded JSON key file: " KEY_PATH

if [ ! -f "$KEY_PATH" ]; then
  echo "ERROR: File not found: $KEY_PATH"
  exit 1
fi

cp "$KEY_PATH" "$SECRETS_DIR/service_account.json"
echo "✓ Saved to secrets/service_account.json"

echo ""
echo "STEP 2: Enable Google Drive API"
echo "--------------------------------"
echo "  1. Go to: https://console.cloud.google.com/apis/library/drive.googleapis.com"
echo "  2. Click 'Enable'"
echo ""
read -rp "Press Enter when the Drive API is enabled..."

echo ""
echo "STEP 3: Share a Google Drive folder with the service account"
echo "-------------------------------------------------------------"
SERVICE_ACCOUNT_EMAIL=$(python3 -c "import json; d=json.load(open('$SECRETS_DIR/service_account.json')); print(d['client_email'])")
echo "  Service account email: $SERVICE_ACCOUNT_EMAIL"
echo ""
echo "  1. Go to Google Drive"
echo "  2. Create a folder named 'questdb-backups' (or any name you like)"
echo "  3. Right-click → Share → paste the email above → Editor role"
echo "  4. Copy the folder ID from the URL:"
echo "     https://drive.google.com/drive/folders/<FOLDER_ID_IS_HERE>"
echo ""
read -rp "Paste the Google Drive folder ID: " FOLDER_ID

if [ -z "$FOLDER_ID" ]; then
  echo "ERROR: Folder ID cannot be empty"
  exit 1
fi

echo ""
echo "STEP 4: Generating rclone config..."
echo "------------------------------------"

mkdir -p "$SECRETS_DIR"
cat > "$SECRETS_DIR/rclone.conf" <<EOF
[gdrive]
type = drive
scope = drive
service_account_file = /app/secrets/service_account.json
EOF

echo "✓ Saved to secrets/rclone.conf"

echo ""
echo "STEP 5: Update prefect.yaml with your folder ID"
echo "-------------------------------------------------"
echo "  Edit prefect.yaml → questdb-backup deployment → set:"
echo "    gdrive_folder_id: \"$FOLDER_ID\""
echo "  Then set active: true to enable the daily schedule."
echo ""
echo "  OR set an environment variable in docker-compose.yml:"
echo "    - GDRIVE_FOLDER_ID=$FOLDER_ID"
echo ""

echo "STEP 6: Add secrets/ to .gitignore"
echo "------------------------------------"
GITIGNORE="$(dirname "$0")/../.gitignore"
if ! grep -q "^secrets/" "$GITIGNORE" 2>/dev/null; then
  echo "secrets/" >> "$GITIGNORE"
  echo "✓ Added secrets/ to .gitignore"
else
  echo "✓ secrets/ already in .gitignore"
fi

echo ""
echo "STEP 7: Rebuild and restart Docker"
echo "------------------------------------"
echo "  Run these commands from the project root:"
echo ""
echo "    docker compose build prefect-worker"
echo "    docker compose up -d"
echo ""
echo "  Then verify rclone can reach Google Drive:"
echo ""
echo "    docker exec prefect-worker rclone lsd gdrive: --drive-root-folder-id $FOLDER_ID"
echo ""
echo "  To run a backup manually now:"
echo ""
echo "    # From Prefect UI at http://localhost:4200"
echo "    # → Deployments → questdb-backup → Run"
echo ""
echo "============================================================"
echo "  Setup complete!"
echo "============================================================"
