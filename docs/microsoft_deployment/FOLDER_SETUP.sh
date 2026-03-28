#!/bin/bash
################################################################################
# THYROID_2026 Secure Folder Structure Setup
#
# Usage: chmod +x FOLDER_SETUP.sh && ./FOLDER_SETUP.sh
#
# Creates FileVault-encrypted local folder hierarchy at:
# /Users/lhglosser/THYROID_SECURE_2026/
#
# Sets appropriate permissions for PHI isolation:
# - 00_RAW_PHI/: Read-only after ingestion (chmod 500)
# - 01_SILVER_DEID_PARQUET/: Read-write for refresh (chmod 755)
# - SCRIPTS/, DOCUMENTATION/: Executable (chmod 755)
# - VALIDATION_AUDITS/: Audit logs (chmod 755)
#
# Prerequisites: Verify FileVault is enabled on /Users/lhglosser
#                Run as: lhglosser user on macOS
#
################################################################################

set -e  # Exit on any error

ROOT_DIR="/Users/lhglosser/THYROID_SECURE_2026"
USER="lhglosser"

echo "=========================================="
echo "THYROID_2026 Folder Setup Script"
echo "=========================================="
echo ""

# Verify user running script
CURRENT_USER=$(whoami)
if [ "$CURRENT_USER" != "$USER" ]; then
    echo "ERROR: This script must be run as '$USER' user (currently: $CURRENT_USER)"
    exit 1
fi

# Check if FileVault is enabled
echo "[1/3] Checking FileVault encryption status..."
FILEVAULT_STATUS=$(diskutil secureDelete info /Users/$USER 2>/dev/null || echo "Not Available")
if [[ "$FILEVAULT_STATUS" != *"Not Available"* ]]; then
    echo "       ✓ FileVault is enabled"
else
    echo "       WARNING: FileVault status unclear. Please verify manually:"
    echo "       System Preferences → Security & Privacy → FileVault"
fi
echo ""

# Create root directory
echo "[2/3] Creating folder hierarchy..."
if [ -d "$ROOT_DIR" ]; then
    echo "       ! $ROOT_DIR already exists (skipping create, but will set permissions)"
else
    echo "       Creating: $ROOT_DIR"
    mkdir -p "$ROOT_DIR"
fi

# Create all subdirectories
echo "       Creating subdirectories..."

mkdir -p "$ROOT_DIR/00_RAW_PHI"
mkdir -p "$ROOT_DIR/00_RAW_PHI/source_extracts"

mkdir -p "$ROOT_DIR/01_SILVER_DEID_PARQUET"
mkdir -p "$ROOT_DIR/01_SILVER_DEID_PARQUET/validation_tables"

mkdir -p "$ROOT_DIR/02_GOLD_POWERBI"
mkdir -p "$ROOT_DIR/02_GOLD_POWERBI/templates"
mkdir -p "$ROOT_DIR/02_GOLD_POWERBI/queries"

mkdir -p "$ROOT_DIR/03_DEID_EXPORTS"

mkdir -p "$ROOT_DIR/04_EXTRACTION_OUTPUTS"
mkdir -p "$ROOT_DIR/04_EXTRACTION_OUTPUTS/nlm_cell_extractions"
mkdir -p "$ROOT_DIR/04_EXTRACTION_OUTPUTS/data_quality_issues"
mkdir -p "$ROOT_DIR/04_EXTRACTION_OUTPUTS/reconciliation_logs"

mkdir -p "$ROOT_DIR/05_ARCHIVE_BACKUPS"

mkdir -p "$ROOT_DIR/SCRIPTS"

mkdir -p "$ROOT_DIR/DOCUMENTATION"

mkdir -p "$ROOT_DIR/VALIDATION_AUDITS"
mkdir -p "$ROOT_DIR/VALIDATION_AUDITS/access_logs"

echo "       ✓ All directories created"
echo ""

# Set permissions (Phase 3: Permission Enforcement)
echo "[3/3] Setting permissions for PHI isolation..."

# Root directory: Logan only (700 = rwx------)
chmod 700 "$ROOT_DIR"
echo "       ✓ $ROOT_DIR/ → chmod 700 (Logan only)"

# 00_RAW_PHI: Read-only after initial load (500 = r-x------)
chmod 500 "$ROOT_DIR/00_RAW_PHI"
chmod 500 "$ROOT_DIR/00_RAW_PHI/source_extracts"
echo "       ✓ 00_RAW_PHI/ → chmod 500 (read-only)"

# 01_SILVER_DEID_PARQUET: Read-write for refresh, DVC tracking (755 = rwxr-xr-x)
chmod 755 "$ROOT_DIR/01_SILVER_DEID_PARQUET"
chmod 755 "$ROOT_DIR/01_SILVER_DEID_PARQUET/validation_tables"
echo "       ✓ 01_SILVER_DEID_PARQUET/ → chmod 755 (read-write for refresh)"

# 02_GOLD_POWERBI: Read-write for .pbix, templates, queries (755)
chmod 755 "$ROOT_DIR/02_GOLD_POWERBI"
chmod 755 "$ROOT_DIR/02_GOLD_POWERBI/templates"
chmod 755 "$ROOT_DIR/02_GOLD_POWERBI/queries"
echo "       ✓ 02_GOLD_POWERBI/ → chmod 755 (Power BI working dir)"

# 03_DEID_EXPORTS: Read-write for exports (755)
chmod 755 "$ROOT_DIR/03_DEID_EXPORTS"
echo "       ✓ 03_DEID_EXPORTS/ → chmod 755 (de-identified exports)"

# 04_EXTRACTION_OUTPUTS: Read-write for NLP results (755)
chmod 755 "$ROOT_DIR/04_EXTRACTION_OUTPUTS"
chmod 755 "$ROOT_DIR/04_EXTRACTION_OUTPUTS/nlm_cell_extractions"
chmod 755 "$ROOT_DIR/04_EXTRACTION_OUTPUTS/data_quality_issues"
chmod 755 "$ROOT_DIR/04_EXTRACTION_OUTPUTS/reconciliation_logs"
echo "       ✓ 04_EXTRACTION_OUTPUTS/ → chmod 755 (extraction outputs)"

# 05_ARCHIVE_BACKUPS: Read-write for weekly snapshots (755)
chmod 755 "$ROOT_DIR/05_ARCHIVE_BACKUPS"
echo "       ✓ 05_ARCHIVE_BACKUPS/ → chmod 755 (encrypted backups)"

# SCRIPTS: Executable for Python scripts (755)
chmod 755 "$ROOT_DIR/SCRIPTS"
echo "       ✓ SCRIPTS/ → chmod 755 (Python ETL scripts)"

# DOCUMENTATION: Read-write for docs (755)
chmod 755 "$ROOT_DIR/DOCUMENTATION"
echo "       ✓ DOCUMENTATION/ → chmod 755 (SOPs, data dictionary)"

# VALIDATION_AUDITS: Read-write for audit logs (755)
chmod 755 "$ROOT_DIR/VALIDATION_AUDITS"
chmod 755 "$ROOT_DIR/VALIDATION_AUDITS/access_logs"
echo "       ✓ VALIDATION_AUDITS/ → chmod 755 (audit trail, compliance)"

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Folder Structure:"
ls -la "$ROOT_DIR" | awk '{print "  " $0}'
echo ""
echo "Next Steps:"
echo "  1. Copy original Excel files (PHI) to 00_RAW_PHI/source_extracts/"
echo "  2. Run de-identification script: python3 SCRIPTS/00_deid_gateway.py"
echo "  3. Verify no PHI leakage in 01_SILVER_DEID_PARQUET/"
echo "  4. Open Power BI Desktop; import Parquet files"
echo "  5. Set up Power Automate Desktop robot for weekly refresh"
echo ""
echo "Security Reminders:"
echo "  - 00_RAW_PHI/ is now READ-ONLY (chmod 500)"
echo "  - To modify 00_RAW_PHI/ (e.g., add new data), run: chmod 755 $ROOT_DIR/00_RAW_PHI"
echo "  - After adding data, restore read-only: chmod 500 $ROOT_DIR/00_RAW_PHI"
echo "  - Verify FileVault is enabled: System Preferences → Security & Privacy → FileVault"
echo "  - Never commit 00_RAW_PHI/ or .pbix files to Git (use .gitignore)"
echo ""
echo "Questions? See: DOCUMENTATION/README_THYROID_SECURE.md"
echo ""
