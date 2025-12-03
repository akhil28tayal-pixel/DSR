#!/bin/bash
# Sync database from AWS server to local machine
# Usage: ./sync_db_from_aws.sh [AWS_HOST] [SSH_KEY_PATH]

# Configuration - Update these values
AWS_HOST="${1:-your-aws-host.amazonaws.com}"  # e.g., ec2-xx-xx-xx-xx.compute-1.amazonaws.com or IP address
AWS_USER="ec2-user"
SSH_KEY="${2:-~/.ssh/your-key.pem}"  # Path to your SSH key

# Remote and local paths
REMOTE_DB_PATH="/var/www/dsr/webapp_sales_collections.db"
LOCAL_DIR="/Users/akhiltayal/CascadeProjects/DSR"
LOCAL_DB_PATH="${LOCAL_DIR}/webapp_sales_collections.db"
BACKUP_DIR="${LOCAL_DIR}/db_backups"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}  AWS Database Sync Script${NC}"
echo -e "${YELLOW}========================================${NC}"

# Check if AWS host is provided
if [ "$AWS_HOST" == "your-aws-host.amazonaws.com" ]; then
    echo -e "${RED}Error: Please provide AWS host as first argument${NC}"
    echo "Usage: ./sync_db_from_aws.sh <AWS_HOST> [SSH_KEY_PATH]"
    echo "Example: ./sync_db_from_aws.sh ec2-12-34-56-78.compute-1.amazonaws.com ~/.ssh/my-key.pem"
    exit 1
fi

# Check if SSH key exists
if [ ! -f "${SSH_KEY/#\~/$HOME}" ]; then
    echo -e "${RED}Error: SSH key not found at ${SSH_KEY}${NC}"
    echo "Please provide the correct path to your SSH key as second argument"
    exit 1
fi

# Create backup directory if it doesn't exist
mkdir -p "$BACKUP_DIR"

# Backup existing local database if it exists
if [ -f "$LOCAL_DB_PATH" ]; then
    BACKUP_NAME="webapp_sales_collections_$(date +%Y%m%d_%H%M%S).db"
    echo -e "${YELLOW}Backing up existing local database...${NC}"
    cp "$LOCAL_DB_PATH" "${BACKUP_DIR}/${BACKUP_NAME}"
    echo -e "${GREEN}Backup saved to: ${BACKUP_DIR}/${BACKUP_NAME}${NC}"
fi

# Fetch database from AWS
echo -e "${YELLOW}Fetching database from AWS server...${NC}"
echo "Host: $AWS_HOST"
echo "Remote path: $REMOTE_DB_PATH"

scp -i "${SSH_KEY/#\~/$HOME}" "${AWS_USER}@${AWS_HOST}:${REMOTE_DB_PATH}" "$LOCAL_DB_PATH"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}  Database synced successfully!${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo "Local database: $LOCAL_DB_PATH"
    echo "Size: $(ls -lh "$LOCAL_DB_PATH" | awk '{print $5}')"
    
    # Show database info
    echo ""
    echo -e "${YELLOW}Database Statistics:${NC}"
    sqlite3 "$LOCAL_DB_PATH" "SELECT 'Sales records: ' || COUNT(*) FROM sales_data;" 2>/dev/null
    sqlite3 "$LOCAL_DB_PATH" "SELECT 'Collection records: ' || COUNT(*) FROM collections_data;" 2>/dev/null
    sqlite3 "$LOCAL_DB_PATH" "SELECT 'Opening balances: ' || COUNT(*) FROM opening_balances;" 2>/dev/null
else
    echo -e "${RED}========================================${NC}"
    echo -e "${RED}  Error: Failed to sync database${NC}"
    echo -e "${RED}========================================${NC}"
    echo "Please check:"
    echo "1. AWS host is correct"
    echo "2. SSH key path is correct"
    echo "3. You have network access to the server"
    exit 1
fi
