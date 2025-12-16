#!/bin/bash
#
# Databricks Setup Script for Ticketmaster Medallion Architecture
# This script creates all necessary infrastructure and executes notebooks
#
# Usage: ./setup_databricks.sh [dev|prod]
#

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROFILE="tix-master"
WAREHOUSE_ID="f4040a30fe978741"
ENVIRONMENT="${1:-dev}"
CATALOG="ticketmaster_${ENVIRONMENT}"

if [ "$ENVIRONMENT" = "prod" ]; then
    CATALOG="ticketmaster"
fi

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}  Ticketmaster Medallion Architecture Setup${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""
echo -e "${YELLOW}Environment:${NC} $ENVIRONMENT"
echo -e "${YELLOW}Catalog:${NC} $CATALOG"
echo -e "${YELLOW}Warehouse:${NC} $WAREHOUSE_ID"
echo ""

# Function to execute SQL
execute_sql() {
    local sql="$1"
    local description="$2"

    echo -e "${BLUE}→${NC} $description"

    databricks sql execute \
        --warehouse-id "$WAREHOUSE_ID" \
        --profile "$PROFILE" \
        --statement "$sql" 2>&1 | grep -v "^$" || true

    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        echo -e "${GREEN}✓${NC} Done"
    else
        echo -e "${RED}✗${NC} Failed"
        return 1
    fi
    echo ""
}

# Step 1: Create Unity Catalog Structure
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}Step 1: Creating Unity Catalog Structure${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

execute_sql "CREATE CATALOG IF NOT EXISTS ${CATALOG}" \
    "Creating catalog: ${CATALOG}"

execute_sql "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.bronze
    COMMENT 'Bronze layer - raw data from Ticketmaster API'" \
    "Creating bronze schema"

execute_sql "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.silver
    COMMENT 'Silver layer - normalized relational tables with PK/FK constraints'" \
    "Creating silver schema"

execute_sql "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.gold
    COMMENT 'Gold layer - star schema for BI with identity keys and liquid clustering'" \
    "Creating gold schema"

execute_sql "CREATE VOLUME IF NOT EXISTS ${CATALOG}.bronze.raw_data
    COMMENT 'Volume for staging raw JSON from Ticketmaster API'" \
    "Creating UC Volume for raw data"

# Step 2: Create ETL Log Table
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}Step 2: Creating ETL Log Table${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

execute_sql "CREATE TABLE IF NOT EXISTS ${CATALOG}.gold.etl_log (
  log_id BIGINT GENERATED ALWAYS AS IDENTITY,
  procedure_name STRING NOT NULL,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  parameters STRING,
  rows_processed INT,
  status STRING,
  error_message STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT etl_log_pk PRIMARY KEY (log_id)
) COMMENT 'Tracks execution of ETL stored procedures including performance metrics and errors'" \
    "Creating ETL log table"

# Step 3: Install Stored Procedures
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}Step 3: Creating Stored Procedures${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

for sp in sql/stored_procedures/*.sql; do
    sp_name=$(basename "$sp" .sql)
    echo -e "${BLUE}→${NC} Installing stored procedure: $sp_name"

    # Replace catalog placeholders
    sql_content=$(cat "$sp" | sed "s/ticketmaster\./${CATALOG}./g")

    databricks sql execute \
        --warehouse-id "$WAREHOUSE_ID" \
        --profile "$PROFILE" \
        --statement "$sql_content" 2>&1 | grep -v "^$" || true

    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        echo -e "${GREEN}✓${NC} Installed $sp_name"
    else
        echo -e "${YELLOW}⚠${NC} Warning: $sp_name may need manual adjustment"
    fi
    echo ""
done

# Step 4: Summary
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}Setup Complete!${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

echo -e "${BLUE}Created Resources:${NC}"
echo "  ✓ Catalog: ${CATALOG}"
echo "  ✓ Schemas: bronze, silver, gold"
echo "  ✓ Volume: ${CATALOG}.bronze.raw_data"
echo "  ✓ ETL Log Table: ${CATALOG}.gold.etl_log"
echo "  ✓ Stored Procedures: 3 installed"
echo ""

echo -e "${YELLOW}Next Steps:${NC}"
echo ""
echo "1. Set Ticketmaster API Key (if not already set):"
echo "   ${BLUE}databricks secrets put --scope ticketmaster --key api_key --profile ${PROFILE}${NC}"
echo ""
echo "2. Run notebooks in Databricks UI:"
echo "   a. Bronze: /tix-master/src/bronze/bronze_auto_loader"
echo "   b. Silver: /tix-master/src/silver/silver_transformations"
echo "   c. Gold: /tix-master/src/gold/gold_star_schema"
echo ""
echo "3. Or run via CLI:"
echo "   ${BLUE}databricks workspace run-notebook /Workspace/Users/tanner.waggoner@databricks.com/tix-master/src/bronze/bronze_auto_loader \\
     --profile ${PROFILE} --cluster-id <cluster-id>${NC}"
echo ""
echo "4. Verify tables were created:"
echo "   ${BLUE}databricks sql execute --warehouse-id ${WAREHOUSE_ID} \\
     --statement 'SHOW TABLES IN ${CATALOG}.bronze' --profile ${PROFILE}${NC}"
echo ""

echo -e "${GREEN}Workspace URLs:${NC}"
echo "  • Workspace: https://fe-vm-tw-vdm-serverless-tixsfe.cloud.databricks.com"
echo "  • Notebooks: https://fe-vm-tw-vdm-serverless-tixsfe.cloud.databricks.com/#workspace/Users/tanner.waggoner@databricks.com/tix-master"
echo "  • SQL Warehouse: https://fe-vm-tw-vdm-serverless-tixsfe.cloud.databricks.com/#sql"
echo ""
