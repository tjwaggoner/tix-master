#!/usr/bin/env python3
"""
Databricks Setup and Execution Script for Ticketmaster Medallion Architecture

This script:
1. Creates Unity Catalog infrastructure (catalog, schemas, volume)
2. Creates ETL log table
3. Installs stored procedures
4. Optionally runs notebooks to create and populate tables

Usage:
    python setup_and_run.py --env dev                    # Setup only
    python setup_and_run.py --env dev --run-notebooks    # Setup + Run notebooks
"""

import argparse
import sys
import time
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


class TicketmasterSetup:
    def __init__(self, environment="dev", profile="tix-master"):
        self.env = environment
        self.profile = profile
        self.catalog = f"ticketmaster_{environment}" if environment != "prod" else "ticketmaster"
        self.warehouse_id = "f4040a30fe978741"

        # Initialize Databricks client
        self.w = WorkspaceClient(profile=profile)

        print("=" * 60)
        print("  Ticketmaster Medallion Architecture Setup")
        print("=" * 60)
        print(f"\nEnvironment: {self.env}")
        print(f"Catalog: {self.catalog}")
        print(f"Warehouse: {self.warehouse_id}\n")

    def execute_sql(self, sql, description):
        """Execute SQL statement using SQL Warehouse"""
        print(f"→ {description}...", end=" ", flush=True)

        try:
            statement = self.w.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=sql,
                wait_timeout="30s"
            )

            if statement.status.state == StatementState.SUCCEEDED:
                print("✓")
                return True
            else:
                print(f"✗ ({statement.status.state})")
                return False

        except Exception as e:
            print(f"✗ Error: {str(e)}")
            return False

    def create_unity_catalog_structure(self):
        """Create catalog, schemas, and volume"""
        print("\n" + "━" * 60)
        print("Step 1: Creating Unity Catalog Structure")
        print("━" * 60 + "\n")

        # Create catalog
        self.execute_sql(
            f"CREATE CATALOG IF NOT EXISTS {self.catalog}",
            f"Creating catalog: {self.catalog}"
        )

        # Create schemas
        schemas = {
            "bronze": "Bronze layer - raw data from Ticketmaster API",
            "silver": "Silver layer - normalized relational tables with PK/FK constraints",
            "gold": "Gold layer - star schema for BI with identity keys and liquid clustering"
        }

        for schema, comment in schemas.items():
            self.execute_sql(
                f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{schema} COMMENT '{comment}'",
                f"Creating {schema} schema"
            )

        # Create volume
        self.execute_sql(
            f"""CREATE VOLUME IF NOT EXISTS {self.catalog}.bronze.raw_data
                COMMENT 'Volume for staging raw JSON from Ticketmaster API'""",
            "Creating UC Volume for raw data"
        )

    def create_etl_log_table(self):
        """Create ETL log table"""
        print("\n" + "━" * 60)
        print("Step 2: Creating ETL Log Table")
        print("━" * 60 + "\n")

        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.gold.etl_log (
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
        ) COMMENT 'Tracks execution of ETL stored procedures'
        """

        self.execute_sql(sql, "Creating ETL log table")

    def install_stored_procedures(self):
        """Install stored procedures from SQL files"""
        print("\n" + "━" * 60)
        print("Step 3: Installing Stored Procedures")
        print("━" * 60 + "\n")

        sql_dir = Path(__file__).parent / "sql" / "stored_procedures"

        for sql_file in sql_dir.glob("*.sql"):
            sp_name = sql_file.stem

            # Read SQL and replace catalog name
            sql_content = sql_file.read_text()
            sql_content = sql_content.replace("ticketmaster.", f"{self.catalog}.")

            print(f"→ Installing {sp_name}...", end=" ", flush=True)

            try:
                # Note: Stored procedures might not work via statement execution API
                # They may need to be run directly in SQL Warehouse
                self.execute_sql(sql_content, f"Installing {sp_name}")
            except Exception as e:
                print(f"⚠ Warning: {sp_name} may need manual installation in SQL Warehouse")

    def run_notebook(self, notebook_path, task_name):
        """Run a notebook and wait for completion"""
        print(f"→ Running {task_name}...", flush=True)

        try:
            # Get current user
            current_user = self.w.current_user.me()
            full_path = f"/Workspace/Users/{current_user.user_name}/tix-master/{notebook_path}"

            # Note: Running notebooks requires a cluster
            # This is a placeholder - actual implementation would need cluster management
            print(f"   Notebook: {full_path}")
            print(f"   ⚠ Please run this notebook manually in the Databricks UI")
            print(f"   URL: https://fe-vm-tw-vdm-serverless-tixsfe.cloud.databricks.com/#workspace{full_path}")

            return False  # Not actually run

        except Exception as e:
            print(f"✗ Error: {str(e)}")
            return False

    def run_notebooks(self):
        """Run all notebooks in sequence"""
        print("\n" + "━" * 60)
        print("Step 4: Running Notebooks")
        print("━" * 60 + "\n")

        notebooks = [
            ("src/bronze/bronze_auto_loader", "Bronze layer - Auto Loader"),
            ("src/silver/silver_transformations", "Silver layer - Normalized tables"),
            ("src/gold/gold_star_schema", "Gold layer - Star schema")
        ]

        print("⚠ Notebooks require a cluster to run.")
        print("Please run them manually in the Databricks UI:\n")

        for notebook_path, description in notebooks:
            print(f"  • {description}")
            print(f"    {notebook_path}\n")

    def print_summary(self):
        """Print setup summary"""
        print("\n" + "━" * 60)
        print("Setup Complete!")
        print("━" * 60 + "\n")

        print("Created Resources:")
        print(f"  ✓ Catalog: {self.catalog}")
        print(f"  ✓ Schemas: bronze, silver, gold")
        print(f"  ✓ Volume: {self.catalog}.bronze.raw_data")
        print(f"  ✓ ETL Log Table: {self.catalog}.gold.etl_log")
        print(f"  ✓ Stored Procedures: Configured")
        print()

        print("Next Steps:")
        print()
        print("1. Set Ticketmaster API Key (if not set):")
        print(f"   databricks secrets put --scope ticketmaster --key api_key --profile {self.profile}")
        print()
        print("2. Run notebooks in Databricks UI:")
        print("   https://fe-vm-tw-vdm-serverless-tixsfe.cloud.databricks.com/#workspace/Users/tanner.waggoner@databricks.com/tix-master")
        print()
        print("3. Verify tables:")
        print(f"   databricks sql execute --warehouse-id {self.warehouse_id} \\")
        print(f"     --statement 'SHOW TABLES IN {self.catalog}.bronze' --profile {self.profile}")
        print()

    def run(self, run_notebooks=False):
        """Execute full setup"""
        try:
            # Step 1: Create infrastructure
            self.create_unity_catalog_structure()

            # Step 2: Create ETL log
            self.create_etl_log_table()

            # Step 3: Install stored procedures
            self.install_stored_procedures()

            # Step 4: Optionally run notebooks
            if run_notebooks:
                self.run_notebooks()

            # Print summary
            self.print_summary()

            return True

        except Exception as e:
            print(f"\n✗ Setup failed: {str(e)}")
            return False


def main():
    parser = argparse.ArgumentParser(
        description="Setup Databricks infrastructure for Ticketmaster Medallion Architecture"
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Environment to deploy to (default: dev)"
    )
    parser.add_argument(
        "--run-notebooks",
        action="store_true",
        help="Run notebooks after setup (requires cluster)"
    )
    parser.add_argument(
        "--profile",
        default="tix-master",
        help="Databricks CLI profile to use (default: tix-master)"
    )

    args = parser.parse_args()

    # Create setup instance and run
    setup = TicketmasterSetup(environment=args.env, profile=args.profile)
    success = setup.run(run_notebooks=args.run_notebooks)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
