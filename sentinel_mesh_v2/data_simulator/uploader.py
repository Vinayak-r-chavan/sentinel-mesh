"""
===============================================================================
SENTINEL MESH V2 -- Dimension Data Uploader
===============================================================================

Uploads generated dimension data (customers, accounts, merchants, devices, IPs)
to Microsoft Fabric Lakehouse and/or Eventhouse.

Supports multiple upload methods:
    1. CSV file generation (for manual upload to Fabric portal)
    2. Direct Eventhouse ingestion via KQL streaming
    3. Lakehouse SQL endpoint (via pyodbc/JDBC) -- future

Usage:
    # Generate CSVs ready for Fabric upload
    python -m data_simulator.uploader --method csv

    # Stream dimensions directly to Eventhouse tables
    python -m data_simulator.uploader --method eventhouse

    # Generate + Upload in one step
    python -m data_simulator.uploader --method csv --generate
===============================================================================
"""

import os
import sys
import json
import csv
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.config_loader import get_config, get_secret
from data_simulator.dimension_generator import DimensionGenerator


class DimensionUploader:
    """
    Uploads dimension data to Fabric Lakehouse / Eventhouse.
    """

    def __init__(self, dimension_generator: DimensionGenerator = None,
                 config: dict = None):
        """
        Initialize uploader.

        Args:
            dimension_generator: DimensionGenerator with populated pools.
                                If None, creates and generates a new one.
            config: Full config dict. If None, loads from config.yaml.
        """
        self.config = config or get_config()

        if dimension_generator is None:
            self.dim_gen = DimensionGenerator(config=self.config)
            self.dim_gen.generate_all()
        else:
            self.dim_gen = dimension_generator

    # -------------------------------------------------------------------------
    # METHOD 1: CSV Export (for Fabric Portal upload)
    # -------------------------------------------------------------------------

    def export_csv_for_lakehouse(self, output_dir: str = None) -> dict:
        """
        Export dimension tables as CSVs formatted for Fabric Lakehouse upload.
        These CSVs can be uploaded via:
          - Fabric Portal -> Lakehouse -> Upload files
          - OneLake file explorer
          - Azure Storage Explorer

        Args:
            output_dir: Output directory. Default: data_simulator/upload_ready/

        Returns:
            Dict mapping table name to file path.
        """
        if output_dir is None:
            output_dir = str(Path(__file__).parent / "upload_ready")
        os.makedirs(output_dir, exist_ok=True)

        files = {}

        # dim_customer
        files["dim_customer"] = self._write_csv(
            output_dir, "dim_customer.csv",
            self.dim_gen.customers,
            columns=["customer_id", "name", "risk_score", "kyc_status",
                      "pep_flag", "country_code", "city", "created_date"]
        )

        # dim_account
        files["dim_account"] = self._write_csv(
            output_dir, "dim_account.csv",
            self.dim_gen.accounts,
            columns=["account_id", "customer_id", "account_type",
                      "balance", "velocity_index", "opened_date", "dormancy_score"]
        )

        # dim_merchant
        files["dim_merchant"] = self._write_csv(
            output_dir, "dim_merchant.csv",
            self.dim_gen.merchants,
            columns=["merchant_id", "merchant_name", "mcc_code",
                      "category", "risk_tier", "shell_score", "registered_date"]
        )

        # dim_device (flatten customer_ids list to JSON string for CSV)
        device_rows = []
        for d in self.dim_gen.devices:
            row = dict(d)
            row["customer_ids"] = json.dumps(row["customer_ids"])
            device_rows.append(row)
        files["dim_device"] = self._write_csv(
            output_dir, "dim_device.csv",
            device_rows,
            columns=["device_id", "device_type", "first_seen",
                      "last_seen", "customer_ids"]
        )

        # dim_ip_address (flatten customer_ids list)
        ip_rows = []
        for ip in self.dim_gen.ip_addresses:
            row = dict(ip)
            row["customer_ids"] = json.dumps(row["customer_ids"])
            ip_rows.append(row)
        files["dim_ip_address"] = self._write_csv(
            output_dir, "dim_ip_address.csv",
            ip_rows,
            columns=["ip_address", "geo_location", "is_vpn",
                      "is_tor", "customer_ids"]
        )

        print(f"\n[UPLOADER] CSV files ready for Fabric upload in: {output_dir}")
        return files

    def _write_csv(self, output_dir: str, filename: str,
                   data: list[dict], columns: list[str]) -> str:
        """Write a list of dicts to CSV with specified columns."""
        filepath = os.path.join(output_dir, filename)
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(data)
        print(f"[UPLOADER] Wrote {filename}: {len(data)} rows")
        return filepath

    # -------------------------------------------------------------------------
    # METHOD 2: KQL Inline Ingestion (for Eventhouse dimension tables)
    # -------------------------------------------------------------------------

    def generate_kql_ingestion(self, output_dir: str = None) -> dict:
        """
        Generate KQL .ingest inline commands for each dimension table.
        These can be run directly in the Eventhouse KQL query editor.

        Args:
            output_dir: Output directory. Default: data_simulator/upload_ready/

        Returns:
            Dict mapping table name to KQL file path.
        """
        if output_dir is None:
            output_dir = str(Path(__file__).parent / "upload_ready")
        os.makedirs(output_dir, exist_ok=True)

        files = {}

        # dim_customer KQL
        files["dim_customer"] = self._write_kql_ingest(
            output_dir, "ingest_dim_customer.kql",
            "dim_customer",
            self.dim_gen.customers,
            columns=["customer_id", "name", "risk_score", "kyc_status",
                      "pep_flag", "country_code", "city", "created_date"],
            formatters={"pep_flag": lambda v: str(v).lower()}
        )

        # dim_account KQL
        files["dim_account"] = self._write_kql_ingest(
            output_dir, "ingest_dim_account.kql",
            "dim_account",
            self.dim_gen.accounts,
            columns=["account_id", "customer_id", "account_type",
                      "balance", "velocity_index", "opened_date", "dormancy_score"]
        )

        # dim_merchant KQL
        files["dim_merchant"] = self._write_kql_ingest(
            output_dir, "ingest_dim_merchant.kql",
            "dim_merchant",
            self.dim_gen.merchants,
            columns=["merchant_id", "merchant_name", "mcc_code",
                      "category", "risk_tier", "shell_score", "registered_date"]
        )

        # dim_device KQL
        device_rows = []
        for d in self.dim_gen.devices:
            row = dict(d)
            row["customer_ids"] = json.dumps(row["customer_ids"])
            device_rows.append(row)
        files["dim_device"] = self._write_kql_ingest(
            output_dir, "ingest_dim_device.kql",
            "dim_device",
            device_rows,
            columns=["device_id", "device_type", "first_seen",
                      "last_seen", "customer_ids"]
        )

        # dim_ip_address KQL
        ip_rows = []
        for ip in self.dim_gen.ip_addresses:
            row = dict(ip)
            row["customer_ids"] = json.dumps(row["customer_ids"])
            ip_rows.append(row)
        files["dim_ip_address"] = self._write_kql_ingest(
            output_dir, "ingest_dim_ip_address.kql",
            "dim_ip_address",
            ip_rows,
            columns=["ip_address", "geo_location", "is_vpn",
                      "is_tor", "customer_ids"]
        )

        print(f"\n[UPLOADER] KQL ingestion files ready in: {output_dir}")
        return files

    def _write_kql_ingest(self, output_dir: str, filename: str,
                          table_name: str, data: list[dict],
                          columns: list[str],
                          formatters: dict = None) -> str:
        """Generate a .kql file with .ingest inline commands."""
        filepath = os.path.join(output_dir, filename)
        formatters = formatters or {}

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(f"// Auto-generated KQL ingestion for {table_name}\n")
            f.write(f"// Generated at: {datetime.now(timezone.utc).isoformat()}\n")
            f.write(f"// Rows: {len(data)}\n\n")

            # Clear existing data first
            f.write(f".clear table {table_name} data\n\n")

            # Write inline ingestion
            f.write(f".ingest inline into table {table_name} <|\n")

            for row in data:
                values = []
                for col in columns:
                    val = row.get(col, "")
                    if col in formatters:
                        val = formatters[col](val)
                    if val is None:
                        val = ""
                    values.append(str(val))
                f.write(",".join(values) + "\n")

            f.write("\n")
            f.write(f"// Verify: {table_name} | count\n")

        print(f"[UPLOADER] Wrote {filename}: {len(data)} rows -> {table_name}")
        return filepath

    # -------------------------------------------------------------------------
    # METHOD 3: Eventstream JSON mapping helper
    # -------------------------------------------------------------------------

    def generate_eventstream_mapping(self, output_dir: str = None) -> str:
        """
        Generate a JSON mapping file for Eventstream -> fact_transactions.
        This helps configure the Eventstream destination in Fabric.

        Returns:
            Path to the generated mapping file.
        """
        if output_dir is None:
            output_dir = str(Path(__file__).parent / "upload_ready")
        os.makedirs(output_dir, exist_ok=True)

        # V2 schema columns in order
        columns = [
            {"column": "transaction_id", "path": "$.transaction_id", "datatype": "string"},
            {"column": "customer_id", "path": "$.customer_id", "datatype": "string"},
            {"column": "customer_name", "path": "$.customer_name", "datatype": "string"},
            {"column": "account_id", "path": "$.account_id", "datatype": "string"},
            {"column": "amount", "path": "$.amount", "datatype": "real"},
            {"column": "timestamp", "path": "$.timestamp", "datatype": "datetime"},
            {"column": "channel", "path": "$.channel", "datatype": "string"},
            {"column": "device_id", "path": "$.device_id", "datatype": "string"},
            {"column": "ip_address", "path": "$.ip_address", "datatype": "string"},
            {"column": "geo_location", "path": "$.geo_location", "datatype": "string"},
            {"column": "merchant_id", "path": "$.merchant_id", "datatype": "string"},
            {"column": "mcc_code", "path": "$.mcc_code", "datatype": "string"},
            {"column": "counterparty_account", "path": "$.counterparty_account", "datatype": "string"},
            {"column": "transaction_type", "path": "$.transaction_type", "datatype": "string"},
            {"column": "ingestion_timestamp", "path": "$.ingestion_timestamp", "datatype": "datetime"},
            {"column": "processing_status", "path": "$.processing_status", "datatype": "string"},
            {"column": "risk_flag", "path": "$.risk_flag", "datatype": "string"},
            {"column": "pattern_hash", "path": "$.pattern_hash", "datatype": "string"},
            {"column": "scenario_id", "path": "$.scenario_id", "datatype": "string"},
        ]

        mapping = {
            "mapping_name": "fact_transactions_v2_mapping",
            "mapping_kind": "Json",
            "columns": columns,
        }

        # Also generate the KQL mapping command
        kql_mapping_entries = []
        for col in columns:
            kql_mapping_entries.append(
                f'{{"column": "{col["column"]}", '
                f'"Properties": {{"Path": "{col["path"]}"}}}}'
            )

        kql_command = (
            f".create-or-alter table fact_transactions ingestion json mapping "
            f"'fact_transactions_v2_mapping' '[{', '.join(kql_mapping_entries)}]'"
        )

        filepath = os.path.join(output_dir, "eventstream_mapping.json")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(mapping, f, indent=2)

        kql_filepath = os.path.join(output_dir, "create_mapping.kql")
        with open(kql_filepath, "w", encoding="utf-8") as f:
            f.write("// JSON ingestion mapping for fact_transactions V2 schema\n")
            f.write(f"// Generated at: {datetime.now(timezone.utc).isoformat()}\n\n")
            f.write(kql_command + "\n")

        print(f"[UPLOADER] Wrote eventstream_mapping.json")
        print(f"[UPLOADER] Wrote create_mapping.kql")
        return filepath


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import argparse
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

    parser = argparse.ArgumentParser(
        description="SENTINEL MESH V2 -- Dimension Data Uploader"
    )
    parser.add_argument(
        "--method", choices=["csv", "kql", "both"],
        default="both",
        help="Upload method: csv (for portal upload), kql (inline ingestion), both"
    )
    parser.add_argument(
        "--output-dir", default=None,
        help="Output directory (default: data_simulator/upload_ready/)"
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="Random seed for reproducible generation"
    )
    parser.add_argument(
        "--mapping", action="store_true",
        help="Also generate Eventstream JSON mapping"
    )

    args = parser.parse_args()

    print("=" * 70)
    print("SENTINEL MESH V2 -- Dimension Uploader")
    print("=" * 70)
    print()

    # Generate dimensions
    dim_gen = DimensionGenerator(seed=args.seed)
    dim_gen.generate_all()
    print()

    # Create uploader
    uploader = DimensionUploader(dimension_generator=dim_gen)

    if args.method in ("csv", "both"):
        print("-- CSV Export (for Fabric Portal upload) --")
        csv_files = uploader.export_csv_for_lakehouse(args.output_dir)
        print()

    if args.method in ("kql", "both"):
        print("-- KQL Ingestion Scripts (for Eventhouse) --")
        kql_files = uploader.generate_kql_ingestion(args.output_dir)
        print()

    if args.mapping or True:  # Always generate mapping
        print("-- Eventstream Mapping --")
        uploader.generate_eventstream_mapping(args.output_dir)
        print()

    print("=" * 70)
    print("[OK] All upload files generated!")
    print()
    print("NEXT STEPS:")
    print("  1. Open Fabric Eventhouse -> Run kql/L2_eventhouse_ddl.kql")
    print("  2. Run the ingest_dim_*.kql files to populate dimension tables")
    print("  3. Upload CSVs to Lakehouse (portal or OneLake explorer)")
    print("  4. Configure Eventstream mapping (if needed)")
    print("  5. Run simulator: python -m data_simulator.simulator --mode stream")
    print("=" * 70)
