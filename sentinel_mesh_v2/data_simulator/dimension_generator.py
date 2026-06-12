"""
===============================================================================
SENTINEL MESH V2 -- Dynamic Dimension Generator
===============================================================================

Generates realistic dimension data (customers, accounts, merchants, devices,
IP addresses) using Faker and config-driven pool sizes.

ZERO hard-coded entity data. Everything is generated dynamically from
config/config.yaml parameters.

Usage:
    python -m data_simulator.dimension_generator [--output-dir ./output] [--format csv]

    Or import in code:
        from data_simulator.dimension_generator import DimensionGenerator
        gen = DimensionGenerator()
        customers = gen.generate_customers()
===============================================================================
"""

import os
import sys
import csv
import json
import random
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add project root to path for config imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from faker import Faker
from config.config_loader import get_config, get_param


class DimensionGenerator:
    """
    Generates all dimension tables from config-driven parameters.
    No hard-coded entity names, IDs, or values anywhere.
    """

    def __init__(self, config: dict = None, seed: int = None):
        """
        Initialize the generator with config and optional random seed.

        Args:
            config: Full config dict. If None, loads from config.yaml.
            seed: Random seed for reproducible generation. None = random.
        """
        self.config = config or get_config()
        self.locale = self.config["simulator"]["locale"]
        self.fake = Faker(self.locale)

        if seed is not None:
            Faker.seed(seed)
            random.seed(seed)

        # Load dimension configs
        self.dim_config = self.config["dimensions"]
        self.sim_config = self.config["simulator"]

        # Prefixes for ID generation
        self.prefixes = self.sim_config["id_prefixes"]

        # Generated pools (populated on generate)
        self.customers = []
        self.accounts = []
        self.merchants = []
        self.devices = []
        self.ip_addresses = []

    # -------------------------------------------------------------------------
    # ID GENERATION -- Unique, deterministic, never hand-typed
    # -------------------------------------------------------------------------

    def _generate_id(self, prefix: str, index: int) -> str:
        """Generate a unique ID with prefix and zero-padded index."""
        return f"{prefix}-{index:04d}"

    def _generate_hash_id(self, prefix: str, *parts) -> str:
        """Generate a hash-based ID from parts for uniqueness."""
        raw = "-".join(str(p) for p in parts)
        short_hash = hashlib.md5(raw.encode()).hexdigest()[:8].upper()
        return f"{prefix}-{short_hash}"

    # -------------------------------------------------------------------------
    # CUSTOMER GENERATION
    # -------------------------------------------------------------------------

    def generate_customers(self) -> list[dict]:
        """
        Generate a pool of realistic customers from config parameters.

        Returns:
            List of customer dicts with: customer_id, name, risk_score,
            kyc_status, pep_flag, country_code, city, created_date
        """
        pool_size = self.dim_config["customer"]["pool_size"]
        kyc_statuses = self.dim_config["customer"]["kyc_statuses"]
        kyc_weights = self.dim_config["customer"]["kyc_status_weights"]
        pep_prob = self.dim_config["customer"]["pep_probability"]
        country_codes = self.dim_config["customer"]["country_codes"]
        risk_range = self.dim_config["customer"]["risk_score_range"]
        cities = self.sim_config["cities"]

        customers = []
        for i in range(1, pool_size + 1):
            customer_id = self._generate_id(self.prefixes["customer"], i)
            name = self.fake.name()
            city = random.choice(cities)

            customers.append({
                "customer_id": customer_id,
                "name": name,
                "risk_score": round(random.uniform(risk_range["min"], risk_range["max"]), 4),
                "kyc_status": random.choices(kyc_statuses, weights=kyc_weights, k=1)[0],
                "pep_flag": random.random() < pep_prob,
                "country_code": random.choice(country_codes),
                "city": city,
                "created_date": self.fake.date_between(
                    start_date="-3y", end_date="today"
                ).isoformat(),
            })

        self.customers = customers
        print(f"[DIM-GEN] Generated {len(customers)} customers")
        return customers

    # -------------------------------------------------------------------------
    # ACCOUNT GENERATION
    # -------------------------------------------------------------------------

    def generate_accounts(self) -> list[dict]:
        """
        Generate accounts linked to existing customers.
        Some customers get multiple accounts (realistic).

        Returns:
            List of account dicts with: account_id, customer_id, account_type,
            balance, velocity_index, opened_date, dormancy_score
        """
        if not self.customers:
            self.generate_customers()

        pool_size = self.dim_config["account"]["pool_size"]
        acc_types = self.dim_config["account"]["types"]
        type_weights = self.dim_config["account"]["type_weights"]
        balance_range = self.dim_config["account"]["balance_range"]

        accounts = []
        customer_ids = [c["customer_id"] for c in self.customers]

        for i in range(1, pool_size + 1):
            # Distribute accounts across customers (some get multiple)
            customer_id = random.choice(customer_ids)
            acc_type = random.choices(acc_types, weights=type_weights, k=1)[0]
            account_id = self._generate_id(self.prefixes["account"], i)

            # Random balance with log-normal distribution (more realistic)
            balance = round(
                min(
                    max(random.lognormvariate(10, 2), balance_range["min"]),
                    balance_range["max"]
                ),
                2
            )

            # Opened date: between customer creation and today
            customer = next(c for c in self.customers if c["customer_id"] == customer_id)
            cust_created = datetime.fromisoformat(customer["created_date"]).date()
            opened_date = self.fake.date_between(
                start_date=cust_created, end_date="today"
            ).isoformat()

            # Dormancy score: 0 (active) to 1.0 (fully dormant)
            # Most accounts active, some dormant
            dormancy = round(random.betavariate(1.5, 8), 4)  # Skewed toward 0

            accounts.append({
                "account_id": account_id,
                "customer_id": customer_id,
                "account_type": acc_type,
                "balance": balance,
                "velocity_index": round(random.uniform(0.1, 3.0), 2),
                "opened_date": opened_date,
                "dormancy_score": dormancy,
            })

        self.accounts = accounts
        print(f"[DIM-GEN] Generated {len(accounts)} accounts")
        return accounts

    # -------------------------------------------------------------------------
    # MERCHANT GENERATION
    # -------------------------------------------------------------------------

    def generate_merchants(self) -> list[dict]:
        """
        Generate merchant entities with category-based risk profiles.

        Returns:
            List of merchant dicts with: merchant_id, merchant_name,
            mcc_code, category, risk_tier, shell_score, registered_date
        """
        pool_size = self.dim_config["merchant"]["pool_size"]
        categories = self.dim_config["merchant"]["categories"]
        shell_threshold = self.config["scenarios"]["shell_merchant"]["shell_score_threshold"]

        merchants = []
        for i in range(1, pool_size + 1):
            category = random.choice(categories)
            merchant_id = self._generate_id(self.prefixes["merchant"], i)

            # Shell score: high-risk categories get higher shell scores
            if category["risk_tier"] == "High":
                shell_score = round(random.uniform(0.40, 0.95), 4)
            elif category["risk_tier"] == "Medium":
                shell_score = round(random.uniform(0.15, 0.50), 4)
            else:
                shell_score = round(random.uniform(0.01, 0.20), 4)

            # Registration date: newer merchants can be more suspicious
            registered_date = self.fake.date_between(
                start_date="-5y", end_date="today"
            ).isoformat()

            merchants.append({
                "merchant_id": merchant_id,
                "merchant_name": self.fake.company(),
                "mcc_code": category["mcc_code"],
                "category": category["name"],
                "risk_tier": category["risk_tier"],
                "shell_score": shell_score,
                "registered_date": registered_date,
            })

        self.merchants = merchants
        print(f"[DIM-GEN] Generated {len(merchants)} merchants")
        return merchants

    # -------------------------------------------------------------------------
    # DEVICE GENERATION
    # -------------------------------------------------------------------------

    def generate_devices(self) -> list[dict]:
        """
        Generate device entities for shadow-link detection.

        Returns:
            List of device dicts with: device_id, device_type, first_seen,
            last_seen, customer_ids (list of customers who used this device)
        """
        if not self.customers:
            self.generate_customers()

        pool_size = self.dim_config["device"]["pool_size"]
        device_types = self.dim_config["device"]["types"]
        type_weights = self.dim_config["device"]["type_weights"]
        shadow_prob = self.config["scenarios"]["shadow_link"]["shared_device_probability"]

        customer_ids = [c["customer_id"] for c in self.customers]
        devices = []

        for i in range(1, pool_size + 1):
            device_type = random.choices(device_types, weights=type_weights, k=1)[0]
            device_id = f"{self.prefixes['device']}-{device_type}-{i:05d}"

            # Assign to 1 customer by default, sometimes shared (shadow link)
            assigned_customers = [random.choice(customer_ids)]
            if random.random() < shadow_prob:
                # Shared device -- assign to 2-3 customers
                extra = random.sample(customer_ids, min(random.randint(1, 2), len(customer_ids)))
                assigned_customers = list(set(assigned_customers + extra))

            first_seen = self.fake.date_time_between(
                start_date="-1y", end_date="-30d", tzinfo=timezone.utc
            ).isoformat()
            last_seen = self.fake.date_time_between(
                start_date="-30d", end_date="now", tzinfo=timezone.utc
            ).isoformat()

            devices.append({
                "device_id": device_id,
                "device_type": device_type,
                "first_seen": first_seen,
                "last_seen": last_seen,
                "customer_ids": assigned_customers,
            })

        self.devices = devices
        print(f"[DIM-GEN] Generated {len(devices)} devices ({sum(1 for d in devices if len(d['customer_ids']) > 1)} shared)")
        return devices

    # -------------------------------------------------------------------------
    # IP ADDRESS GENERATION
    # -------------------------------------------------------------------------

    def generate_ip_addresses(self) -> list[dict]:
        """
        Generate IP address entities for network fingerprinting.

        Returns:
            List of IP dicts with: ip_address, geo_location, is_vpn,
            is_tor, customer_ids (list of customers who used this IP)
        """
        if not self.customers:
            self.generate_customers()

        pool_size = self.dim_config["ip_address"]["pool_size"]
        vpn_prob = self.dim_config["ip_address"]["vpn_probability"]
        tor_prob = self.dim_config["ip_address"]["tor_probability"]
        shadow_ip_prob = self.config["scenarios"]["shadow_link"]["shared_ip_probability"]
        cities = self.sim_config["cities"]

        customer_ids = [c["customer_id"] for c in self.customers]
        ip_addresses = []

        for i in range(1, pool_size + 1):
            ip = self.fake.ipv4_private()

            # Assign to 1 customer by default, sometimes shared (shadow link)
            assigned_customers = [random.choice(customer_ids)]
            if random.random() < shadow_ip_prob:
                extra = random.sample(customer_ids, min(random.randint(1, 2), len(customer_ids)))
                assigned_customers = list(set(assigned_customers + extra))

            ip_addresses.append({
                "ip_address": ip,
                "geo_location": f"{random.choice(cities)}, India",
                "is_vpn": random.random() < vpn_prob,
                "is_tor": random.random() < tor_prob,
                "customer_ids": assigned_customers,
            })

        self.ip_addresses = ip_addresses
        vpn_count = sum(1 for ip in ip_addresses if ip["is_vpn"])
        tor_count = sum(1 for ip in ip_addresses if ip["is_tor"])
        shared_count = sum(1 for ip in ip_addresses if len(ip["customer_ids"]) > 1)
        print(f"[DIM-GEN] Generated {len(ip_addresses)} IPs ({shared_count} shared, {vpn_count} VPN, {tor_count} TOR)")
        return ip_addresses

    # -------------------------------------------------------------------------
    # GENERATE ALL DIMENSIONS
    # -------------------------------------------------------------------------

    def generate_all(self) -> dict:
        """
        Generate all dimension tables at once.

        Returns:
            Dict with keys: customers, accounts, merchants, devices, ip_addresses
        """
        print("[DIM-GEN] Generating all dimension tables from config...")
        print(f"[DIM-GEN] Config: {self.dim_config['customer']['pool_size']} customers, "
              f"{self.dim_config['account']['pool_size']} accounts, "
              f"{self.dim_config['merchant']['pool_size']} merchants, "
              f"{self.dim_config['device']['pool_size']} devices, "
              f"{self.dim_config['ip_address']['pool_size']} IPs")
        print()

        return {
            "customers": self.generate_customers(),
            "accounts": self.generate_accounts(),
            "merchants": self.generate_merchants(),
            "devices": self.generate_devices(),
            "ip_addresses": self.generate_ip_addresses(),
        }

    # -------------------------------------------------------------------------
    # EXPORT FUNCTIONS
    # -------------------------------------------------------------------------

    def export_to_csv(self, output_dir: str = None) -> dict:
        """
        Export all dimensions to CSV files.

        Args:
            output_dir: Directory to write CSVs. Defaults to data_simulator/generated_data/

        Returns:
            Dict mapping dimension name to output file path.
        """
        if output_dir is None:
            output_dir = str(Path(__file__).parent / "generated_data")

        os.makedirs(output_dir, exist_ok=True)

        # Generate if not already generated
        if not self.customers:
            self.generate_all()

        output_files = {}

        # Export each dimension
        dimensions = {
            "dim_customer": self.customers,
            "dim_account": self.accounts,
            "dim_merchant": self.merchants,
            "dim_device": self.devices,
            "dim_ip_address": self.ip_addresses,
        }

        for name, data in dimensions.items():
            if not data:
                continue

            filepath = os.path.join(output_dir, f"{name}.csv")

            # Flatten list fields (e.g., customer_ids) to pipe-delimited strings
            flat_data = []
            for row in data:
                flat_row = {}
                for k, v in row.items():
                    if isinstance(v, list):
                        flat_row[k] = "|".join(str(x) for x in v)
                    elif isinstance(v, bool):
                        flat_row[k] = str(v)
                    else:
                        flat_row[k] = v
                flat_data.append(flat_row)

            # Write CSV
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=flat_data[0].keys())
                writer.writeheader()
                writer.writerows(flat_data)

            output_files[name] = filepath
            print(f"[DIM-GEN] Exported {name} -> {filepath} ({len(data)} rows)")

        return output_files

    def export_to_json(self, output_dir: str = None) -> dict:
        """
        Export all dimensions to JSON files.

        Args:
            output_dir: Directory to write JSONs. Defaults to data_simulator/generated_data/

        Returns:
            Dict mapping dimension name to output file path.
        """
        if output_dir is None:
            output_dir = str(Path(__file__).parent / "generated_data")

        os.makedirs(output_dir, exist_ok=True)

        if not self.customers:
            self.generate_all()

        output_files = {}

        dimensions = {
            "dim_customer": self.customers,
            "dim_account": self.accounts,
            "dim_merchant": self.merchants,
            "dim_device": self.devices,
            "dim_ip_address": self.ip_addresses,
        }

        for name, data in dimensions.items():
            if not data:
                continue

            filepath = os.path.join(output_dir, f"{name}.json")
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, default=str)

            output_files[name] = filepath
            print(f"[DIM-GEN] Exported {name} -> {filepath} ({len(data)} rows)")

        return output_files

    # -------------------------------------------------------------------------
    # LOOKUP HELPERS (used by simulator and scenario engine)
    # -------------------------------------------------------------------------

    def get_random_customer(self) -> dict:
        """Get a random customer from the pool."""
        if not self.customers:
            self.generate_customers()
        return random.choice(self.customers)

    def get_random_account(self, customer_id: str = None) -> dict:
        """Get a random account, optionally filtered by customer_id."""
        if not self.accounts:
            self.generate_accounts()
        if customer_id:
            customer_accounts = [a for a in self.accounts if a["customer_id"] == customer_id]
            if customer_accounts:
                return random.choice(customer_accounts)
        return random.choice(self.accounts)

    def get_random_merchant(self, risk_tier: str = None) -> dict:
        """Get a random merchant, optionally filtered by risk tier."""
        if not self.merchants:
            self.generate_merchants()
        if risk_tier:
            filtered = [m for m in self.merchants if m["risk_tier"] == risk_tier]
            if filtered:
                return random.choice(filtered)
        return random.choice(self.merchants)

    def get_random_device(self, customer_id: str = None) -> dict:
        """Get a random device, optionally one assigned to a specific customer."""
        if not self.devices:
            self.generate_devices()
        if customer_id:
            customer_devices = [d for d in self.devices if customer_id in d["customer_ids"]]
            if customer_devices:
                return random.choice(customer_devices)
        return random.choice(self.devices)

    def get_random_ip(self, customer_id: str = None) -> dict:
        """Get a random IP address, optionally one assigned to a specific customer."""
        if not self.ip_addresses:
            self.generate_ip_addresses()
        if customer_id:
            customer_ips = [ip for ip in self.ip_addresses if customer_id in ip["customer_ids"]]
            if customer_ips:
                return random.choice(customer_ips)
        return random.choice(self.ip_addresses)

    def get_geo_location(self, city: str = None) -> str:
        """Get a random geo-location string (locality, city, India)."""
        localities = self.sim_config.get("localities", {})
        if city and city in localities:
            locality = random.choice(localities[city])
            return f"{locality}, {city}"
        elif localities:
            city = random.choice(list(localities.keys()))
            locality = random.choice(localities[city])
            return f"{locality}, {city}"
        else:
            return f"{random.choice(self.sim_config['cities'])}, India"


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import argparse
    import io

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

    parser = argparse.ArgumentParser(
        description="SENTINEL MESH V2 -- Dynamic Dimension Generator"
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory to write output files (default: data_simulator/generated_data/)",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "json", "both"],
        default="both",
        help="Output format (default: both)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible generation",
    )

    args = parser.parse_args()

    print("=" * 70)
    print("SENTINEL MESH V2 -- Dimension Generator")
    print("=" * 70)
    print()

    gen = DimensionGenerator(seed=args.seed)
    gen.generate_all()

    print()

    if args.format in ("csv", "both"):
        print("-- Exporting CSV --")
        csv_files = gen.export_to_csv(args.output_dir)
        print()

    if args.format in ("json", "both"):
        print("-- Exporting JSON --")
        json_files = gen.export_to_json(args.output_dir)
        print()

    # Summary stats
    print("=" * 70)
    print("GENERATION SUMMARY")
    print("=" * 70)
    print(f"  Customers:    {len(gen.customers)}")
    print(f"  Accounts:     {len(gen.accounts)}")
    print(f"  Merchants:    {len(gen.merchants)}")
    print(f"  Devices:      {len(gen.devices)}")
    print(f"  IP Addresses: {len(gen.ip_addresses)}")

    # Shadow link stats
    shared_devices = sum(1 for d in gen.devices if len(d["customer_ids"]) > 1)
    shared_ips = sum(1 for ip in gen.ip_addresses if len(ip["customer_ids"]) > 1)
    print(f"\n  Shadow Links:")
    print(f"    Shared devices: {shared_devices}/{len(gen.devices)}")
    print(f"    Shared IPs:     {shared_ips}/{len(gen.ip_addresses)}")

    print("=" * 70)
    print("[OK] All dimensions generated successfully!")
    print("=" * 70)
