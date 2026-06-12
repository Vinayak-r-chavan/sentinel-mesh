"""
===============================================================================
SENTINEL MESH V2 -- Scenario Engine
===============================================================================

Reads AML scenario templates (JSON files) and generates DYNAMIC transaction
events for each pattern. Every run produces DIFFERENT data.

Replaces V1's hard-coded functions like:
    get_raj_kumar_structuring_events()
    get_priya_sharma_shadow_link_events()
    get_circular_flow_events()

Now: scenario_engine.generate("structuring") produces fresh events each time.

Usage:
    from data_simulator.scenario_engine import ScenarioEngine
    from data_simulator.dimension_generator import DimensionGenerator

    dim_gen = DimensionGenerator()
    dim_gen.generate_all()

    engine = ScenarioEngine(dim_gen)
    events = engine.generate("structuring")
===============================================================================
"""

import os
import sys
import json
import random
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.config_loader import get_config, get_param


class ScenarioEngine:
    """
    Reads scenario templates and generates dynamic AML transaction events.
    Every generation produces different data — zero hard-coded payloads.
    """

    def __init__(self, dimension_generator, config: dict = None):
        """
        Initialize with a DimensionGenerator instance (for entity pools).

        Args:
            dimension_generator: DimensionGenerator instance with populated pools.
            config: Full config dict. If None, loads from config.yaml.
        """
        self.dim_gen = dimension_generator
        self.config = config or get_config()
        self.sim_config = self.config["simulator"]
        self.scenario_configs = self.config["scenarios"]

        # Load scenario templates from JSON files
        self.templates = self._load_templates()

    def _load_templates(self) -> dict:
        """Load all scenario template JSON files from config/scenarios/."""
        scenarios_dir = Path(self.config["_paths"]["scenarios_dir"])
        templates = {}

        if not scenarios_dir.exists():
            print(f"[SCENARIO] Warning: Scenarios directory not found: {scenarios_dir}")
            return templates

        for json_file in scenarios_dir.glob("*.json"):
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    template = json.load(f)
                scenario_id = template.get("scenario_id", json_file.stem)
                templates[scenario_id] = template
            except (json.JSONDecodeError, KeyError) as e:
                print(f"[SCENARIO] Warning: Failed to load {json_file}: {e}")

        print(f"[SCENARIO] Loaded {len(templates)} scenario templates: {list(templates.keys())}")
        return templates

    def _generate_txn_id(self, scenario_id: str, index: int) -> str:
        """Generate a unique transaction ID for a scenario event."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        rand = random.randint(1000, 9999)
        return f"TXN-{scenario_id[:4].upper()}-{ts}-{rand}-{index:03d}"

    def _make_event(self, customer, account, amount, channel, geo_location,
                    counterparty_account, txn_type, timestamp, scenario_id,
                    device_id=None, ip_address=None, merchant_id=None,
                    mcc_code=None) -> dict:
        """Create a standardized transaction event dict."""
        return {
            "transaction_id": self._generate_txn_id(scenario_id, random.randint(1, 999)),
            "customer_id": customer["customer_id"],
            "customer_name": customer["name"],
            "account_id": account["account_id"],
            "amount": round(amount, 2),
            "timestamp": timestamp.isoformat() if isinstance(timestamp, datetime) else timestamp,
            "channel": channel,
            "device_id": device_id,
            "ip_address": ip_address,
            "geo_location": geo_location,
            "merchant_id": merchant_id or "MERC-NONE",
            "mcc_code": mcc_code,
            "counterparty_account": counterparty_account,
            "transaction_type": txn_type,
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            "processing_status": "pending",
            "risk_flag": "none",
            "pattern_hash": hashlib.md5(f"{scenario_id}-{random.random()}".encode()).hexdigest()[:12],
            "scenario_id": scenario_id,
        }

    # =========================================================================
    # SCENARIO GENERATORS
    # =========================================================================

    def generate(self, scenario_id: str) -> list[dict]:
        """
        Generate transaction events for a given scenario.

        Args:
            scenario_id: One of: structuring, circular_flow, shadow_link,
                        velocity_spike, shell_merchant, dormant_activation

        Returns:
            List of transaction event dicts ready to stream.
        """
        generators = {
            "structuring": self._generate_structuring,
            "circular_flow": self._generate_circular_flow,
            "shadow_link": self._generate_shadow_link,
            "velocity_spike": self._generate_velocity_spike,
            "shell_merchant": self._generate_shell_merchant,
            "dormant_activation": self._generate_dormant_activation,
        }

        if scenario_id not in generators:
            print(f"[SCENARIO] Unknown scenario: {scenario_id}")
            return []

        # Check if scenario is enabled in config
        scenario_cfg = self.scenario_configs.get(scenario_id, {})
        if not scenario_cfg.get("enabled", True):
            return []

        events = generators[scenario_id]()
        print(f"[SCENARIO] Generated {len(events)} events for '{scenario_id}'")
        return events

    def generate_random(self) -> list[dict]:
        """Generate events for a random enabled scenario."""
        enabled = [
            sid for sid in ["structuring", "circular_flow", "shadow_link",
                           "velocity_spike", "shell_merchant", "dormant_activation"]
            if self.scenario_configs.get(sid, {}).get("enabled", True)
        ]
        if not enabled:
            return []
        return self.generate(random.choice(enabled))

    def generate_all(self) -> list[dict]:
        """Generate events for ALL enabled scenarios."""
        all_events = []
        for scenario_id in ["structuring", "circular_flow", "shadow_link",
                           "velocity_spike", "shell_merchant", "dormant_activation"]:
            all_events.extend(self.generate(scenario_id))
        return all_events

    # -------------------------------------------------------------------------
    # STRUCTURING (Smurfing)
    # -------------------------------------------------------------------------

    def _generate_structuring(self) -> list[dict]:
        """Generate structuring / smurfing transaction events."""
        cfg = self.scenario_configs["structuring"]
        params = self.templates.get("structuring", {}).get("parameters", {})

        # Pick a random customer from the pool
        customer = self.dim_gen.get_random_customer()
        account = self.dim_gen.get_random_account(customer["customer_id"])

        # How many deposits?
        count_range = cfg.get("deposit_count_range", params.get("deposit_count", {}))
        deposit_count = random.randint(count_range["min"], count_range["max"])

        # Amount range (each below threshold)
        amt_range = cfg.get("individual_amount_range", params.get("individual_amount", {}))

        # Time spread
        time_spread_hours = cfg.get("time_spread_hours", 8)

        # Channels
        channels = cfg.get("preferred_channels", ["Branch", "ATM", "Mobile", "UPI"])

        # Geo locations
        city = customer.get("city", "Bangalore")
        geo_pool_size = cfg.get("geo_pool_size", 5)

        base_time = datetime.now(timezone.utc)
        events = []

        for i in range(deposit_count):
            # Amount: just below threshold, with some variation
            amount = random.uniform(amt_range["min"], amt_range["max"])
            # Avoid perfectly round numbers (realistic structuring)
            amount = round(amount + random.uniform(-5000, 5000), 2)

            # Spread across time window
            delay_minutes = random.uniform(0, time_spread_hours * 60)
            txn_time = base_time + timedelta(minutes=delay_minutes)

            channel = random.choice(channels)
            geo = self.dim_gen.get_geo_location(city)

            # Device/IP for digital channels
            device_id = None
            ip_address = None
            if channel in ["Mobile", "UPI"]:
                device = self.dim_gen.get_random_device(customer["customer_id"])
                device_id = device["device_id"]
                ip_obj = self.dim_gen.get_random_ip(customer["customer_id"])
                ip_address = ip_obj["ip_address"]

            events.append(self._make_event(
                customer=customer,
                account=account,
                amount=amount,
                channel=channel,
                geo_location=geo,
                counterparty_account=account["account_id"],
                txn_type="deposit",
                timestamp=txn_time,
                scenario_id="structuring",
                device_id=device_id,
                ip_address=ip_address,
            ))

        # Sort by timestamp
        events.sort(key=lambda e: e["timestamp"])
        return events

    # -------------------------------------------------------------------------
    # CIRCULAR FLOW (Layering)
    # -------------------------------------------------------------------------

    def _generate_circular_flow(self) -> list[dict]:
        """Generate circular fund flow events (A->B->C->...->A)."""
        cfg = self.scenario_configs["circular_flow"]

        # Number of hops
        hop_range = cfg.get("hop_count_range", {"min": 3, "max": 7})
        hop_count = random.randint(hop_range["min"], hop_range["max"])

        # Pick unique customers for each hop
        participants = []
        used_ids = set()
        for _ in range(hop_count):
            attempts = 0
            while attempts < 50:
                cust = self.dim_gen.get_random_customer()
                if cust["customer_id"] not in used_ids:
                    used_ids.add(cust["customer_id"])
                    acc = self.dim_gen.get_random_account(cust["customer_id"])
                    participants.append({"customer": cust, "account": acc})
                    break
                attempts += 1

        if len(participants) < 3:
            return []

        # Base amount with slight variation between hops
        amt_range = cfg.get("amount_range", {"min": 1000000, "max": 5000000})
        base_amount = random.uniform(amt_range["min"], amt_range["max"])
        variation_pct = cfg.get("amount_variation_pct", 0.02)

        # Timing
        delay_range = cfg.get("inter_hop_delay_seconds", {"min": 2, "max": 30})
        channels = ["Mobile", "UPI", "SWIFT"]
        base_time = datetime.now(timezone.utc)

        events = []
        for i in range(len(participants)):
            sender = participants[i]
            receiver = participants[(i + 1) % len(participants)]  # Wraps around

            # Slight amount variation
            variation = base_amount * random.uniform(-variation_pct, variation_pct)
            amount = base_amount + variation

            delay_secs = random.uniform(delay_range["min"], delay_range["max"])
            txn_time = base_time + timedelta(seconds=i * delay_secs)

            channel = random.choice(channels)
            geo = self.dim_gen.get_geo_location()

            device_id = None
            ip_address = None
            if channel in ["Mobile", "UPI"]:
                device = self.dim_gen.get_random_device(sender["customer"]["customer_id"])
                device_id = device["device_id"]
                ip_obj = self.dim_gen.get_random_ip(sender["customer"]["customer_id"])
                ip_address = ip_obj["ip_address"]

            events.append(self._make_event(
                customer=sender["customer"],
                account=sender["account"],
                amount=amount,
                channel=channel,
                geo_location=geo,
                counterparty_account=receiver["account"]["account_id"],
                txn_type="transfer",
                timestamp=txn_time,
                scenario_id="circular_flow",
                device_id=device_id,
                ip_address=ip_address,
            ))

        return events

    # -------------------------------------------------------------------------
    # SHADOW LINK (Hidden Relationship)
    # -------------------------------------------------------------------------

    def _generate_shadow_link(self) -> list[dict]:
        """Generate shadow link events — shared device/IP between customers."""
        cfg = self.scenario_configs["shadow_link"]

        # Pick 2-3 customers who will share a device
        linked_count = random.randint(2, 3)
        customers = []
        used_ids = set()
        for _ in range(linked_count):
            attempts = 0
            while attempts < 50:
                cust = self.dim_gen.get_random_customer()
                if cust["customer_id"] not in used_ids:
                    used_ids.add(cust["customer_id"])
                    customers.append(cust)
                    break
                attempts += 1

        if len(customers) < 2:
            return []

        # Shared device and IP (the shadow link)
        shared_device = self.dim_gen.get_random_device()
        shared_ip = self.dim_gen.get_random_ip()

        amt_range = cfg.get("linked_txn_amount_range", {"min": 500000, "max": 5000000})
        time_window = cfg.get("time_proximity_hours", 48)
        base_time = datetime.now(timezone.utc)

        events = []
        for cust in customers:
            account = self.dim_gen.get_random_account(cust["customer_id"])
            txn_count = random.randint(1, 3)

            for j in range(txn_count):
                amount = random.uniform(amt_range["min"], amt_range["max"])
                delay_hours = random.uniform(0, time_window)
                txn_time = base_time - timedelta(hours=delay_hours)

                channel = random.choice(["Mobile", "UPI"])
                geo = self.dim_gen.get_geo_location(cust.get("city"))
                counterparty = self.dim_gen.get_random_account()

                events.append(self._make_event(
                    customer=cust,
                    account=account,
                    amount=amount,
                    channel=channel,
                    geo_location=geo,
                    counterparty_account=counterparty["account_id"],
                    txn_type="transfer",
                    timestamp=txn_time,
                    scenario_id="shadow_link",
                    device_id=shared_device["device_id"],  # Same device!
                    ip_address=shared_ip["ip_address"],     # Same IP!
                ))

        events.sort(key=lambda e: e["timestamp"])
        return events

    # -------------------------------------------------------------------------
    # VELOCITY SPIKE
    # -------------------------------------------------------------------------

    def _generate_velocity_spike(self) -> list[dict]:
        """Generate velocity spike / burst activity events."""
        cfg = self.scenario_configs["velocity_spike"]

        customer = self.dim_gen.get_random_customer()
        account = self.dim_gen.get_random_account(customer["customer_id"])

        txn_range = cfg.get("spike_txn_count_range", {"min": 8, "max": 20})
        txn_count = random.randint(txn_range["min"], txn_range["max"])

        duration_hours = cfg.get("spike_duration_hours", 8)
        channels = ["Mobile", "UPI", "Branch", "ATM"]
        base_time = datetime.now(timezone.utc)

        events = []
        for i in range(txn_count):
            # High amounts (spike)
            amount = random.uniform(100000, 2000000)
            delay_minutes = random.uniform(0, duration_hours * 60)
            txn_time = base_time + timedelta(minutes=delay_minutes)

            channel = random.choice(channels)
            geo = self.dim_gen.get_geo_location(customer.get("city"))
            counterparty = self.dim_gen.get_random_account()

            device_id = None
            ip_address = None
            if channel in ["Mobile", "UPI"]:
                device = self.dim_gen.get_random_device(customer["customer_id"])
                device_id = device["device_id"]
                ip_obj = self.dim_gen.get_random_ip(customer["customer_id"])
                ip_address = ip_obj["ip_address"]

            txn_type = random.choice(["transfer", "deposit", "withdrawal"])

            events.append(self._make_event(
                customer=customer,
                account=account,
                amount=amount,
                channel=channel,
                geo_location=geo,
                counterparty_account=counterparty["account_id"],
                txn_type=txn_type,
                timestamp=txn_time,
                scenario_id="velocity_spike",
                device_id=device_id,
                ip_address=ip_address,
            ))

        events.sort(key=lambda e: e["timestamp"])
        return events

    # -------------------------------------------------------------------------
    # SHELL MERCHANT
    # -------------------------------------------------------------------------

    def _generate_shell_merchant(self) -> list[dict]:
        """Generate shell merchant / front company events."""
        cfg = self.scenario_configs["shell_merchant"]

        # Pick a high-risk merchant
        merchant = self.dim_gen.get_random_merchant(risk_tier="High")
        if not merchant:
            merchant = self.dim_gen.get_random_merchant()

        # 1-3 concentrated customers (most volume from few sources)
        concentrated_count = random.randint(1, 3)
        customers = []
        for _ in range(concentrated_count):
            customers.append(self.dim_gen.get_random_customer())

        txn_count = random.randint(5, 15)
        base_time = datetime.now(timezone.utc)

        events = []
        for i in range(txn_count):
            # Most transactions from the concentrated customers
            customer = random.choice(customers)
            account = self.dim_gen.get_random_account(customer["customer_id"])

            amount = random.uniform(200000, 3000000)
            delay_hours = random.uniform(0, 72)
            txn_time = base_time - timedelta(hours=delay_hours)

            channel = random.choice(["POS", "Mobile", "UPI"])
            geo = self.dim_gen.get_geo_location(customer.get("city"))

            device_id = None
            ip_address = None
            if channel in ["Mobile", "UPI"]:
                device = self.dim_gen.get_random_device(customer["customer_id"])
                device_id = device["device_id"]
                ip_obj = self.dim_gen.get_random_ip(customer["customer_id"])
                ip_address = ip_obj["ip_address"]

            events.append(self._make_event(
                customer=customer,
                account=account,
                amount=amount,
                channel=channel,
                geo_location=geo,
                counterparty_account=f"ACC-{merchant['merchant_id']}",
                txn_type="payment",
                timestamp=txn_time,
                scenario_id="shell_merchant",
                device_id=device_id,
                ip_address=ip_address,
                merchant_id=merchant["merchant_id"],
                mcc_code=merchant["mcc_code"],
            ))

        events.sort(key=lambda e: e["timestamp"])
        return events

    # -------------------------------------------------------------------------
    # DORMANT ACTIVATION
    # -------------------------------------------------------------------------

    def _generate_dormant_activation(self) -> list[dict]:
        """Generate dormant account activation events."""
        cfg = self.scenario_configs["dormant_activation"]

        customer = self.dim_gen.get_random_customer()

        # Try to find a dormant account
        dormant_accounts = [
            a for a in self.dim_gen.accounts
            if a["customer_id"] == customer["customer_id"]
            and a["dormancy_score"] > 0.5
        ]
        if dormant_accounts:
            account = random.choice(dormant_accounts)
        else:
            account = self.dim_gen.get_random_account(customer["customer_id"])

        txn_range = cfg.get("activation_txn_count_range", {"min": 3, "max": 10})
        txn_count = random.randint(txn_range["min"], txn_range["max"])

        amt_range = cfg.get("activation_amount_range", {"min": 500000, "max": 3000000})
        window_hours = cfg.get("activation_window_hours", 24)

        base_time = datetime.now(timezone.utc)
        channels = ["Mobile", "UPI", "Branch"]

        events = []
        for i in range(txn_count):
            amount = random.uniform(amt_range["min"], amt_range["max"])
            delay_minutes = random.uniform(0, window_hours * 60)
            txn_time = base_time + timedelta(minutes=delay_minutes)

            channel = random.choice(channels)
            geo = self.dim_gen.get_geo_location(customer.get("city"))
            counterparty = self.dim_gen.get_random_account()

            device_id = None
            ip_address = None
            if channel in ["Mobile", "UPI"]:
                device = self.dim_gen.get_random_device(customer["customer_id"])
                device_id = device["device_id"]
                ip_obj = self.dim_gen.get_random_ip(customer["customer_id"])
                ip_address = ip_obj["ip_address"]

            txn_type = "deposit" if i < txn_count // 2 else "transfer"

            events.append(self._make_event(
                customer=customer,
                account=account,
                amount=amount,
                channel=channel,
                geo_location=geo,
                counterparty_account=counterparty["account_id"],
                txn_type=txn_type,
                timestamp=txn_time,
                scenario_id="dormant_activation",
                device_id=device_id,
                ip_address=ip_address,
            ))

        events.sort(key=lambda e: e["timestamp"])
        return events
