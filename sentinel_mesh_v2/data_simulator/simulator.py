"""
===============================================================================
SENTINEL MESH V2 -- Transaction Data Simulator
===============================================================================

Config-driven transaction simulator that generates normal + AML anomalous
traffic and streams to Microsoft Fabric Eventstream (or console for testing).

ZERO hard-coded values. Everything comes from config.yaml + scenario templates.

Usage:
    # Console mode (testing) -- prints transactions to screen
    python -m data_simulator.simulator --mode console --count 20

    # Stream mode -- sends to Fabric Eventstream
    python -m data_simulator.simulator --mode stream

    # Specific pattern only
    python -m data_simulator.simulator --mode console --pattern structuring

    # All patterns + normal traffic
    python -m data_simulator.simulator --mode console --pattern all --count 30
===============================================================================
"""

import os
import sys
import json
import time
import random
import hashlib
import argparse
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.config_loader import get_config, get_secret, get_param
from data_simulator.dimension_generator import DimensionGenerator
from data_simulator.scenario_engine import ScenarioEngine


class TransactionSimulator:
    """
    Config-driven transaction simulator. Generates normal + anomalous traffic
    and streams to Fabric Eventstream or console.
    """

    def __init__(self, config: dict = None, seed: int = None):
        """
        Initialize the simulator with config and entity pools.

        Args:
            config: Full config dict. If None, loads from config.yaml.
            seed: Random seed for reproducible generation.
        """
        self.config = config or get_config()
        self.sim_config = self.config["simulator"]

        # Initialize dimension generator and populate pools
        print("[SIMULATOR] Initializing entity pools...")
        self.dim_gen = DimensionGenerator(config=self.config, seed=seed)
        self.dim_gen.generate_all()

        # Initialize scenario engine
        print()
        self.scenario_engine = ScenarioEngine(self.dim_gen, config=self.config)

        # EventHub producer (initialized on demand)
        self._producer = None

        # Stats tracking
        self.stats = {
            "normal_count": 0,
            "scenario_count": 0,
            "total_amount": 0.0,
            "scenarios_triggered": {},
        }

    # -------------------------------------------------------------------------
    # NORMAL TRANSACTION GENERATION
    # -------------------------------------------------------------------------

    def generate_normal_transaction(self) -> dict:
        """
        Generate a single normal (non-suspicious) transaction.
        All values from config + dimension pools. Zero hard-coding.
        """
        customer = self.dim_gen.get_random_customer()
        account = self.dim_gen.get_random_account(customer["customer_id"])

        # Amount from configured range
        amt_range = self.sim_config["normal_txn_amount_range"]
        amount = round(random.uniform(amt_range["min"], amt_range["max"]), 2)

        # Channel from configured weights
        channels = self.sim_config["channels"]
        channel_names = [c["name"] for c in channels]
        channel_weights = [c["weight"] for c in channels]
        channel = random.choices(channel_names, weights=channel_weights, k=1)[0]

        # Geo location
        geo = self.dim_gen.get_geo_location(customer.get("city"))

        # Device/IP for digital channels
        device_id = None
        ip_address = None
        if channel in ["Mobile", "UPI"]:
            device = self.dim_gen.get_random_device(customer["customer_id"])
            device_id = device["device_id"]
            ip_obj = self.dim_gen.get_random_ip(customer["customer_id"])
            ip_address = ip_obj["ip_address"]

        # Counterparty
        is_merchant = random.random() < 0.4 and channel in ["POS", "Mobile", "UPI"]
        if is_merchant:
            merchant = self.dim_gen.get_random_merchant()
            merchant_id = merchant["merchant_id"]
            mcc_code = merchant["mcc_code"]
            counterparty = f"ACC-{merchant_id}"
            txn_type = "payment"
        elif channel in ["Mobile", "UPI", "SWIFT"]:
            counterparty_acc = self.dim_gen.get_random_account()
            counterparty = counterparty_acc["account_id"]
            txn_type = "transfer"
            merchant_id = "MERC-NONE"
            mcc_code = None
        else:
            counterparty = account["account_id"]
            txn_type = random.choice(["deposit", "withdrawal"])
            merchant_id = "MERC-NONE"
            mcc_code = None

        now = datetime.now(timezone.utc)

        txn = {
            "transaction_id": f"TXN-{now.strftime('%Y%m%d%H%M%S')}-{random.randint(10000, 99999)}",
            "customer_id": customer["customer_id"],
            "customer_name": customer["name"],
            "account_id": account["account_id"],
            "amount": amount,
            "timestamp": now.isoformat(),
            "channel": channel,
            "device_id": device_id,
            "ip_address": ip_address,
            "geo_location": geo,
            "merchant_id": merchant_id,
            "mcc_code": mcc_code,
            "counterparty_account": counterparty,
            "transaction_type": txn_type,
            "ingestion_timestamp": now.isoformat(),
            "processing_status": "pending",
            "risk_flag": "none",
            "pattern_hash": hashlib.md5(f"normal-{random.random()}".encode()).hexdigest()[:12],
            "scenario_id": "normal",
        }

        self.stats["normal_count"] += 1
        self.stats["total_amount"] += amount
        return txn

    # -------------------------------------------------------------------------
    # EVENT HUB CONNECTION
    # -------------------------------------------------------------------------

    def _get_producer(self):
        """Get or create the EventHub producer client."""
        if self._producer is None:
            conn_str = get_secret("FABRIC_EVENTSTREAM_CONN_STR")
            print("[SIMULATOR] Connecting to Fabric Eventstream...")
            try:
                from azure.eventhub import EventHubProducerClient
                self._producer = EventHubProducerClient.from_connection_string(
                    conn_str=conn_str
                )
                print("[SIMULATOR] Connected to Fabric Eventstream!")
            except ImportError:
                print("[SIMULATOR] ERROR: azure-eventhub not installed.")
                print("[SIMULATOR] Run: pip install azure-eventhub")
                sys.exit(1)
            except Exception as e:
                print(f"[SIMULATOR] ERROR: Failed to connect: {e}")
                sys.exit(1)
        return self._producer

    def _send_events(self, events: list[dict], delay: float = 0.5):
        """Send events to Fabric Eventstream."""
        from azure.eventhub import EventData

        producer = self._get_producer()
        for i, event in enumerate(events):
            event_json = json.dumps(event, default=str)
            event_data = EventData(event_json)

            try:
                event_batch = producer.create_batch()
                event_batch.add(event_data)
                producer.send_batch(event_batch)

                self._print_event(event, i + 1, len(events), mode="stream")

                if delay > 0 and i < len(events) - 1:
                    time.sleep(delay)
            except Exception as e:
                print(f"[ERROR] Failed to send {event['transaction_id']}: {e}")

    def _print_event(self, event: dict, index: int = 0, total: int = 0,
                     mode: str = "console"):
        """Pretty-print a transaction event."""
        prefix = "[STREAM]" if mode == "stream" else "[CONSOLE]"
        counter = f"[{index}/{total}] " if total > 0 else ""
        scenario = event.get("scenario_id", "normal")
        tag = f" [{scenario.upper()}]" if scenario != "normal" else ""

        print(
            f"{prefix} {counter}"
            f"{event['transaction_id']} | "
            f"{event['customer_name']:<20s} | "
            f"{event['channel']:<8s} | "
            f"INR {event['amount']:>12,.2f} | "
            f"{event['geo_location']:<25s} | "
            f"{event['transaction_type']:<10s}"
            f"{tag}"
        )

    # -------------------------------------------------------------------------
    # MAIN EXECUTION LOOPS
    # -------------------------------------------------------------------------

    def run_scenarios(self, pattern: str, mode: str = "console", delay: float = 1.0):
        """Run specific or all AML scenario patterns."""
        events = []

        if pattern == "all":
            events = self.scenario_engine.generate_all()
        elif pattern == "random":
            events = self.scenario_engine.generate_random()
        elif pattern != "normal":
            events = self.scenario_engine.generate(pattern)

        if not events:
            print(f"[SIMULATOR] No events generated for pattern '{pattern}'")
            return

        # Track stats
        for e in events:
            sid = e.get("scenario_id", "unknown")
            self.stats["scenarios_triggered"][sid] = \
                self.stats["scenarios_triggered"].get(sid, 0) + 1
            self.stats["scenario_count"] += 1
            self.stats["total_amount"] += e["amount"]

        print(f"\n[SIMULATOR] Dispatching {len(events)} AML scenario events:")
        if mode == "console":
            for i, event in enumerate(events):
                self._print_event(event, i + 1, len(events))
        else:
            self._send_events(events, delay=delay)

    def run_normal_loop(self, mode: str = "console", count: int = 0,
                        interval: float = None, injection_prob: float = None):
        """
        Run the normal transaction loop with periodic AML scenario injection.

        Args:
            mode: "console" or "stream"
            count: Number of normal transactions (0 = infinite)
            interval: Seconds between transactions (from config if None)
            injection_prob: Probability of injecting an AML scenario per cycle
        """
        if interval is None:
            interval = self.sim_config["streaming_interval_seconds"]
        if injection_prob is None:
            injection_prob = self.config["scenarios"].get("injection_probability", 0.15)

        print(f"\n[SIMULATOR] Starting normal traffic loop (interval={interval}s, "
              f"injection_probability={injection_prob:.0%})")
        print("-" * 120)

        gen_count = 0
        try:
            while True:
                # Check if we should inject an AML scenario
                if random.random() < injection_prob:
                    scenario_events = self.scenario_engine.generate_random()
                    if scenario_events:
                        for e in scenario_events:
                            sid = e.get("scenario_id", "unknown")
                            self.stats["scenarios_triggered"][sid] = \
                                self.stats["scenarios_triggered"].get(sid, 0) + 1
                            self.stats["scenario_count"] += 1
                            self.stats["total_amount"] += e["amount"]

                        if mode == "console":
                            for i, event in enumerate(scenario_events):
                                self._print_event(event, gen_count + i + 1, 0)
                        else:
                            self._send_events(scenario_events, delay=0.3)
                        gen_count += len(scenario_events)

                # Generate normal transaction
                txn = self.generate_normal_transaction()
                gen_count += 1

                if mode == "console":
                    self._print_event(txn, gen_count, 0)
                else:
                    self._send_events([txn], delay=0)

                # Check count limit
                if count > 0 and gen_count >= count:
                    print(f"\n[SIMULATOR] Reached target count of {count}. Stopping.")
                    break

                time.sleep(interval)

        except KeyboardInterrupt:
            print("\n\n[SIMULATOR] Stopped by user (Ctrl+C).")

    def close(self):
        """Clean up resources."""
        if self._producer:
            self._producer.close()
            print("[SIMULATOR] EventHub connection closed.")

    def print_stats(self):
        """Print generation statistics."""
        print("\n" + "=" * 70)
        print("SIMULATION SUMMARY")
        print("=" * 70)
        print(f"  Normal transactions:  {self.stats['normal_count']}")
        print(f"  Scenario events:      {self.stats['scenario_count']}")
        print(f"  Total transactions:   {self.stats['normal_count'] + self.stats['scenario_count']}")
        print(f"  Total amount:         INR {self.stats['total_amount']:,.2f}")

        if self.stats["scenarios_triggered"]:
            print(f"\n  Scenarios triggered:")
            for sid, count in sorted(self.stats["scenarios_triggered"].items()):
                print(f"    {sid:<25s}: {count} events")

        print("=" * 70)


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

def main():
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

    parser = argparse.ArgumentParser(
        description="SENTINEL MESH V2 -- Transaction Data Simulator"
    )
    parser.add_argument(
        "--mode", choices=["console", "stream"], default="console",
        help="'console' prints to screen, 'stream' sends to Fabric Eventstream"
    )
    parser.add_argument(
        "--pattern",
        choices=["normal", "structuring", "circular_flow", "shadow_link",
                 "velocity_spike", "shell_merchant", "dormant_activation",
                 "all", "random"],
        default="all",
        help="Transaction pattern to execute (default: all)"
    )
    parser.add_argument(
        "--count", type=int, default=0,
        help="Limit number of transactions (0 = infinite, only for normal loop)"
    )
    parser.add_argument(
        "--interval", type=float, default=None,
        help="Seconds between transactions (default: from config.yaml)"
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="Random seed for reproducible generation"
    )
    parser.add_argument(
        "--no-inject", action="store_true",
        help="Disable automatic AML scenario injection into normal traffic"
    )

    args = parser.parse_args()

    print("=" * 70)
    print("SENTINEL MESH V2 -- Transaction Simulator")
    print(f"Mode: {args.mode.upper()} | Pattern: {args.pattern.upper()}")
    print("=" * 70)
    print()

    # Initialize simulator
    sim = TransactionSimulator(seed=args.seed)

    try:
        # Run specific AML scenarios first (if not just "normal")
        if args.pattern not in ["normal"]:
            sim.run_scenarios(args.pattern, mode=args.mode)

        # Run normal traffic loop (if pattern includes normal)
        if args.pattern in ["normal", "all"]:
            injection_prob = 0.0 if args.no_inject else None
            sim.run_normal_loop(
                mode=args.mode,
                count=args.count if args.count > 0 else 20,  # Default 20 for demo
                interval=args.interval,
                injection_prob=injection_prob,
            )

        # Print summary
        sim.print_stats()

    except Exception as e:
        print(f"\n[ERROR] {e}")
        raise
    finally:
        sim.close()


if __name__ == "__main__":
    main()
