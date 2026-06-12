"""
═══════════════════════════════════════════════════════════════════════════════
SENTINEL MESH V2 — Unified Configuration Loader
═══════════════════════════════════════════════════════════════════════════════

Loads configuration from multiple sources with priority:
    dim_adaptive_config (L9) > Azure App Config > config.yaml > defaults

Usage:
    from config.config_loader import get_config, get_secret

    config = get_config()
    conn_str = get_secret("FABRIC_EVENTSTREAM_CONN_STR")

    # Access any parameter
    pool_size = config["simulator"]["customer_pool_size"]
    threshold = config["agents"]["structuring"]["amount_threshold"]
═══════════════════════════════════════════════════════════════════════════════
"""

import os
import sys
import yaml
from pathlib import Path
from dotenv import load_dotenv
from functools import lru_cache


# ─────────────────────────────────────────────────────────────────────────────
# PATH RESOLUTION
# ─────────────────────────────────────────────────────────────────────────────

def _get_config_dir() -> Path:
    """Returns the absolute path to the config/ directory."""
    return Path(__file__).parent.resolve()


def _get_project_root() -> Path:
    """Returns the absolute path to the sentinel_mesh_v2/ root directory."""
    return _get_config_dir().parent


# ─────────────────────────────────────────────────────────────────────────────
# ENVIRONMENT VARIABLES
# ─────────────────────────────────────────────────────────────────────────────

def _load_env():
    """Load environment variables from .env file."""
    env_path = _get_config_dir() / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=str(env_path), override=True)
    else:
        print(f"[CONFIG] Warning: .env file not found at {env_path}", file=sys.stderr)


# ─────────────────────────────────────────────────────────────────────────────
# YAML CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

def _load_yaml() -> dict:
    """Load the master config.yaml file."""
    yaml_path = _get_config_dir() / "config.yaml"
    if not yaml_path.exists():
        print(f"[CONFIG] CRITICAL: config.yaml not found at {yaml_path}", file=sys.stderr)
        sys.exit(1)

    with open(yaml_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if config is None:
        print("[CONFIG] CRITICAL: config.yaml is empty!", file=sys.stderr)
        sys.exit(1)

    return config


# ─────────────────────────────────────────────────────────────────────────────
# ENVIRONMENT VARIABLE INTERPOLATION
# ─────────────────────────────────────────────────────────────────────────────

def _interpolate_env_vars(config: dict) -> dict:
    """
    Replace ${ENV_VAR} placeholders in config values with actual
    environment variable values. Walks the entire config tree recursively.
    """
    if isinstance(config, dict):
        return {k: _interpolate_env_vars(v) for k, v in config.items()}
    elif isinstance(config, list):
        return [_interpolate_env_vars(item) for item in config]
    elif isinstance(config, str) and config.startswith("${") and config.endswith("}"):
        env_var_name = config[2:-1]
        env_value = os.getenv(env_var_name)
        if env_value:
            return env_value
        # Return the placeholder if env var not set (non-critical for POC)
        return config
    else:
        return config


# ─────────────────────────────────────────────────────────────────────────────
# KEY VAULT INTEGRATION (Future — when Azure Key Vault is provisioned)
# ─────────────────────────────────────────────────────────────────────────────

def _load_keyvault_secrets(config: dict) -> dict:
    """
    Fetch secrets from Azure Key Vault and merge into config.
    Only activates when AZURE_KEYVAULT_URL is set.

    Future implementation — currently returns config unchanged.
    """
    keyvault_url = os.getenv("AZURE_KEYVAULT_URL")
    if not keyvault_url or keyvault_url.startswith("${"):
        return config

    # When Key Vault is provisioned, uncomment this:
    # try:
    #     from azure.identity import DefaultAzureCredential
    #     from azure.keyvault.secrets import SecretClient
    #
    #     credential = DefaultAzureCredential()
    #     client = SecretClient(vault_url=keyvault_url, credential=credential)
    #
    #     # Fetch specific secrets and inject into config
    #     secrets_to_fetch = [
    #         "aoai-api-key", "aoai-endpoint",
    #         "eventstream-conn-str", "fabric-sql-endpoint",
    #         "teams-webhook-url", "alert-email-list",
    #     ]
    #     for secret_name in secrets_to_fetch:
    #         try:
    #             secret = client.get_secret(secret_name)
    #             os.environ[secret_name.upper().replace("-", "_")] = secret.value
    #         except Exception:
    #             pass
    # except ImportError:
    #     print("[CONFIG] Warning: azure-identity not installed. Key Vault disabled.")

    return config


# ─────────────────────────────────────────────────────────────────────────────
# APP CONFIGURATION INTEGRATION (Future — when Azure App Config is provisioned)
# ─────────────────────────────────────────────────────────────────────────────

def _load_app_config_overrides(config: dict) -> dict:
    """
    Fetch runtime overrides from Azure App Configuration.
    Only activates when AZURE_APP_CONFIG_ENDPOINT is set.

    Future implementation — currently returns config unchanged.
    """
    app_config_endpoint = os.getenv("AZURE_APP_CONFIG_ENDPOINT")
    if not app_config_endpoint or app_config_endpoint.startswith("${"):
        return config

    # When App Configuration is provisioned, uncomment this:
    # try:
    #     from azure.identity import DefaultAzureCredential
    #     from azure.appconfiguration import AzureAppConfigurationClient
    #
    #     credential = DefaultAzureCredential()
    #     client = AzureAppConfigurationClient(
    #         base_url=app_config_endpoint, credential=credential
    #     )
    #
    #     # Fetch all settings with "sentinel." prefix
    #     for setting in client.list_configuration_settings(key_filter="sentinel.*"):
    #         # Convert "sentinel.scoring.weights.f1" → config["scoring"]["weights"]["f1"]
    #         keys = setting.key.replace("sentinel.", "").split(".")
    #         _deep_set(config, keys, setting.value)
    # except ImportError:
    #     print("[CONFIG] Warning: azure-appconfiguration not installed.")

    return config


# ─────────────────────────────────────────────────────────────────────────────
# UTILITY FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def _deep_get(config: dict, keys: list, default=None):
    """
    Safely get a nested value from config dict.
    Example: _deep_get(config, ["agents", "structuring", "amount_threshold"])
    """
    current = config
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current


def _deep_set(config: dict, keys: list, value):
    """
    Set a nested value in config dict, creating intermediate dicts as needed.
    Example: _deep_set(config, ["scoring", "weights", "f1"], 40)
    """
    current = config
    for key in keys[:-1]:
        if key not in current or not isinstance(current[key], dict):
            current[key] = {}
        current = current[key]
    current[keys[-1]] = value


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def get_config() -> dict:
    """
    Load and return the complete merged configuration.

    Priority (highest wins):
        1. Azure App Configuration overrides
        2. Azure Key Vault secrets
        3. Environment variable interpolation
        4. config.yaml values
        5. Built-in defaults

    Returns:
        dict: The complete configuration dictionary.

    The result is cached — call reload_config() to force a refresh.
    """
    # Step 1: Load .env
    _load_env()

    # Step 2: Load config.yaml
    config = _load_yaml()

    # Step 3: Interpolate ${ENV_VAR} placeholders
    config = _interpolate_env_vars(config)

    # Step 4: Overlay Key Vault secrets (future)
    config = _load_keyvault_secrets(config)

    # Step 5: Overlay App Configuration (future)
    config = _load_app_config_overrides(config)

    # Inject project paths for convenience
    config["_paths"] = {
        "project_root": str(_get_project_root()),
        "config_dir": str(_get_config_dir()),
        "scenarios_dir": str(_get_config_dir() / "scenarios"),
        "data_simulator_dir": str(_get_project_root() / "data_simulator"),
    }

    print(f"[CONFIG] Loaded configuration from {_get_config_dir() / 'config.yaml'}")
    return config


def get_secret(env_var_name: str, required: bool = True) -> str:
    """
    Get a secret value from environment variables.
    In production, secrets are loaded from Key Vault into env vars by get_config().

    Args:
        env_var_name: The environment variable name (e.g., "FABRIC_EVENTSTREAM_CONN_STR")
        required: If True, exit with error if not found.

    Returns:
        The secret value as a string.
    """
    # Ensure .env is loaded
    _load_env()

    value = os.getenv(env_var_name)
    if not value and required:
        print(
            f"[CONFIG] CRITICAL: Required secret '{env_var_name}' not found in environment!",
            file=sys.stderr,
        )
        sys.exit(1)
    return value or ""


def get_param(dotted_key: str, default=None):
    """
    Convenience function to get a config parameter using dot notation.

    Example:
        get_param("agents.structuring.amount_threshold")  → 1000000
        get_param("scoring.weights.f1_agent_consensus")    → 35
        get_param("simulator.customer_pool_size")          → 50

    Args:
        dotted_key: Dot-separated path to the config value.
        default: Default value if key not found.

    Returns:
        The config value, or default if not found.
    """
    config = get_config()
    keys = dotted_key.split(".")
    return _deep_get(config, keys, default)


def reload_config():
    """Force reload of configuration (clears cache)."""
    get_config.cache_clear()
    print("[CONFIG] Configuration cache cleared. Next call to get_config() will reload.")


# ─────────────────────────────────────────────────────────────────────────────
# SELF-TEST (run this file directly to verify config loading)
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

    print("=" * 70)
    print("SENTINEL MESH V2 -- Configuration Loader Self-Test")
    print("=" * 70)

    config = get_config()

    # Verify key sections exist
    required_sections = [
        "azure", "fabric", "simulator", "dimensions",
        "scenarios", "scoring", "dna", "agents",
        "sar", "feedback", "monitoring", "schema",
    ]

    print("\n-- Section Verification --")
    all_ok = True
    for section in required_sections:
        if section in config:
            print(f"  [OK] {section}")
        else:
            print(f"  [MISSING] {section}")
            all_ok = False

    # Print some sample values
    print("\n-- Sample Values --")
    samples = [
        ("simulator.customer_pool_size", get_param("simulator.customer_pool_size")),
        ("simulator.streaming_interval_seconds", get_param("simulator.streaming_interval_seconds")),
        ("agents.structuring.amount_threshold", get_param("agents.structuring.amount_threshold")),
        ("scoring.weights.f1_agent_consensus", get_param("scoring.weights.f1_agent_consensus")),
        ("dna.dimensions", get_param("dna.dimensions")),
        ("scenarios.structuring.total_threshold", get_param("scenarios.structuring.total_threshold")),
        ("scoring.tiers.high", get_param("scoring.tiers.high")),
    ]

    for key, value in samples:
        print(f"  {key} = {value}")

    # Check secrets
    print("\n-- Secrets --")
    conn_str = get_secret("FABRIC_EVENTSTREAM_CONN_STR", required=False)
    if conn_str:
        # Only show first/last few chars for security
        masked = conn_str[:20] + "..." + conn_str[-15:]
        print(f"  [OK] FABRIC_EVENTSTREAM_CONN_STR = {masked}")
    else:
        print("  [WARN] FABRIC_EVENTSTREAM_CONN_STR -- not set (needed for streaming mode)")

    # Project paths
    print("\n-- Paths --")
    for path_name, path_value in config["_paths"].items():
        print(f"  {path_name} = {path_value}")

    print("\n" + "=" * 70)
    if all_ok:
        print("[OK] All configuration sections loaded successfully!")
    else:
        print("[FAIL] Some sections are missing -- check config.yaml")
    print("=" * 70)
