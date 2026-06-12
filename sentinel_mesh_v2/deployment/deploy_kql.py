import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
except ImportError:
    print("[ERROR] 'azure-kusto-data' package is required for this deployment script.")
    print("Please install it by running: pip install azure-kusto-data")
    sys.exit(1)


def split_kql_commands(content: str) -> list[str]:
    """
    Parses a KQL file and splits it into individual control commands starting with '.'.
    Handles multiline commands and inline ingestion blocks.
    """
    commands = []
    current_command = []
    lines = content.split("\n")
    
    for line in lines:
        stripped = line.strip()
        # Ignore comments outside of command blocks
        if stripped.startswith("//") and not current_command:
            continue
        
        if stripped.startswith("."):
            if current_command:
                commands.append("\n".join(current_command).strip())
                current_command = []
            current_command.append(line)
        elif current_command:
            current_command.append(line)
            
    if current_command:
        commands.append("\n".join(current_command).strip())
        
    # Return non-empty commands
    return [c for c in commands if c.strip()]


def get_kusto_client(cluster_uri: str) -> KustoClient:
    """
    Creates a KustoClient. Supports both Service Principal credentials
    (GitHub Actions) and Interactive Browser Login (Local run).
    """
    client_id = os.environ.get("AZURE_CLIENT_ID")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET")
    tenant_id = os.environ.get("AZURE_TENANT_ID")

    if client_id and client_secret and tenant_id:
        print("[DEPLOY] Authenticating using Service Principal credentials (CI/CD mode)...")
        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
            cluster_uri, client_id, client_secret, tenant_id
        )
    else:
        print("[DEPLOY] No Service Principal credentials found. Authenticating using Device Login...")
        print("[DEPLOY] Copy the device code printed below and enter it at the Microsoft login link.")
        kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(cluster_uri)
        
    return KustoClient(kcsb)


def main():
    print("=" * 70)
    print("SENTINEL MESH V2 -- Automated Eventhouse/KQL Deployer")
    print("=" * 70)

    # Get connection details
    cluster_uri = os.environ.get("EVENTHOUSE_CLUSTER_URI")
    db_name = os.environ.get("EVENTHOUSE_DATABASE")

    if not cluster_uri or not db_name:
        print("[ERROR] EVENTHOUSE_CLUSTER_URI and EVENTHOUSE_DATABASE environment variables must be set.")
        print("Example:")
        print("  Windows PowerShell: $env:EVENTHOUSE_CLUSTER_URI='https://...'; $env:EVENTHOUSE_DATABASE='...'")
        print("  Linux/Mac/GitHub:   export EVENTHOUSE_CLUSTER_URI='...'; export EVENTHOUSE_DATABASE='...'")
        sys.exit(1)

    # Resolve paths
    project_root = Path(__file__).parent.parent.parent
    kql_dir = project_root / "sentinel_mesh_v2" / "kql"
    upload_ready_dir = project_root / "sentinel_mesh_v2" / "data_simulator" / "upload_ready"

    # Define files in order of deployment
    deployment_steps = [
        # 1. Base DDL and configuration seeds
        ("DDL & Default Config Seeds", kql_dir / "L2_eventhouse_ddl.kql"),
        # 2. Seeding CSV data
        ("Seeding dim_customer", upload_ready_dir / "ingest_dim_customer.kql"),
        ("Seeding dim_account", upload_ready_dir / "ingest_dim_account.kql"),
        ("Seeding dim_merchant", upload_ready_dir / "ingest_dim_merchant.kql"),
        ("Seeding dim_device", upload_ready_dir / "ingest_dim_device.kql"),
        ("Seeding dim_ip_address", upload_ready_dir / "ingest_dim_ip_address.kql"),
        # 3. View, graph and agent analytics logic
        ("DNA & Drift View Functions", kql_dir / "L3_behavioral_dna_view.kql"),
        ("Knowledge Graph Queries", kql_dir / "L4_graph_queries.kql"),
        ("Swarm Agents Detections", kql_dir / "L5_swarm_agents_v2.kql"),
        ("Composite Alerts & Risk Scoring", kql_dir / "L6_risk_scoring_v2.kql"),
    ]

    # Initialize client
    try:
        client = get_kusto_client(cluster_uri)
    except Exception as e:
        print(f"[ERROR] Failed to initialize database client connection: {e}")
        sys.exit(1)

    print(f"\n[DEPLOY] Connected to: {cluster_uri}")
    print(f"[DEPLOY] Target Database: {db_name}\n")

    # Run deployment steps
    for step_name, file_path in deployment_steps:
        if not file_path.exists():
            print(f"[WARN] File not found: {file_path.name}. Skipping step: {step_name}.")
            continue

        print(f"🚀 Deploying: {step_name} ({file_path.name})...")
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        commands = split_kql_commands(content)
        print(f"   Parsed {len(commands)} control commands.")

        for i, cmd in enumerate(commands, start=1):
            cmd_preview = cmd.strip().split("\n")[0][:60]
            try:
                # Kusto control commands (starting with .) must run on execute_mgmt
                client.execute_mgmt(db_name, cmd)
            except Exception as e:
                print(f"\n❌ [FAIL] Error executing command {i}/{len(commands)}: '{cmd_preview}...'")
                print(f"   Details: {e}\n")
                sys.exit(1)

        print(f"   ✅ Done: {step_name}")

    print("\n" + "=" * 70)
    print("🎉 SUCCESS: All KQL schemas, seeds, and agents successfully deployed!")
    print("=" * 70)


if __name__ == "__main__":
    main()
