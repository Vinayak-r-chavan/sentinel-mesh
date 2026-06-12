# ============================================================================
# L4: GRAPH ANALYSIS — Community Detection + PageRank
# SENTINEL MESH V2 — Fabric Spark Notebook
# ============================================================================
#
# Purpose:
#   1. Louvain Community Detection — discover fraud rings (tightly connected clusters)
#   2. PageRank Centrality — identify money mule hub accounts
#
# How to use:
#   - Create a new Notebook in your Fabric Workspace
#   - Copy each cell (separated by # %% markers) into separate notebook cells
#   - Attach to your Lakehouse AND Eventhouse
#   - Run all cells, or schedule via Data Pipeline
#
# Config-driven: ALL parameters read from dim_adaptive_config (zero hardcoding)
# ============================================================================


# %% Cell 1: CONFIGURATION — Read connection settings from Fabric environment
# ============================================================================
# These are Fabric notebook parameters — set them in the Pipeline activity
# or in the notebook's parameter cell. NO hardcoded values.
# ============================================================================

# -- Notebook Parameters (set via Data Pipeline or manually) --
# In Fabric, mark this cell as a "Parameters" cell so the Pipeline can override these.
EVENTHOUSE_CLUSTER_URI = ""   # e.g. "https://trd-xxxxxxx.z0.kusto.fabric.microsoft.com"
EVENTHOUSE_DATABASE = ""       # e.g. "SentinelMesh_Eventhouse"

# -- Attempt to auto-discover from Fabric environment if not set --
import os

if not EVENTHOUSE_CLUSTER_URI:
    EVENTHOUSE_CLUSTER_URI = os.environ.get("EVENTHOUSE_CLUSTER_URI", "")
if not EVENTHOUSE_DATABASE:
    EVENTHOUSE_DATABASE = os.environ.get("EVENTHOUSE_DATABASE", "")

# Validate
assert EVENTHOUSE_CLUSTER_URI, "ERROR: EVENTHOUSE_CLUSTER_URI must be set (via Pipeline parameter or environment variable)"
assert EVENTHOUSE_DATABASE, "ERROR: EVENTHOUSE_DATABASE must be set (via Pipeline parameter or environment variable)"

print(f"✅ Eventhouse Cluster: {EVENTHOUSE_CLUSTER_URI}")
print(f"✅ Eventhouse Database: {EVENTHOUSE_DATABASE}")


# %% Cell 2: LOAD CONFIG — Read algorithm parameters from dim_adaptive_config
# ============================================================================

# Read all config keys from the adaptive config table
config_query = "dim_adaptive_config | project config_key, config_value"

config_df = spark.read \
    .format("com.microsoft.kusto.spark.synapse.datasource") \
    .option("kustoCluster", EVENTHOUSE_CLUSTER_URI) \
    .option("kustoDatabase", EVENTHOUSE_DATABASE) \
    .option("kustoQuery", config_query) \
    .load()

# Convert to a Python dictionary for easy lookup
config_rows = config_df.collect()
CONFIG = {row["config_key"]: float(row["config_value"]) for row in config_rows}

# Extract graph-specific parameters with safe defaults
COMMUNITY_MIN_SIZE = int(CONFIG.get("graph.community.min_size", 3))
PAGERANK_DAMPING = CONFIG.get("graph.pagerank.damping_factor", 0.85)
PAGERANK_ITERATIONS = int(CONFIG.get("graph.pagerank.max_iterations", 100))
HUB_PERCENTILE = CONFIG.get("graph.hub.percentile_threshold", 90)

print(f"✅ Config loaded: {len(CONFIG)} parameters")
print(f"   Community min size: {COMMUNITY_MIN_SIZE}")
print(f"   PageRank damping: {PAGERANK_DAMPING}")
print(f"   PageRank iterations: {PAGERANK_ITERATIONS}")
print(f"   Hub percentile threshold: {HUB_PERCENTILE}")


# %% Cell 3: LOAD TRANSACTION DATA — Build edge list from Eventhouse
# ============================================================================

# Load transaction edges (account → counterparty)
edges_query = """
fact_transactions 
| where counterparty_account != account_id and counterparty_account !startswith "ACC-MERC"
| summarize 
    tx_count = count(), 
    total_amount = sum(amount),
    latest_tx = max(timestamp)
    by source_account = account_id, target_account = counterparty_account
"""

edges_df = spark.read \
    .format("com.microsoft.kusto.spark.synapse.datasource") \
    .option("kustoCluster", EVENTHOUSE_CLUSTER_URI) \
    .option("kustoDatabase", EVENTHOUSE_DATABASE) \
    .option("kustoQuery", edges_query) \
    .load()

# Load account-to-customer mapping
mapping_query = """
fact_transactions 
| summarize customer_id = take_any(customer_id) by account_id
"""

mapping_df = spark.read \
    .format("com.microsoft.kusto.spark.synapse.datasource") \
    .option("kustoCluster", EVENTHOUSE_CLUSTER_URI) \
    .option("kustoDatabase", EVENTHOUSE_DATABASE) \
    .option("kustoQuery", mapping_query) \
    .load()

# Convert to Python lists for networkx
edges_list = edges_df.collect()
mapping_list = mapping_df.collect()

account_to_customer = {row["account_id"]: row["customer_id"] for row in mapping_list}

print(f"✅ Loaded {len(edges_list)} edges (account→account)")
print(f"✅ Loaded {len(account_to_customer)} account→customer mappings")


# %% Cell 4: BUILD GRAPH + RUN LOUVAIN COMMUNITY DETECTION
# ============================================================================

import networkx as nx
from networkx.algorithms.community import louvain_communities
from datetime import datetime, timezone

# Build directed graph
G = nx.DiGraph()

for row in edges_list:
    src = row["source_account"]
    tgt = row["target_account"]
    if src == tgt or tgt.startswith("ACC-MERC") or tgt == "ACC-SELF" or tgt == "ACC-MERC-NONE":
        continue
    G.add_edge(
        src, 
        tgt, 
        weight=float(row["total_amount"]),
        tx_count=int(row["tx_count"])
    )

print(f"✅ Graph built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

# Louvain requires undirected graph — create undirected copy for community detection
G_undirected = G.to_undirected()

# Run Louvain community detection
# seed parameter ensures reproducibility
communities = louvain_communities(G_undirected, weight="weight", seed=42)

# Build community assignments
community_map = {}
for community_id, members in enumerate(communities):
    for node in members:
        community_map[node] = {
            "community_id": community_id,
            "community_size": len(members)
        }

print(f"✅ Louvain detected {len(communities)} communities")
for i, c in enumerate(communities):
    if len(c) >= COMMUNITY_MIN_SIZE:
        print(f"   Community {i}: {len(c)} accounts (potential fraud ring)")


# %% Cell 5: RUN PAGERANK CENTRALITY
# ============================================================================

import numpy as np

# Run PageRank on the directed graph (money flow direction matters)
pagerank_scores = nx.pagerank(
    G, 
    alpha=PAGERANK_DAMPING, 
    max_iter=int(PAGERANK_ITERATIONS), 
    weight="weight"
)

# Calculate in-degree and out-degree for each node
in_degrees = dict(G.in_degree())
out_degrees = dict(G.out_degree())

# Determine hub threshold from config (percentile-based, not hardcoded)
scores_array = np.array(list(pagerank_scores.values()))
hub_threshold = np.percentile(scores_array, HUB_PERCENTILE)

print(f"✅ PageRank computed for {len(pagerank_scores)} nodes")
print(f"   Hub threshold (P{int(HUB_PERCENTILE)}): {hub_threshold:.6f}")

# Count hubs
hub_count = sum(1 for score in pagerank_scores.values() if score >= hub_threshold)
print(f"   Identified {hub_count} hub accounts (potential money mules)")


# %% Cell 6: COMBINE RESULTS + WRITE TO EVENTHOUSE
# ============================================================================

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType

# Build result rows
computed_at = datetime.now(timezone.utc).replace(tzinfo=None)
results = []

for account_id in G.nodes():
    customer_id = account_to_customer.get(account_id, "UNKNOWN")
    comm = community_map.get(account_id, {"community_id": -1, "community_size": 1})
    pr_score = pagerank_scores.get(account_id, 0.0)
    in_deg = in_degrees.get(account_id, 0)
    out_deg = out_degrees.get(account_id, 0)
    is_hub = bool(pr_score >= hub_threshold)

    results.append((
        customer_id,
        account_id,
        comm["community_id"],
        comm["community_size"],
        round(float(pr_score), 8),
        in_deg,
        out_deg,
        is_hub,
        computed_at
    ))

# Create Spark DataFrame
schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("community_id", IntegerType(), True),
    StructField("community_size", IntegerType(), True),
    StructField("pagerank_score", DoubleType(), True),
    StructField("in_degree", IntegerType(), True),
    StructField("out_degree", IntegerType(), True),
    StructField("is_hub", BooleanType(), True),
    StructField("computed_at", TimestampType(), True)
])

results_df = spark.createDataFrame(results, schema)

print(f"✅ Result DataFrame: {results_df.count()} rows")
results_df.show(10, truncate=False)

# Write to Eventhouse — replaces previous results (latest snapshot only)
results_df.write \
    .format("com.microsoft.kusto.spark.synapse.datasource") \
    .option("kustoCluster", EVENTHOUSE_CLUSTER_URI) \
    .option("kustoDatabase", EVENTHOUSE_DATABASE) \
    .option("kustoTable", "fact_graph_communities") \
    .mode("append") \
    .save()

print("✅ Results written to fact_graph_communities table in Eventhouse")


# %% Cell 7: SUMMARY — Print detection summary
# ============================================================================

print("=" * 70)
print("  SENTINEL MESH L4 — Graph Analysis Complete")
print("=" * 70)
print(f"  Nodes analyzed:        {G.number_of_nodes()}")
print(f"  Edges analyzed:        {G.number_of_edges()}")
print(f"  Communities detected:  {len(communities)}")
print(f"  Fraud ring candidates: {sum(1 for c in communities if len(c) >= COMMUNITY_MIN_SIZE)}")
print(f"     (clusters with >= {COMMUNITY_MIN_SIZE} accounts)")
print(f"  Hub accounts (mules): {hub_count}")
print(f"     (PageRank >= P{int(HUB_PERCENTILE)} threshold: {hub_threshold:.6f})")
print(f"  Config source:         dim_adaptive_config ({len(CONFIG)} params)")
print(f"  Output table:          fact_graph_communities")
print("=" * 70)
