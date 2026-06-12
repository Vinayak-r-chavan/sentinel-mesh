# ============================================================================
# L4: ENTITY RESOLUTION — Fuzzy Matching + Shared Infra Analysis
# SENTINEL MESH V2 — Fabric Spark Notebook
# ============================================================================
#
# Purpose:
#   1. Shared Infrastructure Resolution — Link customers sharing devices/IPs.
#   2. Fuzzy Name Matching — Link customers with high name similarity.
#   3. Behavioral DNA Similarity — Compute Cosine Similarity of 12D DNA vectors.
#
# How to use:
#   - Create a new Notebook in your Fabric Workspace named "L4_Entity_Resolution"
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


# %% Cell 2: LOAD CONFIG — Read parameters from dim_adaptive_config
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

# Extract resolution-specific parameters with safe defaults
FUZZY_NAME_THRESHOLD = CONFIG.get("entity.resolution.name_threshold", 0.85)
DNA_SIMILARITY_THRESHOLD = CONFIG.get("entity.resolution.dna_threshold", 0.90)
MIN_SHARED_INFRA = int(CONFIG.get("entity.resolution.min_shared_infra", 1))

print(f"✅ Configuration Loaded:")
print(f"   Fuzzy Name Threshold: {FUZZY_NAME_THRESHOLD:.2f}")
print(f"   DNA Similarity Threshold: {DNA_SIMILARITY_THRESHOLD:.2f}")
print(f"   Min Shared Infra Count: {MIN_SHARED_INFRA}")


# %% Cell 3: LOAD CUSTOMERS & SHADOW ENTITIES
# ============================================================================

# Load customer master
customer_query = "dim_customer | project customer_id, name, city"
customer_df = spark.read \
    .format("com.microsoft.kusto.spark.synapse.datasource") \
    .option("kustoCluster", EVENTHOUSE_CLUSTER_URI) \
    .option("kustoDatabase", EVENTHOUSE_DATABASE) \
    .option("kustoQuery", customer_query) \
    .load()

# Load device associations
device_query = "dim_device | project device_id, customer_ids"
device_df = spark.read \
    .format("com.microsoft.kusto.spark.synapse.datasource") \
    .option("kustoCluster", EVENTHOUSE_CLUSTER_URI) \
    .option("kustoDatabase", EVENTHOUSE_DATABASE) \
    .option("kustoQuery", device_query) \
    .load()

# Load IP associations
ip_query = "dim_ip_address | project ip_address, customer_ids"
ip_df = spark.read \
    .format("com.microsoft.kusto.spark.synapse.datasource") \
    .option("kustoCluster", EVENTHOUSE_CLUSTER_URI) \
    .option("kustoDatabase", EVENTHOUSE_DATABASE) \
    .option("kustoQuery", ip_query) \
    .load()

# Print counts
print(f"✅ Loaded {customer_df.count()} customer records")
print(f"✅ Loaded {device_df.count()} device profiles")
print(f"✅ Loaded {ip_df.count()} IP profiles")


# %% Cell 4: COMPUTE SHARED DEVICE & IP LINKS
# ============================================================================

from pyspark.sql import functions as F

# 1. Parse and explode customer_ids arrays for devices
# KQL dynamic types might come as JSON strings or raw arrays, handle them robustly.
device_exploded = device_df \
    .withColumn("cust_id", F.explode(F.from_json(F.col("customer_ids"), "array<string>"))) \
    .select("device_id", "cust_id")

# Self-join to find customer pairs sharing devices
shared_devices_df = device_exploded.alias("d1") \
    .join(device_exploded.alias("d2"), "device_id") \
    .filter(F.col("d1.cust_id") < F.col("d2.cust_id")) \
    .groupBy(F.col("d1.cust_id").alias("cust_a"), F.col("d2.cust_id").alias("cust_b")) \
    .agg(F.collect_set("device_id").alias("shared_devices"))

# 2. Explode customer_ids for IPs
ip_exploded = ip_df \
    .withColumn("cust_id", F.explode(F.from_json(F.col("customer_ids"), "array<string>"))) \
    .select("ip_address", "cust_id")

# Self-join to find customer pairs sharing IPs
shared_ips_df = ip_exploded.alias("i1") \
    .join(ip_exploded.alias("i2"), "ip_address") \
    .filter(F.col("i1.cust_id") < F.col("i2.cust_id")) \
    .groupBy(F.col("i1.cust_id").alias("cust_a"), F.col("i2.cust_id").alias("cust_b")) \
    .agg(F.collect_set("ip_address").alias("shared_ips"))

# Combine device and IP shares
shared_infra_df = shared_devices_df.alias("dev") \
    .join(shared_ips_df.alias("ip"), ["cust_a", "cust_b"], "outer") \
    .select(
        F.col("cust_a"),
        F.col("cust_b"),
        F.coalesce(F.col("dev.shared_devices"), F.array()).alias("shared_devices"),
        F.coalesce(F.col("ip.shared_ips"), F.array()).alias("shared_ips")
    ) \
    .withColumn("device_count", F.size(F.col("shared_devices"))) \
    .withColumn("ip_count", F.size(F.col("shared_ips"))) \
    .withColumn("total_shared_infra", F.col("device_count") + F.col("ip_count")) \
    .filter(F.col("total_shared_infra") >= MIN_SHARED_INFRA)

print(f"✅ Found {shared_infra_df.count()} customer pairs sharing infrastructure")


# %% Cell 5: FUZZY NAME MATCHING & BEHAVIORAL DNA CORRELATION
# ============================================================================

# Load latest behavioral DNA for each customer
dna_query = """
dim_behavioral_dna
| summarize arg_max(computed_at, *) by customer_id
"""
dna_df = spark.read \
    .format("com.microsoft.kusto.spark.synapse.datasource") \
    .option("kustoCluster", EVENTHOUSE_CLUSTER_URI) \
    .option("kustoDatabase", EVENTHOUSE_DATABASE) \
    .option("kustoQuery", dna_query) \
    .load()

# Prepare customer metadata joins
cust_meta_a = customer_df.alias("c_a").select(
    F.col("c_a.customer_id").alias("cust_a"),
    F.col("c_a.name").alias("name_a"),
    F.col("c_a.city").alias("city_a")
)
cust_meta_b = customer_df.alias("c_b").select(
    F.col("c_b.customer_id").alias("cust_b"),
    F.col("c_b.name").alias("name_b"),
    F.col("c_b.city").alias("city_b")
)

# 1. Join infrastructure links with customer details
candidate_pairs = shared_infra_df \
    .join(cust_meta_a, "cust_a") \
    .join(cust_meta_b, "cust_b")

# Also add fuzzy name match candidates (same city + similar names, even without shared infra)
# We do this to capture identity theft / synthetic identity rings.
fuzzy_name_candidates = customer_df.alias("c1") \
    .join(customer_df.alias("c2"), F.col("c1.city") == F.col("c2.city")) \
    .filter(F.col("c1.customer_id") < F.col("c2.customer_id")) \
    .select(
        F.col("c1.customer_id").alias("cust_a"),
        F.col("c2.customer_id").alias("cust_b"),
        F.array().alias("shared_devices"),
        F.array().alias("shared_ips"),
        F.lit(0).alias("device_count"),
        F.lit(0).alias("ip_count"),
        F.lit(0).alias("total_shared_infra"),
        F.col("c1.name").alias("name_a"),
        F.col("c1.city").alias("city_a"),
        F.col("c2.name").alias("name_b"),
        F.col("c2.city").alias("city_b")
    )

# Combine the candidate pools
all_candidates = candidate_pairs.unionByName(fuzzy_name_candidates).distinct()

# Compute string similarity using Spark native Levenshtein distance
all_candidates = all_candidates \
    .withColumn("len_a", F.length(F.col("name_a"))) \
    .withColumn("len_b", F.length(F.col("name_b"))) \
    .withColumn("max_len", F.greatest(F.col("len_a"), F.col("len_b"))) \
    .withColumn("lev_dist", F.levenshtein(F.col("name_a"), F.col("name_b"))) \
    .withColumn("name_similarity", F.round(1.0 - (F.col("lev_dist") / F.col("max_len")), 4))

# 2. Behavioral DNA vectors calculations (Cosine Similarity)
# Select vector dimensions
dna_cols = [
    "velocity", "amount_profile", "temporal_pattern", "counterparty_diversity",
    "channel_mix", "geographic_spread", "amount_entropy", "round_number_ratio",
    "counterparty_recurrence", "dormancy_burst", "cross_border_ratio", "device_switching"
]

dna_a = dna_df.alias("d_a").select(
    F.col("d_a.customer_id").alias("cust_a"),
    *[F.col(f"d_a.{col}").alias(f"{col}_a") for col in dna_cols]
)
dna_b = dna_df.alias("d_b").select(
    F.col("d_b.customer_id").alias("cust_b"),
    *[F.col(f"d_b.{col}").alias(f"{col}_b") for col in dna_cols]
)

# Join DNA features
all_candidates = all_candidates \
    .join(dna_a, "cust_a", "left") \
    .join(dna_b, "cust_b", "left")

# Calculate dot product and norms for cosine similarity
dot_product = F.lit(0.0)
norm_a_sq = F.lit(0.0)
norm_b_sq = F.lit(0.0)

for col in dna_cols:
    val_a = F.coalesce(F.col(f"{col}_a"), F.lit(0.0))
    val_b = F.coalesce(F.col(f"{col}_b"), F.lit(0.0))
    dot_product += (val_a * val_b)
    norm_a_sq += (val_a * val_a)
    norm_b_sq += (val_b * val_b)

all_candidates = all_candidates \
    .withColumn("dot_product", dot_product) \
    .withColumn("norm_a", F.sqrt(norm_a_sq)) \
    .withColumn("norm_b", F.sqrt(norm_b_sq)) \
    .withColumn(
        "dna_similarity",
        F.round(
            F.coalesce(F.col("dot_product") / (F.col("norm_a") * F.col("norm_b")), F.lit(0.0)),
            4
        )
    )

print(f"✅ Similarity analysis computed for {all_candidates.count()} candidates")


# %% Cell 6: RESOLVE ENTITIES + DETERMINE MATCH TYPE
# ============================================================================

from datetime import datetime

# Determine match type and filter based on thresholds
resolved_df = all_candidates \
    .withColumn(
        "match_type",
        F.when(
            (F.col("name_similarity") >= FUZZY_NAME_THRESHOLD) & (F.col("total_shared_infra") > 0),
            F.lit("IDENTITY_THEFT_SUSPECT")
        ).when(
            (F.col("name_similarity") >= FUZZY_NAME_THRESHOLD),
            F.lit("FUZZY_NAME_MATCH")
        ).when(
            (F.col("device_count") > 0) & (F.col("ip_count") > 0),
            F.lit("SHARED_DEVICE_AND_IP")
        ).when(
            (F.col("device_count") > 0),
            F.lit("SHARED_DEVICE")
        ).when(
            (F.col("ip_count") > 0),
            F.lit("SHARED_IP")
        ).when(
            (F.col("dna_similarity") >= DNA_SIMILARITY_THRESHOLD),
            F.lit("BEHAVIORAL_CLONE")
        ).otherwise(F.lit("NO_MATCH"))
    ) \
    .filter(F.col("match_type") != "NO_MATCH") \
    .withColumn(
        "match_score",
        F.round(
            F.greatest(
                F.col("name_similarity"),
                F.coalesce(F.col("dna_similarity"), F.lit(0.0)),
                F.least(F.col("total_shared_infra").cast("double") / 5.0, F.lit(1.0))
            ),
            4
        )
    ) \
    .withColumn("resolved_at", F.current_timestamp()) \
    .withColumn("status", F.lit("RESOLVED")) \
    .select(
        F.col("cust_a").alias("primary_customer_id"),
        F.col("name_a").alias("primary_customer_name"),
        F.col("cust_b").alias("linked_customer_id"),
        F.col("name_b").alias("linked_customer_name"),
        F.col("match_type"),
        F.col("match_score"),
        F.to_json(F.col("shared_devices")).alias("shared_devices"),
        F.to_json(F.col("shared_ips")).alias("shared_ips"),
        F.col("dna_similarity"),
        F.col("resolved_at"),
        F.col("status")
    )

print(f"✅ Resolved {resolved_df.count()} matching customer entity links")
resolved_df.show(10, truncate=False)


# %% Cell 7: WRITE RESOLVED ENTITIES TO EVENTHOUSE
# ============================================================================

# Write output back to Eventhouse fact_entity_resolution
resolved_df.write \
    .format("com.microsoft.kusto.spark.synapse.datasource") \
    .option("kustoCluster", EVENTHOUSE_CLUSTER_URI) \
    .option("kustoDatabase", EVENTHOUSE_DATABASE) \
    .option("kustoTable", "fact_entity_resolution") \
    .mode("append") \
    .save()

print("✅ Resolved entities written to fact_entity_resolution table in Eventhouse")


# %% Cell 8: PRINT RUN SUMMARY
# ============================================================================

match_counts = resolved_df.groupBy("match_type").count().collect()

print("=" * 70)
print("  SENTINEL MESH L4 — Entity Resolution Complete")
print("=" * 70)
print(f"  Fuzzy Name Threshold:    {FUZZY_NAME_THRESHOLD:.2f}")
print(f"  DNA Similarity Threshold: {DNA_SIMILARITY_THRESHOLD:.2f}")
print(f"  Min Shared Infra:        {MIN_SHARED_INFRA}")
print("-" * 70)
print("  Breakdown by Match Type:")
for row in match_counts:
    print(f"    - {row['match_type']}: {row['count']} pairs")
print("=" * 70)
