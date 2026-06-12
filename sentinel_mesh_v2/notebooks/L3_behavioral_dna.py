# ============================================================================
# L3: BEHAVIORAL DNA ENGINE — Profile Snapshot Ingestion
# SENTINEL MESH V2 — Fabric Spark Notebook
# ============================================================================
#
# Purpose:
#   1. Query the real-time KQL function `get_behavioral_dna()`
#   2. Materialize the computed 12D vectors & drift scores
#   3. Save them to the persistent `dim_behavioral_dna` table for L4 Entity Resolution
#
# How to use:
#   - Create a new Notebook in your Fabric Workspace named "L3_Behavioral_DNA"
#   - Copy each cell (separated by # %% markers) into separate notebook cells
#   - Attach to your Lakehouse AND Eventhouse
#   - Schedule as the first step in your Fabric Data Pipeline
# ============================================================================


# %% Cell 1: CONFIGURATION — Read connection settings
# ============================================================================
# These are Fabric notebook parameters — set them in the Pipeline activity
# or in the notebook's parameter cell. NO hardcoded values.
# ============================================================================

# -- Notebook Parameters (set via Data Pipeline or manually) --
EVENTHOUSE_CLUSTER_URI = ""   # e.g. "https://trd-xxxxxxx.z0.kusto.fabric.microsoft.com"
EVENTHOUSE_DATABASE = ""       # e.g. "SentinelMesh_Eventhouse"

# -- Attempt to auto-discover from Fabric environment if not set --
import os

if not EVENTHOUSE_CLUSTER_URI:
    EVENTHOUSE_CLUSTER_URI = os.environ.get("EVENTHOUSE_CLUSTER_URI", "")
if not EVENTHOUSE_DATABASE:
    EVENTHOUSE_DATABASE = os.environ.get("EVENTHOUSE_DATABASE", "")

# Validate
assert EVENTHOUSE_CLUSTER_URI, "ERROR: EVENTHOUSE_CLUSTER_URI must be set"
assert EVENTHOUSE_DATABASE, "ERROR: EVENTHOUSE_DATABASE must be set"

print(f"✅ Eventhouse Cluster: {EVENTHOUSE_CLUSTER_URI}")
print(f"✅ Eventhouse Database: {EVENTHOUSE_DATABASE}")


# %% Cell 2: COMPUTE BEHAVIORAL DNA ON THE FLY
# ============================================================================
# Call the get_behavioral_dna() KQL function which dynamically aggregates 
# daily materialized views and calculates Euclidean Drift.
# ============================================================================

print("🔄 Calling get_behavioral_dna() function in Eventhouse...")

dna_query = "get_behavioral_dna()"

# Load the dynamic results into a Spark DataFrame
dna_df = spark.read \
    .format("com.microsoft.kusto.spark.synapse.datasource") \
    .option("kustoCluster", EVENTHOUSE_CLUSTER_URI) \
    .option("kustoDatabase", EVENTHOUSE_DATABASE) \
    .option("kustoQuery", dna_query) \
    .load()

# Count the active profiles
profile_count = dna_df.count()
print(f"✅ Successfully computed {profile_count} behavioral DNA profiles!")


# %% Cell 3: SAVE SNAPSHOTS TO PERSISTENT TABLE
# ============================================================================
# Append the dynamic profiles to the dim_behavioral_dna table. 
# Each snapshot contains 'computed_at' which tracks profile history over time.
# ============================================================================

print("📥 Appending DNA snapshots to dim_behavioral_dna table...")

dna_df.write \
    .format("com.microsoft.kusto.spark.synapse.datasource") \
    .option("kustoCluster", EVENTHOUSE_CLUSTER_URI) \
    .option("kustoDatabase", EVENTHOUSE_DATABASE) \
    .option("kustoTable", "dim_behavioral_dna") \
    .mode("append") \
    .save()

print("✅ Profiles successfully materialized in dim_behavioral_dna table!")


# %% Cell 4: PRINT SNAPSHOT SAMPLE
# ============================================================================

dna_df.show(10, truncate=False)
print("======================================================================")
print("  L3 Behavioral DNA Computation & Ingestion Complete!")
print("======================================================================")
