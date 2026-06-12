# ============================================================================
# L9: SELF-LEARNING RECALIBRATION ENGINE — Automated Configuration Auto-Tuning
# SENTINEL MESH V2 — Fabric Spark Notebook & Eventhouse Integration
# ============================================================================
#
# Purpose:
#   1. Query analyst feedback from `fact_analyst_dispositions` table.
#   2. Compute precision, True Positive Rate (TPR) and False Positive Rate (FPR)
#      for each detection agent and risk scoring factor.
#   3. Dynamically adjust composite weights:
#      - Consensus, Centrality, DNA Drift, History, Watchlist
#   4. Dynamically adjust agent detection thresholds:
#      - Velocity sigma deviation threshold
#      - Structuring deposit reporting count / amount thresholds
#      - Geo-temporal Travel window thresholds
#   5. Write recalibrated parameters back to `dim_adaptive_config` in Eventhouse.
#
# How to use:
#   - Create a Spark Notebook in Fabric named "L9_Recalibration"
#   - Copy this script into the notebook cells.
#   - Set the runtime parameters (database, cluster, model).
#   - Orchestrate in the Recalibration pipeline or schedule daily.
# ============================================================================


# %% Cell 1: CONFIGURATION & CREDENTIALS
# ============================================================================
# Mark this cell as a "Parameters" cell in Fabric to override these values
# at runtime.
# ============================================================================

import os

EVENTHOUSE_CLUSTER_URI = ""       # e.g., "https://trd-xxxxxxx.z0.kusto.fabric.microsoft.com"
EVENTHOUSE_DATABASE = ""           # e.g., "SentinelMesh_Eventhouse"

# -- Auto-discover from environment if blank --
if not EVENTHOUSE_CLUSTER_URI:
    EVENTHOUSE_CLUSTER_URI = os.environ.get("EVENTHOUSE_CLUSTER_URI", "")
if not EVENTHOUSE_DATABASE:
    EVENTHOUSE_DATABASE = os.environ.get("EVENTHOUSE_DATABASE", "")

assert EVENTHOUSE_CLUSTER_URI, "ERROR: EVENTHOUSE_CLUSTER_URI is required"
assert EVENTHOUSE_DATABASE, "ERROR: EVENTHOUSE_DATABASE is required"

print("✅ Configuration loaded successfully.")


# %% Cell 2: LOAD REVIEWS AND CURRENT CONFIGS
# ============================================================================
# Query KQL database for historical analyst dispositions and current configs.
# ============================================================================

from pyspark.sql import functions as F

print("📥 Reading analyst dispositions and config data from Eventhouse...")

# 1. Load current configs
config_df = spark.read \
    .format("com.microsoft.kusto.spark.synapse.datasource") \
    .option("kustoCluster", EVENTHOUSE_CLUSTER_URI) \
    .option("kustoDatabase", EVENTHOUSE_DATABASE) \
    .option("kustoQuery", "dim_adaptive_config") \
    .load()

current_configs = {row["config_key"]: row["config_value"] for row in config_df.collect()}
print(f"Loaded {len(current_configs)} config keys from dim_adaptive_config.")

# 2. Load dispositions (last 90 days of feedback, joined and expanded to original agent names)
dispositions_query = """
fact_analyst_dispositions
| where timestamp >= ago(90d)
| join kind=inner (
    fact_alerts
    | project alert_id, agent_list = cross_correlations.agent_list
) on alert_id
| mv-expand agent_name = agent_list to typeof(string)
| project disposition_id, alert_id, customer_id, agent_name, disposition, analyst_id, timestamp, notes
"""

dispositions_df = spark.read \
    .format("com.microsoft.kusto.spark.synapse.datasource") \
    .option("kustoCluster", EVENTHOUSE_CLUSTER_URI) \
    .option("kustoDatabase", EVENTHOUSE_DATABASE) \
    .option("kustoQuery", dispositions_query) \
    .load()

dispositions_count = dispositions_df.count()
print(f"Loaded {dispositions_count} analyst dispositions from the last 90 days.")


# %% Cell 3: COMPUTE AGENT PERFORMANCE & ADAPTIVE RULES
# ============================================================================
# Calculate True Positive / False Positive rates and adjust rules thresholds.
# ============================================================================

updated_configs = current_configs.copy()

if dispositions_count == 0:
    print("ℹ️ No feedback dispositions found. Exiting recalibration without changes.")
else:
    # Group by agent name and disposition to calculate performance metrics
    agent_perf = dispositions_df.groupBy("agent_name", "disposition").count().collect()
    
    # Structure metrics per agent
    stats = {}
    for row in agent_perf:
        agent = row["agent_name"]
        disp = row["disposition"]
        cnt = row["count"]
        
        if agent not in stats:
            stats[agent] = {"TP": 0, "FP": 0}
        
        if disp in ("TRUE_POSITIVE", "ESCALATE"):
            stats[agent]["TP"] += cnt
        elif disp == "FALSE_POSITIVE":
            stats[agent]["FP"] += cnt
            
    print("\n📊 Agent Performance Analysis:")
    for agent, m in stats.items():
        total = m["TP"] + m["FP"]
        tp_rate = m["TP"] / total if total > 0 else 0.0
        fp_rate = m["FP"] / total if total > 0 else 0.0
        print(f" - {agent}: TP={m['TP']}, FP={m['FP']} | Precision (TPR)={tp_rate:.2%}, FPR={fp_rate:.2%}")

        # Recalibrate Thresholds based on performance
        # 1. Velocity Agent
        if agent == "Velocity Anomaly Detector" and total >= 5:
            # If False Positive rate exceeds 30%, increase the sigma threshold to reduce noise
            if fp_rate > 0.30:
                old_val = updated_configs.get("agent.velocity.sigma_threshold", 3.0)
                new_val = min(5.0, old_val + 0.5)
                updated_configs["agent.velocity.sigma_threshold"] = new_val
                print(f"   ⚠️ High FP rate detected for Velocity. Increasing sigma threshold: {old_val} -> {new_val}")
            # If False Positive rate is below 10%, we can reduce the threshold to increase sensitivity
            elif fp_rate < 0.10:
                old_val = updated_configs.get("agent.velocity.sigma_threshold", 3.0)
                new_val = max(2.0, old_val - 0.2)
                updated_configs["agent.velocity.sigma_threshold"] = new_val
                print(f"   💡 Excellent precision for Velocity. Reducing sigma threshold to capture more: {old_val} -> {new_val}")
                
        # 2. Structuring Sentinel
        elif agent == "Structuring Sentinel" and total >= 5:
            if fp_rate > 0.25:
                old_count = int(updated_configs.get("agent.structuring.min_txn_count", 3.0))
                new_count = min(6, old_count + 1)
                updated_configs["agent.structuring.min_txn_count"] = float(new_count)
                
                old_amount = updated_configs.get("agent.structuring.amount_threshold", 1000000.0)
                new_amount = old_amount * 1.1
                updated_configs["agent.structuring.amount_threshold"] = new_amount
                print(f"   ⚠️ High FP rate for Structuring. Tightening structuring rules: Min count {old_count} -> {new_count}, Amt threshold {old_amount} -> {new_amount}")
                
        # 3. Geo-Temporal Analyzer
        elif agent == "Geo-Temporal Analyzer" and total >= 5:
            if fp_rate > 0.30:
                old_minutes = updated_configs.get("agent.geo_temporal.max_travel_minutes", 120.0)
                new_minutes = max(60.0, old_minutes - 15.0) # Lower maximum travel minutes reduces geographic alerts
                updated_configs["agent.geo_temporal.max_travel_minutes"] = new_minutes
                print(f"   ⚠️ High FP rate for Geo-Temporal. Reducing max travel corridor time: {old_minutes}m -> {new_minutes}m")


# %% Cell 4: RECALIBRATE COMPOSITE RISK WEIGHTS
# ============================================================================
# Recalibrate the 5 risk factors in accordance with true positive rates.
# ============================================================================

if dispositions_count > 0:
    print("\n⚖️ Recalibrating Composite Risk Factor Weights...")
    
    # Calculate global factor accuracy correlation
    # We assign accuracy scores to the five weights based on disposition matching:
    # Consensus: Avg agent confidence for TP alerts.
    # Centrality: Did PageRank correctly flag mules?
    # DNA Deviation: Correlation of high drift with TPs.
    # History: Did historical alert count correlate with TPs?
    # Watchlist: Watchlist correlation.
    
    # For simplification, we adjust the 5 weights dynamically by scaling their baseline
    # using the true positive rate of the agents (F1) vs structural/metadata factors (F2-F5).
    # If agent consensus (F1) has high FP, we reduce W1 and redistribute to Graph Centrality (W2) and DNA drift (W3).
    
    w1 = updated_configs.get("scoring.weight.f1_agent_consensus", 35.0)
    w2 = updated_configs.get("scoring.weight.f2_graph_centrality", 20.0)
    w3 = updated_configs.get("scoring.weight.f3_dna_deviation", 20.0)
    w4 = updated_configs.get("scoring.weight.f4_historical_risk", 15.0)
    w5 = updated_configs.get("scoring.weight.f5_watchlist_match", 10.0)
    
    # Compute average agent precision (TPR)
    total_tp = sum(m["TP"] for m in stats.values())
    total_count = sum(m["TP"] + m["FP"] for m in stats.values())
    avg_precision = total_tp / total_count if total_count > 0 else 0.70
    
    print(f"Global Agent Precision: {avg_precision:.2%}")
    
    if avg_precision < 0.60:
        # Agents are noisy. Reduce agent consensus weight and increase centrality + DNA
        w1_old = w1
        w1 = max(15.0, w1 - 5.0)
        actual_diff = w1_old - w1
        w2 = min(40.0, w2 + (actual_diff / 2.0))
        w3 = min(40.0, w3 + (actual_diff / 2.0))
        print(f"⚠️ Decreasing Agent Consensus weight due to noise: {w1_old}% -> {w1}%. Increasing structural weights by {actual_diff}%.")
    elif avg_precision > 0.85:
        # Agents are highly accurate. Increase consensus weight.
        w1_old = w1
        w1 = min(50.0, w1 + 5.0)
        actual_diff = w1 - w1_old
        w2 = max(10.0, w2 - (actual_diff / 2.0))
        w3 = max(10.0, w3 - (actual_diff / 2.0))
        print(f"💡 Increasing Agent Consensus weight due to high precision: {w1_old}% -> {w1}%. Decreasing structural weights by {actual_diff}%.")
        
    # Ensure exact sum of 100%
    total_weight = w1 + w2 + w3 + w4 + w5
    w1_norm = round((w1 / total_weight) * 100.0, 1)
    w2_norm = round((w2 / total_weight) * 100.0, 1)
    w3_norm = round((w3 / total_weight) * 100.0, 1)
    w4_norm = round((w4 / total_weight) * 100.0, 1)
    w5_norm = round(100.0 - (w1_norm + w2_norm + w3_norm + w4_norm), 1)
    
    updated_configs["scoring.weight.f1_agent_consensus"] = w1_norm
    updated_configs["scoring.weight.f2_graph_centrality"] = w2_norm
    updated_configs["scoring.weight.f3_dna_deviation"] = w3_norm
    updated_configs["scoring.weight.f4_historical_risk"] = w4_norm
    updated_configs["scoring.weight.f5_watchlist_match"] = w5_norm
    
    print(f"Normalized Weights: W1={w1_norm}%, W2={w2_norm}%, W3={w3_norm}%, W4={w4_norm}%, W5={w5_norm}% (Sum: {w1_norm+w2_norm+w3_norm+w4_norm+w5_norm}%)")


# %% Cell 5: SAVE ADJUSTED CONFIGURATIONS
# ============================================================================
# Prepare the Spark DataFrame and save (overwrite) dim_adaptive_config table.
# ============================================================================

if dispositions_count > 0:
    from datetime import datetime, timezone
    
    # Map back to rows
    recalibrated_rows = []
    
    # Read descriptions and metadata from the original configuration rows
    original_meta = {row["config_key"]: (row["description"]) for row in config_df.collect()}
    
    for key, value in updated_configs.items():
        desc = original_meta.get(key, "Auto-recalibrated configuration parameter")
        recalibrated_rows.append((
            key,
            float(value),
            desc,
            datetime.now(timezone.utc).replace(tzinfo=None),
            "L9_Recalibration_Engine"
        ))
        
    # Define Schema matching KQL table dim_adaptive_config
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
    
    schema = StructType([
        StructField("config_key", StringType(), False),
        StructField("config_value", DoubleType(), False),
        StructField("description", StringType(), True),
        StructField("last_updated", TimestampType(), False),
        StructField("updated_by", StringType(), False)
    ])
    
    new_config_df = spark.createDataFrame(recalibrated_rows, schema)
    
    print("\n📥 Appending recalibrated configs to dim_adaptive_config in Eventhouse...")
    new_config_df.write \
        .format("com.microsoft.kusto.spark.synapse.datasource") \
        .option("kustoCluster", EVENTHOUSE_CLUSTER_URI) \
        .option("kustoDatabase", EVENTHOUSE_DATABASE) \
        .option("kustoTable", "dim_adaptive_config") \
        .mode("append") \
        .save()
        
    print("✅ Configuration recalibration successfully written to Eventhouse database!")
else:
    print("ℹ️ Recalibration finished. No database modifications needed.")
