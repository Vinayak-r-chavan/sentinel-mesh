# ============================================================================
# L7: AI SAR GENERATOR — Automated Suspicious Activity Reporting
# SENTINEL MESH V2 — Fabric Spark Notebook & Azure OpenAI Integration
# ============================================================================
#
# Purpose:
#   1. Query KQL database for active HIGH/CRITICAL alerts that do not have a SAR
#   2. Pull customer profile, triggered agents, reasons, and DNA drift evidence
#   3. Call Azure OpenAI to generate structured case narratives & SAR sections
#   4. Write generated reports back to `fact_sar_reports` table in Eventhouse
#
# How to use:
#   - Create a new Notebook in your Fabric Workspace named "L7_SAR_Generator"
#   - Copy each cell (separated by # %% markers) into separate notebook cells
#   - Attach to your Lakehouse AND Eventhouse
#   - Schedule as the final step in your Fabric Data Pipeline
# ============================================================================


# %% Cell 0: INSTALL DEPENDENCIES — Programmatic installation for pipeline runs
# ============================================================================
import subprocess
import sys

try:
    import openai
except ImportError:
    print("Installing openai dependency...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "openai"])
    print("openai installed successfully.")



# %% Cell 1: CONFIGURATION & CREDENTIALS
# ============================================================================
# Mark this cell as a "Parameters" cell in Fabric to override these values
# at runtime via Data Pipeline or Key Vault secrets.
# ============================================================================

# -- Connection Parameters --
EVENTHOUSE_CLUSTER_URI = ""       # e.g., "https://trd-xxxxxxx.z0.kusto.fabric.microsoft.com"
EVENTHOUSE_DATABASE = ""           # e.g., "SentinelMesh_Eventhouse"

# -- Azure OpenAI Credentials --
AZURE_OPENAI_ENDPOINT = ""         # e.g., "https://your-resource.openai.azure.com/"
AZURE_OPENAI_API_KEY = ""          # API Key for access
AZURE_OPENAI_DEPLOYMENT_NAME = ""  # The deployment name for your GPT-4 model

# -- Attempt to auto-discover from environment if blank --
import os

if not EVENTHOUSE_CLUSTER_URI:
    EVENTHOUSE_CLUSTER_URI = os.environ.get("EVENTHOUSE_CLUSTER_URI", "")
if not EVENTHOUSE_DATABASE:
    EVENTHOUSE_DATABASE = os.environ.get("EVENTHOUSE_DATABASE", "")
if not AZURE_OPENAI_ENDPOINT:
    AZURE_OPENAI_ENDPOINT = os.environ.get("AZURE_OPENAI_ENDPOINT", "")
if not AZURE_OPENAI_API_KEY:
    AZURE_OPENAI_API_KEY = os.environ.get("AZURE_OPENAI_API_KEY", "")
if not AZURE_OPENAI_DEPLOYMENT_NAME:
    AZURE_OPENAI_DEPLOYMENT_NAME = os.environ.get("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4")

# Validate required variables
assert EVENTHOUSE_CLUSTER_URI, "ERROR: EVENTHOUSE_CLUSTER_URI is required"
assert EVENTHOUSE_DATABASE, "ERROR: EVENTHOUSE_DATABASE is required"
assert AZURE_OPENAI_ENDPOINT, "ERROR: AZURE_OPENAI_ENDPOINT is required"
assert AZURE_OPENAI_API_KEY, "ERROR: AZURE_OPENAI_API_KEY is required"
assert AZURE_OPENAI_DEPLOYMENT_NAME, "ERROR: AZURE_OPENAI_DEPLOYMENT_NAME is required"

print("✅ Configuration and Credentials loaded successfully!")


# %% Cell 2: QUERY UNPROCESSED HIGH-RISK ALERTS
# ============================================================================
# Fetch all customers with HIGH or CRITICAL risk alerts who do not yet have 
# a generated Suspicious Activity Report (leftanti join in KQL).
# ============================================================================

query = """
fact_alerts
| where risk_tier in ("HIGH", "CRITICAL")
| join kind=leftanti (fact_sar_reports) on customer_id
| summarize arg_max(triggered_at, *) by customer_id
| project 
    alert_id, 
    customer_id, 
    customer_name, 
    agent_name, 
    risk_score, 
    risk_tier, 
    reason, 
    cross_correlations
"""

alerts_df = spark.read \
    .format("com.microsoft.kusto.spark.synapse.datasource") \
    .option("kustoCluster", EVENTHOUSE_CLUSTER_URI) \
    .option("kustoDatabase", EVENTHOUSE_DATABASE) \
    .option("kustoQuery", query) \
    .load()

unprocessed_alerts = alerts_df.collect()
print(f"🔍 Found {len(unprocessed_alerts)} unprocessed high-risk customers requiring a SAR.")


# %% Cell 3: COMPILING SAR NARRATIVES VIA AZURE OpenAI
# ============================================================================
# Loop through each alert, construct the context, and call Azure OpenAI
# using structured prompting to generate compliance reports.
# ============================================================================

import json
from datetime import datetime
from openai import AzureOpenAI

# Initialize OpenAI client
client = AzureOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_API_KEY,
    api_version="2024-02-15-preview"
)

sar_reports = []

for alert in unprocessed_alerts:
    customer_id = alert["customer_id"]
    customer_name = alert["customer_name"]
    risk_score = alert["risk_score"]
    risk_tier = alert["risk_tier"]
    reason = alert["reason"]
    
    # Parse the metadata correlations JSON
    correlations = json.loads(alert["cross_correlations"])
    agent_list = correlations.get("agent_list", [])
    pagerank = correlations.get("pagerank_score", 0.0)
    drift = correlations.get("dna_drift_score", 0.0)
    pep_match = correlations.get("pep_watchlist_match", False)

    print(f"📝 Generating SAR for Customer: {customer_name} ({customer_id})...")
    system_prompt = (
        "You are an expert AML Compliance Officer and Forensic Auditor. "
        "Your task is to write a highly detailed, professional Suspicious Activity Report (SAR) "
        "for financial intelligence units (FIU). Use formal banking terminology."
    )

    user_prompt = f"""
    Generate a Suspicious Activity Report (SAR) for the following customer alert profile:
    
    - Customer ID: {customer_id}
    - Customer Name: {customer_name}
    - Calculated Risk Score: {risk_score}/100 ({risk_tier} Risk)
    - Triggered Agents: {', '.join(agent_list)}
    - Primary Trigger Details: {reason}
    
    --- Supporting Evidence ---
    - PageRank Centrality: {pagerank} (centrality score in money flows)
    - Behavioral DNA Drift: {drift} sigma deviation from baseline DNA
    - PEP/Sanctions Watchlist Match: {pep_match}
    
    Please output your response strictly as a JSON object with the following keys. 
    Ensure no markdown packaging, just raw JSON:
    {{
        "executive_summary": "A 2-3 sentence summary of the case and why it is suspicious.",
        "suspicious_activity": "A comprehensive analysis of the transaction structuring, loop transfers, or shared infrastructure, detailing the indicators of crime.",
        "red_flags": ["A list of key red flags observed (e.g. Structuring, Loop Funds, PEP Match)"],
        "recommended_actions": ["A list of specific recommended actions (e.g. freeze account, escalate to FIU)"],
        "risk_assessment": "Assessment of the threat level, likelihood of money laundering, or terrorist financing, and systemic risk."
    }}
    """

    try:
        response = client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.2,
            response_format={"type": "json_object"}
        )
        
        # Parse output JSON
        output_content = response.choices[0].message.content
        report_data = json.loads(output_content)

        import hashlib
        prompt_hash = hashlib.md5(user_prompt.encode("utf-8")).hexdigest()
        sar_id = f"SAR-{int(datetime.utcnow().timestamp())}-{customer_id}"
        generated_at = datetime.utcnow()

        sar_reports.append((
            sar_id,
            customer_id,
            customer_name,
            alert.get("alert_id", ""),
            report_data.get("executive_summary", ""),
            report_data.get("suspicious_activity", ""),
            json.dumps(report_data.get("red_flags", [])),
            json.dumps(report_data.get("recommended_actions", [])),
            report_data.get("risk_assessment", ""),
            float(risk_score),
            AZURE_OPENAI_DEPLOYMENT_NAME,
            prompt_hash,
            generated_at,
            False,
            0.0
        ))
        print(f"✅ SAR generated successfully for {customer_name}.")

    except Exception as e:
        print(f"❌ Failed to generate SAR for {customer_name}. Error: {str(e)}")

# Create Spark DataFrame if reports were generated
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType

if sar_reports:
    schema = StructType([
        StructField("sar_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("case_id", StringType(), True),
        StructField("executive_summary", StringType(), True),
        StructField("suspicious_activity", StringType(), True),
        StructField("red_flags", StringType(), True),
        StructField("recommended_actions", StringType(), True),
        StructField("risk_assessment", StringType(), True),
        StructField("risk_score", DoubleType(), True),
        StructField("model_version", StringType(), True),
        StructField("prompt_hash", StringType(), True),
        StructField("generated_at", TimestampType(), True),
        StructField("validated", BooleanType(), True),
        StructField("content_safety_score", DoubleType(), True)
    ])
    
    sar_df = spark.createDataFrame(sar_reports, schema)
    print(f"✅ Prepared {sar_df.count()} new SAR reports for insertion.")
else:
    print("ℹ️ No new reports generated.")


# %% Cell 4: WRITE REPORTS TO EVENTHOUSE
# ============================================================================
# Append generated SARs to fact_sar_reports.
# ============================================================================

if sar_reports:
    print("📥 Appending reports to fact_sar_reports...")
    
    sar_df.write \
        .format("com.microsoft.kusto.spark.synapse.datasource") \
        .option("kustoCluster", EVENTHOUSE_CLUSTER_URI) \
        .option("kustoDatabase", EVENTHOUSE_DATABASE) \
        .option("kustoTable", "fact_sar_reports") \
        .mode("append") \
        .save()
        
    print("✅ SAR reports successfully written to Eventhouse!")
else:
    print("ℹ️ Script finished with no database writes needed.")
