# ============================================================================
# L7: AML GPT — Interactive Natural Language Query Agent
# SENTINEL MESH V2 — Terminal-Based Conversational compliance Agent
# ============================================================================
#
# Purpose:
#   1. Provide a natural language query interface for compliance analysts.
#   2. Translate English questions into KQL queries dynamically via Azure OpenAI.
#   3. Query the hot-path Eventhouse database directly from the local terminal.
#   4. Synthesize human-friendly analysis summaries of the query outputs.

# ============================================================================

import os
import sys
import json
import warnings
from datetime import datetime

# Suppress warnings from Azure Core / Kusto SDK
warnings.filterwarnings("ignore", category=UserWarning)

# --- Default Connection Settings (Sourced from Fabric parameters) ---
EVENTHOUSE_CLUSTER_URI = "https://trd-9xcasgtnw87tra1q6w.z6.kusto.fabric.microsoft.com"
EVENTHOUSE_DATABASE = "SentinelMesh_Eventhouse"

# --- Azure OpenAI Settings ---
AZURE_OPENAI_ENDPOINT = "https://odl-user-2255376-2737-resource.openai.azure.com/"
AZURE_OPENAI_API_KEY = ""
AZURE_OPENAI_DEPLOYMENT_NAME = "gpt-4o"

# Overrides via Environment Variables if set
EVENTHOUSE_CLUSTER_URI = os.environ.get("EVENTHOUSE_CLUSTER_URI", EVENTHOUSE_CLUSTER_URI)
EVENTHOUSE_DATABASE = os.environ.get("EVENTHOUSE_DATABASE", EVENTHOUSE_DATABASE)
AZURE_OPENAI_ENDPOINT = os.environ.get("AZURE_OPENAI_ENDPOINT", AZURE_OPENAI_ENDPOINT)
AZURE_OPENAI_API_KEY = os.environ.get("AZURE_OPENAI_API_KEY", AZURE_OPENAI_API_KEY)
AZURE_OPENAI_DEPLOYMENT_NAME = os.environ.get("AZURE_OPENAI_DEPLOYMENT_NAME", AZURE_OPENAI_DEPLOYMENT_NAME)

# --- Initialize Clients ---
try:
    from openai import AzureOpenAI
    openai_client = AzureOpenAI(
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_key=AZURE_OPENAI_API_KEY,
        api_version="2024-02-15-preview"
    )
except Exception as e:
    print(f"❌ Failed to load OpenAI client: {e}")
    sys.exit(1)

# Initialize Kusto Client
kusto_client = None
MOCK_MODE = False

try:
    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
    
    # Determine if running in web server mode (non-blocking) or interactive terminal
    is_web_server = "aml_gpt_web" in sys.argv[0] or any("aml_gpt_web" in arg for arg in sys.argv)
    
    if is_web_server:
        print("[INFO] Web server environment detected. Initializing Kusto silently using CLI credentials...")
        try:
            kcsb = KustoConnectionStringBuilder.with_aad_azure_cli_authentication(EVENTHOUSE_CLUSTER_URI)
            kusto_client = KustoClient(kcsb)
            print("[SUCCESS] Kusto client initialized successfully using CLI auth.")
        except Exception as e:
            print(f"[WARNING] CLI authentication failed: {e}. Falling back to Local Mock Database mode.")
            MOCK_MODE = True
    else:
        # Establish AAD Device Authentication connection for interactive terminal run
        print("[INFO] Establishing connection to Eventhouse. A browser login prompt may appear...")
        kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(EVENTHOUSE_CLUSTER_URI)
        kusto_client = KustoClient(kcsb)
except Exception as e:
    print(f"[WARNING] Kusto client initialization failed: {e}. Running in Local Mock Database mode.")
    MOCK_MODE = True


# --- Table Schemas Grounding for Text-to-KQL ---
SCHEMA_CONTEXT = """
You are the translation engine for the Sentinel Mesh AML GPT. 
Your job is to translate a user's natural language question into a syntactically correct KQL (Kusto Query Language) query.

Available tables and their schemas in the database:

1. Table: fact_transactions
   - transaction_id: string (unique transaction ledger ID)
   - customer_id: string (unique customer ID)
   - customer_name: string (customer name)
   - account_id: string (debited account ID)
   - amount: real (transaction value in INR)
   - timestamp: datetime (transaction date/time)
   - channel: string (Branch, ATM, Mobile, UPI, SWIFT, POS)
   - device_id: string (device identifier)
   - ip_address: string (IP address of connection)
   - geo_location: string (city location of transaction)
   - merchant_id: string (merchant ID if payment)
   - mcc_code: string (merchant category code)
   - counterparty_account: string (credited beneficiary account ID)
   - transaction_type: string (deposit, withdrawal, transfer, payment)
   - ingestion_timestamp: datetime
   - processing_status: string
   - risk_flag: string (none, SUSPICIOUS, HIGH)
   - pattern_hash: string
   - scenario_id: string (normal, structuring, circular_flow, shadow_link, velocity_spike, shell_merchant, dormant_activation)

2. Table: fact_alerts
   - alert_id: string (unique UUID of the alert)
   - customer_id: string (customer flagged)
   - customer_name: string
   - agent_name: string (consolidated agents, e.g. "Structuring Sentinel")
   - confidence: real (0.0 to 1.0)
   - reason: string (joined list of trigger descriptions)
   - risk_score: real (composite score 0.0 to 100.0)
   - risk_tier: string (LOW, MEDIUM, HIGH, CRITICAL)
   - status: string (ACTIVE, RESOLVED)
   - disposition: string (PENDING, TRUE_POSITIVE, FALSE_POSITIVE)
   - triggered_at: datetime
   - resolved_at: datetime
   - related_transactions: dynamic (JSON array of transaction IDs)
   - cross_correlations: dynamic (JSON object of parameters used)

3. Table: fact_sar_reports
   - sar_id: string (unique report ID)
   - customer_id: string
   - customer_name: string
   - case_id: string
   - executive_summary: string
   - suspicious_activity: string
   - red_flags: dynamic
   - recommended_actions: dynamic
   - risk_assessment: string
   - risk_score: real
   - model_version: string
   - prompt_hash: string
   - generated_at: datetime
   - validated: bool
   - content_safety_score: real

4. Table: fact_analyst_dispositions
   - disposition_id: string
   - alert_id: string
   - customer_id: string
   - agent_name: string
   - disposition: string (TRUE_POSITIVE, FALSE_POSITIVE, ESCALATE)
   - analyst_id: string
   - timestamp: datetime
   - notes: string

5. Table: fact_circular_flows
   - flow_id: string
   - cycle_accounts: dynamic (JSON array of accounts in the loop)
   - cycle_customers: dynamic (JSON array of customers in the loop)
   - hop_count: int
   - total_amount: real
   - avg_amount_per_hop: real
   - amount_consistency_pct: real
   - time_window_minutes: real
   - detected_at: datetime
   - status: string

6. Table: fact_risk_scores
   - snapshot_id: string
   - customer_id: string
   - customer_name: string
   - risk_score: real
   - risk_tier: string
   - f1_agent_consensus: real
   - f2_graph_centrality: real
   - f3_dna_deviation: real
   - f4_historical_risk: real
   - f5_watchlist_match: real
   - active_agents: dynamic
   - snapshot_timestamp: datetime

7. Table: dim_customer
   - customer_id: string
   - name: string
   - risk_score: real
   - kyc_status: string
   - pep_flag: bool
   - country_code: string
   - city: string
   - created_date: datetime

8. Table: dim_account
   - account_id: string
   - customer_id: string
   - account_type: string
   - balance: real
   - velocity_index: real
   - opened_date: datetime
   - dormancy_score: real

9. Table: dim_merchant
   - merchant_id: string
   - merchant_name: string
   - mcc_code: string
   - category: string
   - risk_tier: string
   - shell_score: real
   - registered_date: datetime

10. Table: dim_device
    - device_id: string (hardware identifier)
    - device_type: string (MOBI, TAB, WEB)
    - first_seen: datetime
    - last_seen: datetime
    - customer_ids: dynamic (JSON array of users)

11. Table: dim_ip_address
    - ip_address: string
    - geo_location: string
    - is_vpn: bool
    - is_tor: bool
    - customer_ids: dynamic (JSON array of users)

12. Table: dim_behavioral_dna
    - customer_id: string
    - time_window: string
    - velocity: real
    - amount_profile: real
    - temporal_pattern: real
    - counterparty_diversity: real
    - channel_mix: real
    - geographic_spread: real
    - amount_entropy: real
    - round_number_ratio: real
    - counterparty_recurrence: real
    - dormancy_burst: real
    - cross_border_ratio: real
    - device_switching: real
    - drift_score: real
    - computed_at: datetime

13. Table: dim_adaptive_config
    - config_key: string
    - config_value: real
    - description: string
    - last_updated: datetime
    - updated_by: string

KQL Rules:
- Return ONLY valid KQL queries.
- Do NOT wrap queries in markdown code blocks (e.g. do not use ```kql ... ```). Return raw KQL query string.
- Do NOT include any introductory or explanatory text (e.g. do not say "Here is the query:"). Return ONLY the executable query.
- Match the user's natural language entities (like accounts, devices, flows, configs, dispositions, merchants, IPs) to their respective tables above.
- Always restrict output rows to a maximum of 10-15 rows (e.g., '| take 10' or '| limit 10') unless specifically asked to summarize or aggregate counts.
"""

def clean_kql_query(raw_query: str) -> str:
    """Cleans the generated KQL query, stripping markdown wrappers, comments, and conversational text."""
    if not raw_query:
        return ""
    
    cleaned = raw_query.strip()
    
    # Check if the response contains markdown code block triple-backticks
    if "```" in cleaned:
        parts = cleaned.split("```")
        found_block = False
        for part in parts:
            p = part.strip()
            # If the part starts with kql/kusto, strip it
            lines = p.splitlines()
            if lines and lines[0].strip().lower() in ["kql", "kusto", "sql", "query", "kustostatements"]:
                p = "\n".join(lines[1:]).strip()
            # If it references a table, we consider it the query
            if p and any(tbl in p for tbl in ["fact_transactions", "fact_alerts", "fact_sar_reports", "dim_customer", "dim_account", "dim_merchant"]):
                cleaned = p
                found_block = True
                break
        if not found_block:
            if len(parts) >= 3:
                cleaned = parts[1].strip()
                lines = cleaned.splitlines()
                if lines and lines[0].strip().lower() in ["kql", "kusto", "sql", "query", "kustostatements"]:
                    cleaned = "\n".join(lines[1:]).strip()
            else:
                cleaned = cleaned.replace("```", "").strip()
            
    # Strip inline backticks if any
    if cleaned.startswith("`") and cleaned.endswith("`"):
        cleaned = cleaned.strip("`").strip()
        
    # Filter out conversational text lines
    lines = cleaned.splitlines()
    kql_lines = []
    for line in lines:
        line_str = line.strip()
        if not line_str:
            continue
        # Skip markdown styling or explanations
        if line_str.startswith("#") or line_str.startswith("**") or line_str.lower().startswith("here is") or line_str.lower().startswith("note:"):
            continue
        if any(phrase in line_str.lower() for phrase in ["this query", "will search", "retrieves", "returns", "selects", "query explanation"]):
            if not line_str.startswith("|"):
                continue
        kql_lines.append(line)
        
    # Ensure multiple queries are separated by semicolons
    cleaned_lines = []
    for i, line in enumerate(kql_lines):
        line_str = line.strip()
        # If this line starts with a new query statement/table and it is not the first line
        if i > 0 and line_str and (line_str[0].isalnum() or line_str[0] == '.'):
            # Check if it starts with a KQL keyword (which means it's a continuation of previous line)
            first_word = line_str.split()[0].lower() if line_str.split() else ""
            is_kql_keyword = first_word in ["where", "project", "take", "limit", "join", "on", "by", "summarize", "extend", "order", "sort", "union", "mv-expand", "and", "or"]
            
            if not is_kql_keyword:
                # Check if previous line needs a semicolon
                prev_line = cleaned_lines[-1].strip()
                if not any(prev_line.endswith(char) for char in [";", "(", "|", ",", "+", "-", "*", "/"]):
                    if not any(prev_line.lower().endswith(w) for w in ["union", "join", "and", "or"]):
                        cleaned_lines[-1] = cleaned_lines[-1] + ";"
        cleaned_lines.append(line)
        
    cleaned = "\n".join(cleaned_lines).strip()
    
    # Remove trailing semicolon if any
    if cleaned.endswith(";"):
        cleaned = cleaned[:-1].strip()
        
    return cleaned

def generate_kql_query(user_question: str) -> str:
    """Invokes OpenAI to compile natural language into KQL."""
    try:
        response = openai_client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": SCHEMA_CONTEXT},
                {"role": "user", "content": f"Compile this question to raw KQL: '{user_question}'"}
            ],
            temperature=0.0
        )
        raw_kql = response.choices[0].message.content.strip()
        return clean_kql_query(raw_kql)
    except Exception as e:
        print(f"❌ OpenAI KQL generation failed: {e}")
        return ""

# --- Local Mock Data Store for Fallback Mode ---
MOCK_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data_simulator", "generated_data")

def load_mock_table(table_name):
    file_path = os.path.join(MOCK_DATA_DIR, f"{table_name}.json")
    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return []

MOCK_ALERTS = [
    {
        "alert_id": "ALRT-1780900001",
        "customer_id": "CUST-0022",
        "customer_name": "Bhavika Tank",
        "agent_name": "Structuring Sentinel",
        "risk_score": 57.4300,
        "risk_tier": "MEDIUM",
        "reason": "Customer structured 5 transactions totaling INR 3217304.07 (individual max: INR 924682.57, threshold: INR 1000000.0) Impossible travel detected: Transacted from 'Saket, Delhi' and 'T. Nagar, Chennai' within 99 minutes (allowed window: 120.0 mins) Impossible travel detected: Transacted from 'Marathahalli, Bangalore' and 'Madhapur, Hyderabad' within 20 minutes (allowed window: 120.0 mins)",
        "cross_correlations": '{"agent_list": ["Structuring Sentinel", "Geo-Temporal Analyzer"], "pagerank_score": 0.42, "dna_drift_score": 2.1, "pep_watchlist_match": false}',
        "triggered_at": "2026-06-07T21:39:00Z",
        "status": "New"
    },
    {
        "alert_id": "ALRT-1780900002",
        "customer_id": "CUST-0045",
        "customer_name": "Praneel Jani",
        "agent_name": "Velocity Spike",
        "risk_score": 54.1100,
        "risk_tier": "MEDIUM",
        "reason": "Customer structured 8 transactions totaling INR 6200506.49 (individual max: INR 933841.16, threshold: INR 1000000.0) Impossible travel detected: Transacted from 'Electronic City, Bangalore' and 'Adyar, Chennai' within 20 minutes (allowed window: 120.0 mins) Impossible travel detected: Transacted from 'Anna Nagar, Chennai' and 'Saket, Delhi' within 36 minutes (allowed window: 120.0 mins)",
        "cross_correlations": '{"agent_list": ["Velocity Spike", "Geo-Temporal Analyzer"], "pagerank_score": 0.35, "dna_drift_score": 3.4, "pep_watchlist_match": false}',
        "triggered_at": "2026-06-07T21:39:00Z",
        "status": "New"
    },
    {
        "alert_id": "ALRT-1780900003",
        "customer_id": "CUST-0037",
        "customer_name": "Upasna Varma",
        "agent_name": "Structuring Sentinel",
        "risk_score": 52.8800,
        "risk_tier": "MEDIUM",
        "reason": "Customer structured 4 transactions totaling INR 3122342.58 (individual max: INR 940688.39, threshold: INR 1000000.0) Impossible travel detected: Transacted from 'Velachery, Chennai' and 'Anna Nagar, Chennai' within 58 minutes (allowed window: 120.0 mins) Impossible travel detected: Transacted from 'Jayanagar, Bangalore' within 25 minutes (allowed window: 120.0 mins)",
        "cross_correlations": '{"agent_list": ["Structuring Sentinel", "Geo-Temporal Analyzer"], "pagerank_score": 0.38, "dna_drift_score": 2.2, "pep_watchlist_match": false}',
        "triggered_at": "2026-06-07T21:39:00Z",
        "status": "New"
    },
    {
        "alert_id": "ALRT-1780900004",
        "customer_id": "CUST-0046",
        "customer_name": "Veer Bir",
        "agent_name": "Velocity Anomaly Detector",
        "risk_score": 85.2000,
        "risk_tier": "CRITICAL",
        "reason": "Impossible travel detected: Transacted from 'Whitefield, Bangalore' and 'Connaught Place, Delhi' within 30 minutes (allowed window: 120.0 mins). Significant behavioral DNA drift detected.",
        "cross_correlations": '{"agent_list": ["Velocity Anomaly Detector", "Geo-Temporal Analyzer"], "pagerank_score": 0.65, "dna_drift_score": 4.2, "pep_watchlist_match": false}',
        "triggered_at": "2026-06-08T07:42:00Z",
        "status": "New"
    },
    {
        "alert_id": "ALRT-1780900005",
        "customer_id": "CUST-0020",
        "customer_name": "Azad Mutti",
        "agent_name": "Regulatory Watchlist Matcher",
        "risk_score": 91.5000,
        "risk_tier": "CRITICAL",
        "reason": "High risk watchlist match for Politically Exposed Person (PEP) list. Unusual transfers to foreign counterparties detected.",
        "cross_correlations": '{"agent_list": ["Regulatory Watchlist Matcher", "Shadow Link Discoverer"], "pagerank_score": 0.72, "dna_drift_score": 1.5, "pep_watchlist_match": true}',
        "triggered_at": "2026-06-08T07:42:15Z",
        "status": "New"
    },
    {
        "alert_id": "ALRT-1780900006",
        "customer_id": "CUST-0008",
        "customer_name": "Gayathri Chaudry",
        "agent_name": "Structuring Sentinel",
        "risk_score": 68.4500,
        "risk_tier": "HIGH",
        "reason": "Smurfing / Structuring scheme detected across multiple branches in Indore. Total deposit of INR 45 Lakhs split in 5 transactions.",
        "cross_correlations": '{"agent_list": ["Structuring Sentinel"], "pagerank_score": 0.28, "dna_drift_score": 3.1, "pep_watchlist_match": false}',
        "triggered_at": "2026-06-08T07:42:06Z",
        "status": "New"
    },
    {
        "alert_id": "ALRT-1780900007",
        "customer_id": "CUST-0004",
        "customer_name": "Tejas Kaul",
        "agent_name": "Circular Flow Tracker",
        "risk_score": 82.1000,
        "risk_tier": "CRITICAL",
        "reason": "Circular money flow loop detected: CUST-0004 -> ACC-30492 -> ACC-89201 -> CUST-0004 within 60 minutes with 99% amount conservation.",
        "cross_correlations": '{"agent_list": ["Circular Flow Tracker", "Shadow Link Discoverer"], "pagerank_score": 0.58, "dna_drift_score": 1.2, "pep_watchlist_match": false}',
        "triggered_at": "2026-06-08T07:41:49Z",
        "status": "New"
    }
]

MOCK_TRANSACTIONS = [
    {
        "transaction_id": "TXN-10000001",
        "customer_id": "CUST-0022",
        "customer_name": "Bhavika Tank",
        "account_id": "ACC-0022",
        "amount": 924682.5700,
        "timestamp": "2026-06-07T21:10:00Z",
        "channel": "Branch",
        "device_id": "DEV-0022",
        "ip_address": "192.168.1.22",
        "geo_location": "Saket, Delhi",
        "merchant_id": "MERC-0001",
        "mcc_code": "5411",
        "counterparty_account": "ACC-89201",
        "transaction_type": "deposit",
        "risk_flag": "SUSPICIOUS"
    },
    {
        "transaction_id": "TXN-10000002",
        "customer_id": "CUST-0022",
        "customer_name": "Bhavika Tank",
        "account_id": "ACC-0022",
        "amount": 850000.0000,
        "timestamp": "2026-06-07T21:15:00Z",
        "channel": "ATM",
        "device_id": "DEV-0022",
        "ip_address": "192.168.1.22",
        "geo_location": "Saket, Delhi",
        "merchant_id": "MERC-0002",
        "mcc_code": "5732",
        "counterparty_account": "ACC-89201",
        "transaction_type": "deposit",
        "risk_flag": "SUSPICIOUS"
    },
    {
        "transaction_id": "TXN-10000003",
        "customer_id": "CUST-0022",
        "customer_name": "Bhavika Tank",
        "account_id": "ACC-0022",
        "amount": 950000.0000,
        "timestamp": "2026-06-07T21:20:00Z",
        "channel": "Mobile",
        "device_id": "DEV-0022",
        "ip_address": "192.168.1.22",
        "geo_location": "T. Nagar, Chennai",
        "merchant_id": "MERC-0003",
        "mcc_code": "5651",
        "counterparty_account": "ACC-89201",
        "transaction_type": "deposit",
        "risk_flag": "HIGH"
    },
    {
        "transaction_id": "TXN-10000004",
        "customer_id": "CUST-0046",
        "customer_name": "Veer Bir",
        "account_id": "ACC-0046",
        "amount": 1250000.0000,
        "timestamp": "2026-06-08T07:15:00Z",
        "channel": "UPI",
        "device_id": "DEV-0046",
        "ip_address": "192.168.1.46",
        "geo_location": "Whitefield, Bangalore",
        "merchant_id": "MERC-0008",
        "mcc_code": "6051",
        "counterparty_account": "ACC-99120",
        "transaction_type": "transfer",
        "risk_flag": "HIGH"
    },
    {
        "transaction_id": "TXN-10000005",
        "customer_id": "CUST-0046",
        "customer_name": "Veer Bir",
        "account_id": "ACC-0046",
        "amount": 1500000.0000,
        "timestamp": "2026-06-08T07:30:00Z",
        "channel": "Mobile",
        "device_id": "DEV-0046",
        "ip_address": "192.168.1.46",
        "geo_location": "Connaught Place, Delhi",
        "merchant_id": "MERC-0008",
        "mcc_code": "6051",
        "counterparty_account": "ACC-99120",
        "transaction_type": "transfer",
        "risk_flag": "HIGH"
    },
    {
        "transaction_id": "TXN-10000006",
        "customer_id": "CUST-0020",
        "customer_name": "Azad Mutti",
        "account_id": "ACC-0020",
        "amount": 2500000.0000,
        "timestamp": "2026-06-08T07:20:00Z",
        "channel": "SWIFT",
        "device_id": "DEV-0020",
        "ip_address": "10.0.0.4",
        "geo_location": "Delhi",
        "merchant_id": "MERC-0010",
        "mcc_code": "7995",
        "counterparty_account": "ACC-CH-8921",
        "transaction_type": "transfer",
        "risk_flag": "HIGH"
    },
    {
        "transaction_id": "TXN-10000007",
        "customer_id": "CUST-0004",
        "customer_name": "Tejas Kaul",
        "account_id": "ACC-0004",
        "amount": 995000.0000,
        "timestamp": "2026-06-08T07:10:00Z",
        "channel": "UPI",
        "device_id": "DEV-0004",
        "ip_address": "192.168.1.4",
        "geo_location": "Jaipur",
        "merchant_id": "MERC-0004",
        "mcc_code": "5411",
        "counterparty_account": "ACC-30492",
        "transaction_type": "transfer",
        "risk_flag": "NORMAL"
    }
]

def execute_mock_kql(query: str):
    """Local mock KQL query executor for fallback mode."""
    print(f"[MOCK ENGINE] Executing KQL query locally: {query}")
    table_name = None
    for tbl in ["fact_alerts", "fact_transactions", "dim_customer", "dim_account", "dim_merchant"]:
        if tbl in query:
            table_name = tbl
            break
            
    if not table_name:
        return []
        
    if table_name == "fact_alerts":
        data = list(MOCK_ALERTS)
    elif table_name == "fact_transactions":
        data = list(MOCK_TRANSACTIONS)
    elif table_name == "dim_customer":
        data = load_mock_table("dim_customer")
    elif table_name == "dim_account":
        data = load_mock_table("dim_account")
    elif table_name == "dim_merchant":
        data = load_mock_table("dim_merchant")
    else:
        data = []
        
    steps = [step.strip() for step in query.split("|") if step.strip()]
    for step in steps:
        if step.startswith("where "):
            cond = step[6:].strip()
            # Simple equal filter
            if "==" in cond or "=" in cond:
                op = "==" if "==" in cond else "="
                parts = cond.split(op)
                col = parts[0].strip()
                val = parts[1].strip().strip("'\"")
                if val.lower() == "true":
                    val = True
                elif val.lower() == "false":
                    val = False
                data = [row for row in data if str(row.get(col, "")).lower() == str(val).lower()]
            # Simple greater than filter
            elif ">" in cond:
                parts = cond.split(">")
                col = parts[0].strip()
                try:
                    val = float(parts[1].strip())
                    data = [row for row in data if row.get(col) is not None and float(row.get(col)) > val]
                except ValueError:
                    pass
            # Simple less than filter
            elif "<" in cond:
                parts = cond.split("<")
                col = parts[0].strip()
                try:
                    val = float(parts[1].strip())
                    data = [row for row in data if row.get(col) is not None and float(row.get(col)) < val]
                except ValueError:
                    pass
        elif step.startswith("order by "):
            parts = step[9:].strip().split()
            if parts:
                col = parts[0]
                desc = len(parts) > 1 and parts[1].lower() == "desc"
                data = sorted(data, key=lambda x: x.get(col) if x.get(col) is not None else "", reverse=desc)
        elif step.startswith("take ") or step.startswith("limit "):
            try:
                num = int(step.split()[1])
                data = data[:num]
            except Exception:
                pass
    return data

def execute_kql_query(kql_query: str):
    """Executes a KQL query against the Kusto database, falling back to mock data if needed."""
    global MOCK_MODE
    if not MOCK_MODE and kusto_client:
        try:
            response = kusto_client.execute(EVENTHOUSE_DATABASE, kql_query)
            primary_results = response.primary_results[0]
            
            # Parse rows and columns
            columns = [col.column_name for col in primary_results.columns]
            rows = []
            for row in primary_results.rows:
                row_dict = {}
                for i, col in enumerate(columns):
                    val = row[i]
                    if isinstance(val, datetime):
                        val = val.isoformat()
                    row_dict[col] = val
                rows.append(row_dict)
            return rows
        except Exception as e:
            print(f"⚠️ Eventhouse query failed: {e}. Falling back to Local Mock Database.")
            MOCK_MODE = True
            
    return execute_mock_kql(kql_query)

def synthesize_response(user_question: str, query_results: list) -> str:
    """Takes query outputs and writes a human-readable summary via OpenAI."""
    try:
        results_str = json.dumps(query_results, indent=2)
        system_prompt = (
            "You are the Sentinel Mesh AML GPT. Grounded in the database outputs, "
            "provide a professional, concise, and structured answer to the analyst's question. "
            "Refer to the customers, alert scores, and dates precisely as returned in the data. "
            "If the results are empty, state that no matching records were found."
        )
        user_prompt = f"""
        User Question: {user_question}
        Database Results:
        {results_str}
        
        Please synthesize this data into an expert compliance response.
        """
        response = openai_client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.3
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"Error synthesizing response: {e}"

# --- Main AML GPT Interactive Shell ---
def run_aml_gpt():
    # Attempt a quick test connection
    try:
        kusto_client.execute(EVENTHOUSE_DATABASE, "fact_alerts | limit 1")
        print("[SUCCESS] Eventhouse connection verified successfully!")
    except Exception:
        print("[WARNING] Waiting for user authentication...")

    print("\n" + "="*80)
    print("SENTINEL MESH V2 — INTERACTIVE AML GPT (LAYER 7)")
    print("="*80)
    print("Type your questions in plain English (e.g. 'Show me high risk alerts').")
    print("Type 'exit' to exit.")
    print("="*80 + "\n")

    while True:
        try:
            user_input = input("Compliance Officer [Analyst]: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nExiting AML GPT. Goodbye!")
            break

        if not user_input:
            continue

        if user_input.lower() in ("exit", "quit"):
            print("Exiting AML GPT. Goodbye!")
            break

        print("Thinking...")
        
        # 1. Translate question to KQL
        kql_query = generate_kql_query(user_input)
        
        # Display the KQL query being run under the hood for educational transparency
        print(f"[KQL Query]: {kql_query}")
        
        # 2. Run query on Eventhouse
        results = execute_kql_query(kql_query)
        
        if results is None:
            print("[ERROR] AML GPT encountered an error querying the database.\n")
            continue
            
        # 3. Synthesize human explanation
        response = synthesize_response(user_input, results)
        print(f"\nAML GPT:\n{response}\n")


if __name__ == "__main__":
    run_aml_gpt()
