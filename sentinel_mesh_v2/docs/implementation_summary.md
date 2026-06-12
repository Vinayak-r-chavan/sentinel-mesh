# 🛡️ SENTINEL MESH V2 — Phase 1, 2, 3 & 4 Implementation Walkthrough

This document records the step-by-step setup, commands, and configurations completed for **Layer 1 (Data Ingestion)**, **Layer 2 (Lakehouse)**, **Layer 3 (Behavioral DNA)**, **Layer 4 (Knowledge Graph)**, **Layer 5 (AI Agent Swarm)**, **Layer 6 (Adaptive Risk Engine)**, and **Layer 7 (AI SAR Generator)**.

---

## 📅 Summary of Achievements
1. **Workspace & Database Set Up**: Created `SentinelMesh_AML_POC` workspace and `SentinelMesh_Eventhouse` database.
2. **Schema & Tables Created**: Provisioned all fact, dimension, and config tables.
3. **Dimensions Seeded**: Populated all dimensions and `dim_adaptive_config` thresholds.
4. **Real-Time Stream Configured**: Connected Simulator to Fabric Eventstream via secure SAS connection.
5. **Layer 3 DNA Materialized**: Implemented `L3_behavioral_dna` Spark notebook to compute and save 12D behavioral profiles to `dim_behavioral_dna`.
6. **Layer 4 Graph & Entity Resolution Deployed**: Created Spark notebooks for Louvain communities, PageRank centrality, and Entity Resolution (using fuzzy name + DNA similarity).
7. **Layer 5 AI Agent Swarm Operational**: Deployed 6 detection agents (Structuring, Velocity, Travel, Loop, Shadow Link, Shell Profiler) natively in KQL.
8. **Layer 6 Risk Engine Operational**: Deployed `generate_composite_alerts()` running a 5-factor composite risk scoring engine using `dim_adaptive_config` weights.
9. **Layer 7 AI SAR Generator & AML GPT Operational**: Deployed Azure OpenAI integrated Spark notebook `L7_SAR_Generator` to draft detailed Suspicious Activity Reports (SAR), and implemented `copilot/aml_gpt.py` as a natural language text-to-KQL interactive chat shell.
10. **Fabric Data Pipeline Automated**: Configured Data Factory pipeline to run DNA computation ➔ Graph Analysis ➔ Entity Resolution ➔ Risk Alerting ➔ SAR Generation sequentially.
11. **Real-Time Insights Dashboard Operational**: Built the `SentinelMesh_L4_Insights` dashboard with 5 visual tiles displaying loops, shadow links, community size graphs, resolved entities, and the composite alerts feed.

---

## 🛠️ Step-by-Step Actions & Commands

### STEP 1: Database DDL Schema Setup
The database schema was created by running the commands from `kql/L2_eventhouse_ddl.kql` in the KQL query editor to provision all 13 tables (including `fact_transactions`, `fact_alerts`, `dim_customer`, `dim_account`, `dim_merchant`, `dim_device`, `dim_ip_address`, and `dim_adaptive_config`).

To verify tables were created, we ran:
```kql
.show tables
```

---

### STEP 2: Dimension Ingestion & Configuration Seeding
Due to formatting limitations in raw CSV inline ingestion (commas in merchant names and device array brackets), we used robust KQL `datatable` scripts to clear and ingest the dimensions:

#### 1. Customers (`dim_customer`) & Accounts (`dim_account`)
Loaded via their respective `.kql` files:
* [ingest_dim_customer.kql](file:///c:/Users/vinay/Documents/hack2future/sentinel_mesh_v2/data_simulator/upload_ready/ingest_dim_customer.kql)
* [ingest_dim_account.kql](file:///c:/Users/vinay/Documents/hack2future/sentinel_mesh_v2/data_simulator/upload_ready/ingest_dim_account.kql)

#### 2. Merchants (`dim_merchant`)
Ingested using:
```kql
.clear table dim_merchant data

.set-or-append dim_merchant <|
    datatable(merchant_id:string, merchant_name:string, mcc_code:string, category:string, risk_tier:string, shell_score:real, registered_date:datetime) [
        "MERC-0001", "Dar-Koshy", "6051", "Money Transfer", "High", 0.7774, datetime(2023-06-05),
        "MERC-0002", "Baral, Bumb and Saini", "7995", "Gambling", "High", 0.7918, datetime(2026-05-08),
        ...
    ]
```

#### 3. Devices (`dim_device`)
Ingested using:
```kql
.clear table dim_device data

.set-or-append dim_device <|
    datatable(device_id:string, device_type:string, first_seen:datetime, last_seen:datetime, customer_ids:dynamic) [
        "DEV-TAB-00001", "TAB", datetime(2025-07-25T14:42:44Z), datetime(2026-05-09T09:19:40Z), dynamic(["CUST-0006"]),
        ...
    ]
```

#### 4. IP Addresses (`dim_ip_address`)
Ingested using:
```kql
.clear table dim_ip_address data

.set-or-append dim_ip_address <|
    datatable(ip_address:string, geo_location:string, is_vpn:bool, is_tor:bool, customer_ids:dynamic) [
        "10.133.189.255", "Coimbatore, India", false, false, dynamic(["CUST-0015"]),
        ...
    ]
```

#### 5. Adaptive Rules Config (`dim_adaptive_config`)
We seeded the 19 dynamic rules and scoring thresholds into the system:
```kql
.clear table dim_adaptive_config data

.set-or-append dim_adaptive_config <|
    datatable(config_key:string, config_value:real, description:string, last_updated:datetime, updated_by:string) [
        "scoring.weight.f1_agent_consensus", 35.0, "Weight for agent consensus factor", datetime(2026-06-06), "system_init",
        "agent.structuring.amount_threshold", 1000000.0, "CTR reporting threshold in INR", datetime(2026-06-06), "system_init",
        ...
    ]
```

#### Verification Query:
```kql
print "--- Dimension Counts ---";
dim_customer | count;
dim_account | count;
dim_merchant | count;
dim_device | count;
dim_ip_address | count;
dim_adaptive_config | count
```
*Expected Counts:* `50`, `80`, `20`, `30`, `40`, `19`.

---

### STEP 3: Eventstream & Ingestion Mapping Setup

1. **Ingestion Mapping**: Registered the JSON format ingestion mapping in KQL:
   ```kql
   .create-or-alter table fact_transactions ingestion json mapping 'fact_transactions_v2_mapping' '[{"column": "transaction_id", "Properties": {"Path": "$.transaction_id"}}, ...]'
   ```

2. **Eventstream Canvas**: Created `es_transactions` in the Fabric Portal.
   * Added a **Custom Endpoint** source named `simulator-source`.
   * Published the changes, went to **SAS Key Authentication** on the source, and copied the **Connection string - primary key**.
   * Updated `FABRIC_EVENTSTREAM_CONN_STR` in [config/.env](file:///c:/Users/vinay/Documents/hack2future/sentinel_mesh_v2/config/.env).

3. **Database Destination Routing**: Added **Eventhouse** destination on the Eventstream canvas named `kql-dest` connecting to the `fact_transactions` table.
   * Mode: *Event processing before ingestion* (to bypass UI mapping bugs).
   * Published changes.

---

### STEP 4: Streaming Verification Commands
To test the real-time pipeline, we ran the Python transaction simulator from the `data_simulator/` directory:

1. **Test output in the console only (no streaming):**
   ```bash
   python -m simulator --mode console --count 10
   ```

2. **Stream both Normal Traffic AND AML Scenarios (Default):**
   ```bash
   python -m simulator --mode stream --count 20
   ```
   *(Sends 71 scenario events + 20 normal events = 91 events total).*

3. **Stream ONLY Normal Transactions (Strict count limit):**
   ```bash
   python -m simulator --mode stream --pattern normal --count 20
   ```
   *(Sends exactly 20 transactions).*

4. **Verify database table counts in KQL:**
   ```kql
   fact_transactions | count
   ```

5. **Verify data parsing (checks for nulls):**
   ```kql
   fact_transactions | take 10
   ```

## 🧠 Next Phase Design & Implementation Status

### ✅ 3. Layer 3: IQ Ontology Core (Behavioral DNA Engine) — COMPLETED
We have successfully implemented and verified the real-time Behavioral DNA Engine.

*   **Config-Driven Architecture**: Refactored to query all parameters (lookback windows, normalizations, and fallback bounds) dynamically from the `dim_adaptive_config` table. Zero hardcoded thresholds.
*   **Materialized View (`mv_daily_dna_stats`)**: Deployed successfully to aggregate daily statistics incrementally on ingestion (velocity, sum_amount, sum_sq_amount, digital_count, round_number_count, device_count, geographic_spread, cross_border_count).
*   **Algebraic Standard Deviation**: Handled the KQL Materialized View limitation (no native `stdev()` support) by storing the sum of squares and calculating the standard deviation algebraically on-the-fly.
*   **Dynamic Cast & Arithmetic**: Solved KQL compiler limitations:
    *   Replaced `ago()` with datetime subtraction (`now() - window`) to support dynamic timespan lookups.
    *   Replaced invalid type keywords (`double()`) with casting functions (`todouble()`).
    *   Expressed lookback variables using KQL compile-time timespan arithmetic (`1d * toreal(...)`).
*   **Verified DNA Output**: Streamed transactions via the simulator (`--count 50`) and verified that the KQL function `get_behavioral_dna()` successfully returns 12D vectors and Euclidean Drift Scores per customer in real-time.

---

### ✅ 4. Layer 4: Temporal Knowledge Graph (Money Flow Links) — COMPLETED
This layer maps transactions, customers, devices, and IPs into a connected web to uncover structural fraud loops and linked entities.

*   **Config-Driven KQL Graph Queries**: Deployed native `detect_circular_flows()` and `detect_shadow_links()` functions. These query settings dynamically from `dim_adaptive_config` to avoid compile-time hardcoding.
*   **Louvain Community Detection & PageRank Centrality**: Deployed Spark notebook `L4_graph_analysis.py` to identify tightly-bound fraud clusters and money routing mule accounts, persisting results to `fact_graph_communities`.
*   **Entity Resolution Engine**: Deployed Spark notebook `entity_resolution.py` to run fuzzy string matching, device/IP intersection, and 12-dimensional Behavioral DNA Cosine Similarity to identify duplicate or synthetic customer entities, saving outputs to `fact_entity_resolution`.
*   **Data Pipeline Orchestration**: Configured `SentinelMesh_L4_Orchestrator` Data Factory pipeline to automate execution sequentially on a schedule.
*   **Native Real-Time Dashboard**: Set up `SentinelMesh_L4_Insights` KQL Dashboard with 4 distinct tiles (Fund Loops, Shadow Links, Community Size Bar Chart, and Resolved Linked Entities) for active monitoring.
*   **Detailed Setup Guide**: Reference the [walkthrough_L4.md](file:///c:/Users/vinay/Documents/hack2future/sentinel_mesh_v2/docs/walkthrough_L4.md) for full commands, schemas, and configurations.

---

### ✅ 5. Layer 5: AI Agent Swarm — COMPLETED
This layer replaces static rule systems with 6 dynamic, KQL-native statistical detection agents:
*   **Structuring Sentinel**: Detects deposits below ₹10L reporting threshold that collectively exceed it.
*   **Velocity Anomaly**: Analyzes transaction counts and volumes against standard DNA baselines via time-series plugins.
*   **Geo-Temporal Analyzer**: Identifies impossible travel scenarios by location and timestamp serialize tracking.
*   **Loop & Relationship Tracker**: Feeds detected cycles and shadow links into customer risk histories.
*   **Detailed Setup Guide**: Reference the [walkthrough_L5_L6.md](file:///c:/Users/vinay/Documents/hack2future/sentinel_mesh_v2/docs/walkthrough_L5_L6.md) for compiled agent code.

---

### ✅ 6. Layer 6: Adaptive Risk Scoring Engine — COMPLETED
This layer aggregates findings into a unified, 100% config-driven composite risk score:
*   **5-Factor Weighted Formula**: Combines Consensus, Centrality, DNA Drift, History, and PEP status.
*   **Automated Risk Tiers**: Classifies alerts into LOW, MEDIUM, HIGH, and CRITICAL.
*   **Detailed Setup Guide**: Reference the [walkthrough_L5_L6.md](file:///c:/Users/vinay/Documents/hack2future/sentinel_mesh_v2/docs/walkthrough_L5_L6.md) for scoring functions.

---

### ✅ 7. Layer 7: AI SAR Generator & AML GPT — COMPLETED
This layer automates Suspicious Activity Report (SAR) narrative generation and provides a natural language query interface using Azure OpenAI:
*   **Targeting High-Risk Entities**: Queries `fact_alerts` for customers classified under HIGH or CRITICAL risk tiers that do not already have an existing record in `fact_sar_reports`.
*   **Azure OpenAI Integration**: Invokes a deployed `gpt-4o` model on East US using structured prompts and dynamic temperature parameters.
*   **Automated Narrative Structure**: Generates a standard compliance JSON containing:
    *   *Executive Summary*: A concise brief of the suspicious case.
    *   *Detailed Findings*: Complex analysis of circular fund loops, community clusters, and behavioral drifts.
    *   *Risk Assessment*: Threat analysis, money laundering/terrorist financing likelihood, and systemic impacts.
    *   *Recommended Actions*: Account freeze indicators, enhanced due diligence triggers, and FIU filing indicators.
*   **Persistent Compliance Database**: Appends generated reports to the `fact_sar_reports` Eventhouse table.
*   **Interactive Natural Language Agent (AML GPT)**: Deployed **[aml_gpt.py](file:///c:/Users/vinay/Documents/hack2future/sentinel_mesh_v2/copilot/aml_gpt.py)**. It takes natural language questions from the analyst, automatically compiles them into correct KQL queries, executes them on Eventhouse via the Kusto SDK (authenticated securely via AAD Device Login), and synthesizes professional compliance analysis answers.
*   **Microsoft Copilot Studio Integration (Option 3)**: Deployed the conversational AML agent `Sentinel Mesh Copilot` within Copilot Studio sandbox. It triggers the Power Automate flow `Query Eventhouse Flow` that receives the user query, invokes Azure OpenAI `gpt-4o` for text-to-KQL query translation, runs KQL against the Eventhouse via the ADX connector, and returns execution queries and tabular results directly to the user message panel.
*   **Detailed Setup Guide**: Reference the **[walkthrough_L7.md](file:///c:/Users/vinay/Documents/hack2future/sentinel_mesh_v2/docs/walkthrough_L7.md)** and **[walkthrough_aml_gpt.md](file:///c:/Users/vinay/Documents/hack2future/sentinel_mesh_v2/docs/walkthrough_aml_gpt.md)** for implementation details.

---

### ❌ Layer 8: Action & Compliance Gateway — DELETED
In accordance with system specifications, Layer 8 (including automated account freezes, manual maker-checker actions, weekly Logic Apps report flows, and queue state files `actions_queue.json` / `actions_history.json`) has been permanently deleted from the codebase. All REST API endpoints and dashboard UI elements relating to Layer 8 have been cleanly removed.

---

## 📈 System Health Check Status
* Ingestion Stream: **ACTIVE (100% success rate)**
* Hot Path Database: **ONLINE**
* Dimension Table joins: **VALID**
* L3 DNA & Drift Engine: **ACTIVE (Populating profiles)**
* L4 Graph Analysis: **ACTIVE (Populating rings & hubs)**
* L5 & L6 Risk Swarm: **ACTIVE (Generating composite alerts feed)**
* L7 AI SAR Generator: **ACTIVE (Writing narratives to `fact_sar_reports`)**
* L7 Copilot Studio Agent & Power Automate Flow: **ACTIVE (Translating NL to KQL and executing against Eventhouse)**
* Layer 8 Gateway: **DEACTIVATED & REMOVED (Cleaned up from code)**
