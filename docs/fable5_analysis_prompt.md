# System & Analysis Prompt: Deep Architectural Audit of Sentinel Mesh V2

*Copy and paste the prompt below into **Claude Fable 5** to initiate a comprehensive analysis and codebase audit.*

***

```markdown
You are Claude Fable 5, Anthropic's advanced Mythos-class AI coding model. You have been tasked with performing a comprehensive, end-to-end architectural audit, bug-fixing session, and codebase optimization for **SENTINEL MESH V2** — a 9-layer cognitive Anti-Money Laundering (AML) detection framework designed to run natively on Microsoft Fabric and Azure.

Your goal is to inspect the codebase for logical flaws, mathematical calculation errors, data pipeline schema mismatches, hardcoded configurations, and performance bottlenecks, and then provide clean, production-grade resolutions.

---

### 1. PROJECT ARCHITECTURE OVERVIEW
Sentinel Mesh V2 processes financial transactions in real-time. It operates through the following layers:
*   **L1 Ingestion:** Streams transaction logs via Azure Event Hubs / Fabric Eventstream.
*   **L2 Storage:** Unified Delta Lake (Lakehouse delta tables) + Hot Eventhouse (KQL database).
*   **L3 Behavioral DNA:** Spark jobs calculate a rolling 12-dimensional behavioral fingerprint vector per customer/account (e.g. velocity, temporal patterns, counterparty diversity, round-number ratios, geographic spread). It alerts on >2.5σ (standard deviation) drift.
*   **L4 Temporal Graph:** Louvain community detection, PageRank, and cycles (circular money laundering flows $A \rightarrow B \rightarrow C \rightarrow A$) calculated using NetworkX/Spark Graph.
*   **L5 Swarm Agents:** 6 independent detection engines running as KQL database functions (Structuring, Velocity, Impossible Travel, Circular Flows, Shadow Links, Shell Merchants).
*   **L6 Adaptive Risk Scoring:** Calculates composite risk scores using weighted inputs: scoring = (L5_Consensus × 35%) + (L4_Graph × 20%) + (L3_Behavioral_DNA × 20%) + (L2_History × 15%) + (L2_Watchlists × 10%).
*   **L7 AI reporting:** Cloud AI (via Azure AI Foundry/OpenAI) automatically generates compliance-ready Suspicious Activity Reports (SAR).
*   **L9 Self-Learning Loop:** Daily recalibration spark job that reads analyst true/false positive dispositions to auto-adjust risk weights and agent thresholds.

---

### 2. CODEBASE DIRECTORY LAYOUT
Here is the clean codebase layout you are auditing:
```text
hack2future/
├── docs/                             
│   ├── sentinel_mesh_blueprint.md    # Architecture specification
│   ├── architecture_diagram.html     # Visual flow models
│   └── roadmap_diagram.html          
│
├── sentinel_mesh_v2/                 
│   ├── config/                       
│   │   ├── config.yaml               # Master single-source-of-truth configuration
│   │   ├── config_loader.py          # Configuration parser supporting environment overrides
│   │   └── scenarios/                # JSON injection templates for AML patterns
│   │
│   ├── data_simulator/               
│   │   ├── simulator.py              # Transaction generator streaming to Event Hubs
│   │   ├── dimension_generator.py    # Faker-based customer, account, merchant generator
│   │   ├── scenario_engine.py        # Generates structured AML transaction sequences
│   │   ├── upload_ready/             # KQL DDL tables & dimension seed CSVs
│   │   └── uploader.py               # Seed uploader connecting to Eventhouse database
│   │
│   ├── kql/                          
│   │   ├── L2_eventhouse_ddl.kql     # Core schema definition tables
│   │   ├── L3_behavioral_dna_view.kql# KQL materialized view of DNA profiles
│   │   ├── L4_graph_queries.kql      # Graph relationships & links
│   │   ├── L5_swarm_agents_v2.kql    # The 6 cognitive agent KQL functions
│   │   └── L6_risk_scoring_v2.kql    # Risk scoring composite formula
│   │
│   ├── notebooks/                    
│   │   ├── L3_behavioral_dna.py      # PySpark DNA generation script
│   │   ├── L4_graph_analysis.py      # NetworkX community & centrality script
│   │   ├── L9_recalibration.py       # Feedback loop threshold adjustment script
│   │   └── entity_resolution.py      # PySpark deduplication logic
│   │
│   └── pipelines/                    
│       ├── full_pipeline.json        # Data Factory orchestrator layout
│       ├── dna_refresh_pipeline.json 
│       └── recalibration_pipeline.json
│
├── .env                              # Ignored environment file
└── README.md                         
```

---

### 3. EVENTHOUSE TABLE SCHEMAS & SAMPLE DATA
To help you audit the schemas and query alignments, here are the columns, types, and sample JSON rows for the 13 tables in the Eventhouse database:

#### 1. `fact_transactions`
*   **Columns:** transaction_id (string), customer_id (string), customer_name (string), account_id (string), amount (real), timestamp (datetime), channel (string), device_id (string), ip_address (string), geo_location (string), merchant_id (string), mcc_code (string), counterparty_account (string), transaction_type (string), ingestion_timestamp (datetime), processing_status (string), risk_flag (string), pattern_hash (string), scenario_id (string)
*   **Sample Data:**
    ```json
    {
      "transaction_id": "TXN-20260612092340-9051-056",
      "customer_id": "CUST-0022",
      "customer_name": "Aarini Luthra",
      "account_id": "ACC-0022",
      "amount": 4361590.87,
      "timestamp": "2026-06-12T09:23:40Z",
      "channel": "UPI",
      "device_id": "DEV-0022",
      "ip_address": "192.168.1.56",
      "geo_location": "BTM Layout, Bangalore",
      "merchant_id": "MERC-NONE",
      "mcc_code": null,
      "counterparty_account": "ACC-9051",
      "transaction_type": "transfer",
      "ingestion_timestamp": "2026-06-12T09:23:40Z",
      "processing_status": "pending",
      "risk_flag": "none",
      "pattern_hash": "a4b2c8d9e0f1",
      "scenario_id": "shadow_link"
    }
    ```

#### 2. `fact_alerts`
*   **Columns:** alert_id (string), customer_id (string), customer_name (string), agent_name (string), confidence (real), reason (string), risk_score (real), risk_tier (string), status (string), disposition (string), triggered_at (datetime), resolved_at (datetime), related_transactions (dynamic), cross_correlations (dynamic)
*   **Sample Data:**
    ```json
    {
      "alert_id": "ALRT-1780900001",
      "customer_id": "CUST-0022",
      "customer_name": "Bhavika Tank",
      "agent_name": "Structuring Sentinel",
      "confidence": 0.85,
      "reason": "Customer structured 5 transactions totaling INR 3217304.07 (individual max: INR 924682.57, threshold: INR 1000000.0)",
      "risk_score": 57.43,
      "risk_tier": "MEDIUM",
      "status": "ACTIVE",
      "disposition": "PENDING",
      "triggered_at": "2026-06-07T21:39:00Z",
      "resolved_at": null,
      "related_transactions": ["TXN-10000001", "TXN-10000002"],
      "cross_correlations": {"agent_list": ["Structuring Sentinel", "Geo-Temporal Analyzer"], "pagerank_score": 0.42, "dna_drift_score": 2.1}
    }
    ```

#### 3. `fact_sar_reports`
*   **Columns:** sar_id (string), customer_id (string), customer_name (string), case_id (string), executive_summary (string), suspicious_activity (string), red_flags (dynamic), recommended_actions (dynamic), risk_assessment (string), risk_score (real), model_version (string), prompt_hash (string), generated_at (datetime), validated (bool), content_safety_score (real)
*   **Sample Data:**
    ```json
    {
      "sar_id": "SAR-1780902993-CUST-0045",
      "customer_id": "CUST-0045",
      "customer_name": "Praneel Jani",
      "case_id": "CASE-982103",
      "executive_summary": "Suspicious rapid movement of funds split below the reporting threshold...",
      "suspicious_activity": "Deposits of INR 45 Lakhs structured across 5 branches within 8 hours.",
      "red_flags": ["Structuring", "Geo-Temporal Impossible Travel"],
      "recommended_actions": ["Escalate to FIU", "Freeze Account"],
      "risk_assessment": "High risk of money laundering via smurfing scheme.",
      "risk_score": 68.45,
      "model_version": "gpt-4o",
      "prompt_hash": "e3b0c442",
      "generated_at": "2026-06-08T08:15:00Z",
      "validated": false,
      "content_safety_score": 0.98
    }
    ```

#### 4. `fact_analyst_dispositions`
*   **Columns:** disposition_id (string), alert_id (string), customer_id (string), agent_name (string), disposition (string), analyst_id (string), timestamp (datetime), notes (string)
*   **Sample Data:**
    ```json
    {
      "disposition_id": "DISP-98213",
      "alert_id": "ALRT-1780900001",
      "customer_id": "CUST-0022",
      "agent_name": "Structuring Sentinel",
      "disposition": "TRUE_POSITIVE",
      "analyst_id": "ANALYST-04",
      "timestamp": "2026-06-08T10:30:00Z",
      "notes": "Structuring activity confirmed on account ACC-0022."
    }
    ```

#### 5. `fact_circular_flows`
*   **Columns:** flow_id (string), cycle_accounts (dynamic), cycle_customers (dynamic), hop_count (int), total_amount (real), avg_amount_per_hop (real), amount_consistency_pct (real), time_window_minutes (real), detected_at (datetime), status (string)
*   **Sample Data:**
    ```json
    {
      "flow_id": "FLOW-9821",
      "cycle_accounts": ["ACC-0022", "ACC-89201", "ACC-30492"],
      "cycle_customers": ["CUST-0022", "CUST-0046", "CUST-0004"],
      "hop_count": 3,
      "total_amount": 1320763.33,
      "avg_amount_per_hop": 1320763.33,
      "amount_consistency_pct": 99.8,
      "time_window_minutes": 45,
      "detected_at": "2026-06-12T09:23:40Z",
      "status": "ACTIVE"
    }
    ```

#### 6. `fact_risk_scores`
*   **Columns:** snapshot_id (string), customer_id (string), customer_name (string), risk_score (real), risk_tier (string), f1_agent_consensus (real), f2_graph_centrality (real), f3_dna_deviation (real), f4_historical_risk (real), f5_watchlist_match (real), active_agents (dynamic), snapshot_timestamp (datetime)
*   **Sample Data:**
    ```json
    {
      "snapshot_id": "SNAP-17809045",
      "customer_id": "CUST-0022",
      "customer_name": "Bhavika Tank",
      "risk_score": 68.45,
      "risk_tier": "HIGH",
      "f1_agent_consensus": 0.92,
      "f2_graph_centrality": 0.42,
      "f3_dna_deviation": 2.1,
      "f4_historical_risk": 0.15,
      "f5_watchlist_match": 0.0,
      "active_agents": ["Structuring Sentinel", "Geo-Temporal Analyzer"],
      "snapshot_timestamp": "2026-06-12T09:23:40Z"
    }
    ```

#### 7. `dim_customer`
*   **Columns:** customer_id (string), name (string), risk_score (real), kyc_status (string), pep_flag (bool), country_code (string), city (string), created_date (datetime)
*   **Sample Data:**
    ```json
    {
      "customer_id": "CUST-0022",
      "name": "Bhavika Tank",
      "risk_score": 0.15,
      "kyc_status": "Verified",
      "pep_flag": false,
      "country_code": "IN",
      "city": "Mumbai",
      "created_date": "2024-01-15T00:00:00Z"
    }
    ```

#### 8. `dim_account`
*   **Columns:** account_id (string), customer_id (string), account_type (string), balance (real), velocity_index (real), opened_date (datetime), dormancy_score (real)
*   **Sample Data:**
    ```json
    {
      "account_id": "ACC-0022",
      "customer_id": "CUST-0022",
      "account_type": "Savings",
      "balance": 2504300.50,
      "velocity_index": 1.2,
      "opened_date": "2024-01-16T00:00:00Z",
      "dormancy_score": 0.05
    }
    ```

#### 9. `dim_merchant`
*   **Columns:** merchant_id (string), merchant_name (string), mcc_code (string), category (string), risk_tier (string), shell_score (real), registered_date (datetime)
*   **Sample Data:**
    ```json
    {
      "merchant_id": "MERC-0010",
      "merchant_name": "Grand Star Casino Ltd",
      "mcc_code": "7995",
      "category": "Gambling",
      "risk_tier": "High",
      "shell_score": 0.82,
      "registered_date": "2025-11-20T00:00:00Z"
    }
    ```

#### 10. `dim_device`
*   **Columns:** device_id (string), device_type (string), first_seen (datetime), last_seen (datetime), customer_ids (dynamic)
*   **Sample Data:**
    ```json
    {
      "device_id": "DEV-0022",
      "device_type": "MOBI",
      "first_seen": "2026-01-01T08:00:00Z",
      "last_seen": "2026-06-12T09:23:40Z",
      "customer_ids": ["CUST-0022", "CUST-0046"]
    }
    ```

#### 11. `dim_ip_address`
*   **Columns:** ip_address (string), geo_location (string), is_vpn (bool), is_tor (bool), customer_ids (dynamic)
*   **Sample Data:**
    ```json
    {
      "ip_address": "192.168.1.56",
      "geo_location": "BTM Layout, Bangalore",
      "is_vpn": false,
      "is_tor": false,
      "customer_ids": ["CUST-0022"]
    }
    ```

#### 12. `dim_behavioral_dna`
*   **Columns:** customer_id (string), time_window (string), velocity (real), amount_profile (real), temporal_pattern (real), counterparty_diversity (real), channel_mix (real), geographic_spread (real), amount_entropy (real), round_number_ratio (real), counterparty_recurrence (real), dormancy_burst (real), cross_border_ratio (real), device_switching (real), drift_score (real), computed_at (datetime)
*   **Sample Data:**
    ```json
    {
      "customer_id": "CUST-0022",
      "time_window": "30d",
      "velocity": 3.42,
      "amount_profile": 2.15,
      "temporal_pattern": 1.05,
      "counterparty_diversity": 0.88,
      "channel_mix": 0.45,
      "geographic_spread": 2.95,
      "amount_entropy": 1.25,
      "round_number_ratio": 0.20,
      "counterparty_recurrence": 0.65,
      "dormancy_burst": 0.0,
      "cross_border_ratio": 0.05,
      "device_switching": 1.50,
      "drift_score": 3.12,
      "computed_at": "2026-06-12T09:00:00Z"
    }
    ```

#### 13. `dim_adaptive_config`
*   **Columns:** config_key (string), config_value (real), description (string), last_updated (datetime), updated_by (string)
*   **Sample Data:**
    ```json
    {
      "config_key": "agent.structuring.amount_threshold",
      "config_value": 1000000.0,
      "description": "CTR reporting threshold for smurfing detection.",
      "last_updated": "2026-06-12T00:00:00Z",
      "updated_by": "L9_recalibration"
    }
    ```

---

### 4. AUDIT INSTRUCTIONS & CHECKS

Analyze the files in detail and check for the following:

#### A. Mathematical & Algorithmic Correctness
1.  **Behavioral DNA Calculations (`notebooks/L3_behavioral_dna.py`):**
    *   Inspect how standard deviations ($\sigma$) and drift scores are computed.
    *   Verify if there are edge cases (e.g. division by zero, empty transaction history for new customers, log of zero, negative entropy).
    *   Ensure 12-dimensional vectors are correctly normalized.
2.  **Graph Analysis & Louvain Partitioning (`notebooks/L4_graph_analysis.py`):**
    *   Verify the NetworkX implementation of PageRank and Louvain community detection.
    *   Ensure temporal versioning of graph edges (e.g. timestamps on transactions) is properly parsed.
    *   Check for potential memory scale issues when building the NetworkX graph in a PySpark environment.
3.  **L9 Recalibration Mathematics (`notebooks/L9_recalibration.py`):**
    *   Verify the threshold update formula. Does it prevent weight scores from growing infinitely or dropping below 0?
    *   Ensure that true/false positive counts do not trigger division-by-zero errors.
    *   Verify the safety limits (clamping weights between 0 and 100, ensuring composite weights sum exactly to 100).

#### B. Configuration, Hardcoding, & Secrets Leakage
1.  **Parameters and Constants:**
    *   Audit all Python files and KQL queries to ensure they query settings from `config.yaml` or `dim_adaptive_config` rather than containing hardcoded parameters (e.g. looking for hardcoded thresholds, static weights, static database names, or cluster connection strings).
2.  **Credentials & Secrets:**
    *   Ensure no active secrets, tokens, or API keys are written in any config files, code, or markdown walkthroughs. Everything must load from environment variables (`os.environ`) or Azure Key Vault via `config_loader.py`.

#### C. Data Schema & Ingestion Consistency
1.  **Schema Alignment:**
    *   Compare the output schemas of `data_simulator/simulator.py` (the generated transaction dictionary) against the KQL table schemas defined in `kql/L2_eventhouse_ddl.kql` and the Fabric Spark DataFrame reads in the `notebooks/` folder.
    *   Ensure data types match exactly (e.g., `real` vs `double`, `datetime` formats, `bool` vs `int`).
2.  **Eventstream Path Integrity:**
    *   Verify that the EventHub uploader and simulator handle payloads in a way that matches the Eventstream JSON ingestion mapping schema (`pipelines/` and `upload_ready/`).

#### D. Sample Data Calculation Verification
1.  Verify if the mock data generated by `dimension_generator.py` and `scenario_engine.py` contains enough data variety to perfectly execute every step of the Spark notebooks and KQL queries without falling back to blank data frames or failing joins.

---

### 5. EXPECTED OUTPUT FORMAT

Please compile your findings into a structured, highly actionable Markdown report containing:

1.  **Executive Summary:** A summary of the general codebase health.
2.  **Critical & High Issues:** Flaws that would cause runtime crashes, calculation failures, or security breaches (such as credentials leakage).
3.  **Medium & Low Issues:** Optimization, parameterization, and performance suggestions.
4.  **Mathematical Corrections:** Step-by-step breakdown of how to fix DNA, Graph, or Recalibration formulas.
5.  **Unified Git Diffs:** Provide clean git diff blocks showing exactly what lines of code to replace in specific files to fix the identified bugs.
6.  **Next Steps:** Suggested steps to verify the updates.
```
***

Use the prompt above to initialize the audit with Claude Fable 5!
