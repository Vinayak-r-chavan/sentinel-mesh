# 🛡️ SENTINEL MESH V2 — Master Implementation Plan
## AI-Powered, Zero Hard-Code, Fully Automated AML Detection on Azure + Fabric

> **Version**: 2.0 (Complete Rebuild)
> **Date**: June 2026
> **Platform**: Microsoft Fabric + Azure AI Services
> **Goal**: Eliminate ALL hard-coded values, implement full automation, leverage AI agents across all 9 layers

---

## 1. Why V2? — Problems With V1

The original SENTINEL MESH implementation was a **hard-coded demo**. It looked good but couldn't adapt to real data or new scenarios.

### V1 Flaws Summary

| # | Flaw | Count | Impact |
|---|------|-------|--------|
| 1 | Hard-coded test scenarios (Raj Kumar, Priya, Circular) | 5+ scripts | Cannot test with new data |
| 2 | Hard-coded API credentials in source code | 3 files | Security risk |
| 3 | Hard-coded risk scoring weights (all 35.0) | L6 KQL | Incorrect risk calculations |
| 4 | Hard-coded detection thresholds | L5 KQL | No adaptability |
| 5 | Hard-coded dimension data (CSV generators) | 2 files | Fixed fake customers |
| 6 | Hard-coded file paths (absolute Windows paths) | 6+ files | Not portable |
| 7 | No config management system | Entire project | Values scattered everywhere |
| 8 | No feedback loop (L9) | Architecture gap | System cannot learn |
| 9 | No dashboard | Missing entirely | No monitoring |
| 10 | Incomplete L8 (Action Gateway) | Only guide written | No automated alerts |
| 11 | Static Behavioral DNA (hand-typed vectors) | 1 file | Not calculated from data |
| 12 | Static confidence scores in agents | L5 KQL | Magic numbers, not statistical |

### V2 Principle: **Nothing is hard-coded. Everything is config-driven, data-computed, or AI-generated.**

---

## 2. V2 Architecture Overview

### 2.1 The 9-Layer Architecture (Enhanced)

```
┌──────────────────────────────────────────────────────────────────────────┐
│                     SENTINEL MESH V2 — 9-Layer Architecture             │
│                                                                          │
│  L1  DATA INGESTION ──→ Eventstreams + Event Grid + Document Intelligence│
│         │                                                                │
│  L2  SEMANTIC LAKEHOUSE ──→ Eventhouse (Hot) + Lakehouse (Warm) + OneLake│
│         │                                                                │
│  L3  IQ ONTOLOGY CORE ──→ 10 Entity Types + 12D DNA + Temporal Versioning│
│         │                                                                │
│  L4  TEMPORAL KNOWLEDGE GRAPH ──→ KQL make-graph + Cosmos DB Graph API  │
│         │                                                                │
│  L5  AI AGENT SWARM ──→ 6 AI Agents (Semantic Kernel) + ML Detection    │
│         │                                                                │
│  L6  ADAPTIVE RISK SCORING ──→ Config-driven weights + Statistical scores│
│         │                                                                │
│  L7  AML COPILOT ──→ Fabric Data Agent + Azure AI Agent Service         │
│         │                                                                │
│  L8  ACTION GATEWAY ──→ Activator + Logic Apps + Power Automate         │
│         │                                                                │
│  L9  SELF-LEARNING LOOP ──→ Disposition capture + Auto-recalibration    │
│                                                                          │
│  CROSS-CUTTING: Key Vault │ App Config │ Purview │ Monitor │ Defender   │
└──────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Complete Azure Services Map

```
┌──────────────────────────────────────────────────────────────────────┐
│                      AZURE SERVICES USED                             │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ── FABRIC SERVICES ──                                               │
│  📡 Eventstream           Real-time transaction ingestion            │
│  💾 Eventhouse (KQL DB)   Hot transaction queries + AI plugins       │
│  🏠 Lakehouse (Delta)     Dimensions + config + DNA tables           │
│  📓 Notebooks (Spark)     DNA calculation + graph analysis           │
│  🔀 Data Pipeline         End-to-end orchestration                   │
│  🚨 Activator             Real-time alert triggers                   │
│  📊 Power BI (DirectLake) Live risk dashboard                        │
│  🤖 Data Agent            NL investigation copilot                   │
│  ⚙️ Operations Agent      Infrastructure health monitoring           │
│                                                                      │
│  ── AZURE AI SERVICES ──                                             │
│  🧠 Azure OpenAI (gpt-4.1)    SAR generation + agent reasoning      │
│  🤖 Azure AI Agent Service     Master investigation agent            │
│  📄 Document Intelligence      KYC document extraction               │
│  🛡️ Content Safety             Validate AI outputs                   │
│                                                                      │
│  ── AZURE PLATFORM SERVICES ──                                       │
│  🔐 Key Vault                  All secrets (API keys, conn strings)  │
│  ⚙️ App Configuration          All thresholds, weights, parameters   │
│  📦 Blob Storage               Scenario templates, output files      │
│  ⚡ Azure Functions             Event-driven SAR + webhook handlers  │
│  📡 Event Grid                 Central event routing                  │
│  🔄 Azure Cache for Redis      Fast config reads                     │
│  🐳 Container Apps             Cloud-hosted simulator                │
│  🌐 API Management             Unified REST API layer                │
│  📡 SignalR Service            Real-time dashboard push              │
│  📋 Logic Apps                 Complex compliance workflows          │
│  🔑 Azure Identity             Passwordless auth (DefaultAzureCredential) │
│                                                                      │
│  ── GOVERNANCE & SECURITY ──                                         │
│  📊 Microsoft Purview          Data lineage + classification + audit │
│  🔍 Azure Monitor + Log Analytics   Observability + custom metrics   │
│  🛡️ Defender for Cloud         Security posture monitoring           │
│                                                                      │
│  ── AI FRAMEWORKS ──                                                 │
│  🧩 Semantic Kernel            Multi-agent swarm orchestration       │
│  🗣️ AutoGen (optional)         Multi-agent debate/consensus         │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 3. Directory Structure (V2)

```
hack2future/
└── sentinel_mesh_v2/
    ├── SENTINEL_MESH_V2_MASTER_PLAN.md    # This file
    │
    ├── config/                             # ALL configuration lives here
    │   ├── config.yaml                     # Thresholds, weights, parameters
    │   ├── .env                            # Only AZURE_KEYVAULT_URL
    │   ├── scenarios/                      # AML pattern templates
    │   │   ├── structuring.json
    │   │   ├── circular_flow.json
    │   │   ├── shadow_link.json
    │   │   ├── velocity_spike.json
    │   │   ├── shell_merchant.json
    │   │   └── dormant_activation.json
    │   └── config_loader.py                # Unified config reader (Key Vault + App Config + YAML)
    │
    ├── data_simulator/                     # Dynamic transaction generator
    │   ├── simulator.py                    # Main simulator (config-driven)
    │   ├── scenario_engine.py              # Reads scenario templates, generates dynamic events
    │   ├── dimension_generator.py          # Generates customers/accounts/merchants from config
    │   ├── uploader.py                     # Uploads dimensions to Lakehouse via API
    │   └── requirements.txt
    │
    ├── kql/                                # KQL queries (AI-enhanced)
    │   ├── L2_eventhouse_ddl.kql           # Table schemas (enhanced)
    │   ├── L5_swarm_agents_v2.kql          # AI-powered agents (series_decompose, autocluster)
    │   ├── L6_risk_scoring_v2.kql          # Config-driven weights from dim_adaptive_config
    │   └── L4_graph_queries.kql            # Native KQL make-graph / graph-match
    │
    ├── notebooks/                          # Spark notebooks
    │   ├── L3_behavioral_dna.py            # Auto-computed 12D DNA from real transactions
    │   ├── L4_graph_analysis.py            # Graph cycle + community detection
    │   ├── L9_recalibration.py             # Feedback-driven threshold adjustment
    │   └── entity_resolution.py            # Deduplication + fuzzy matching
    │
    ├── agents/                             # AI Agent definitions
    │   ├── investigation_agent.py          # Azure AI Agent Service — master investigator
    │   ├── swarm_orchestrator.py           # Semantic Kernel — 6-agent coordination
    │   ├── sar_generator.py                # Structured SAR output via Azure OpenAI
    │   └── triage_agent.py                 # Auto-triage incoming alerts
    │
    ├── copilot/                            # Fabric Data Agent configuration
    │   ├── semantic_model_config.json       # Tables + relationships for grounding
    │   └── copilot_instructions.md         # System prompt for AML copilot
    │
    ├── dashboard/                          # Real-time monitoring UI
    │   ├── index.html                      # Live risk dashboard
    │   ├── signalr_client.js               # Real-time update via SignalR
    │   └── dashboard.css
    │
    ├── pipelines/                          # Fabric Data Pipeline definitions
    │   ├── full_pipeline.json              # End-to-end orchestration
    │   ├── dna_refresh_pipeline.json       # Scheduled DNA recalculation
    │   └── recalibration_pipeline.json     # L9 feedback → threshold update
    │
    ├── workflows/                          # Logic Apps / Power Automate flows
    │   ├── critical_alert_workflow.json     # Score > 80 → freeze + SAR + notify
    │   ├── weekly_report_workflow.json      # Weekly compliance summary
    │   └── disposition_workflow.json        # Analyst feedback → L9 trigger
    │
    ├── api/                                # Azure API Management definitions
    │   ├── api_spec.yaml                   # OpenAPI spec for SENTINEL MESH API
    │   └── functions/                      # Azure Functions (API backends)
    │       ├── get_risk_score/
    │       ├── submit_disposition/
    │       ├── trigger_investigation/
    │       └── health_check/
    │
    ├── infrastructure/                     # Azure resource definitions
    │   ├── deploy.sh                       # One-command Azure resource provisioning
    │   └── resource_inventory.md           # All Azure resources and their purpose
    │
    └── docs/                               # Documentation
        ├── architecture_diagram.html        # Updated interactive diagram
        ├── azure_services_map.md           # Which Azure service does what
        └── demo_script.md                  # Step-by-step hackathon demo guide
```

---

## 4. Layer-by-Layer Implementation Plan

---

### LAYER 1: Data Ingestion Fabric

#### What Changes From V1
- V1: Single Eventstream with Custom App source, local simulator only
- V2: Event Grid routing, Document Intelligence for KYC, cloud-hosted simulator

#### Components to Build

**1.1 Eventstream (es_transactions) — Enhanced**
- Keep existing Eventstream with Custom App source
- Add **in-flight transformations**: schema validation, field normalization, deduplication
- Add **derived stream** for real-time aggregates (rolling 1-hour velocity per customer)

**1.2 Event Grid — Central Event Router**
- Create Event Grid Topic: `sentinel-mesh-events`
- Subscribe all downstream services to relevant events
- Event types:
  - `transaction.ingested` → triggers agent detection
  - `risk.critical` → triggers SAR generation + account freeze
  - `risk.high` → triggers analyst notification
  - `disposition.submitted` → triggers L9 recalibration
  - `dna.drifted` → triggers enhanced monitoring
  - `system.unhealthy` → triggers operations agent

**1.3 Document Intelligence — KYC Ingestion**
- Azure AI Document Intelligence reads KYC documents (PAN cards, Aadhaar, bank statements)
- Extracts structured data → auto-populates `dim_customer` table
- Eliminates manual customer data entry

**1.4 Container Apps — Cloud Simulator**
- Containerize the simulator
- Deploy to Azure Container Apps
- Runs 24/7 in the cloud, configurable via environment variables
- Auto-scales for high-volume testing
- Demo-safe: runs independently of your laptop

---

### LAYER 2: Unified Semantic Lakehouse

#### What Changes From V1
- V1: Basic `fact_transactions` table + 3 dimension CSVs uploaded manually
- V2: Enhanced schema, config tables, automated upload, data quality monitoring

#### Components to Build

**2.1 Eventhouse Tables (Enhanced Schema)**

`fact_transactions` — same as V1 plus:
- `ingestion_timestamp` — when the system received it (vs when it happened)
- `processing_status` — tracked through the pipeline
- `enrichment_flags` — what enrichments have been applied

New tables:
- `fact_alerts` — every agent finding with calculated confidence
- `fact_sar_reports` — AI-generated SAR narratives with metadata
- `fact_analyst_dispositions` — TP/FP/ESCALATE feedback from analysts
- `dim_adaptive_config` — dynamic thresholds, weights, parameters (replaces hard-coding)

**2.2 Lakehouse Tables (Enhanced)**

Existing (auto-generated, not hand-typed):
- `dim_customer` — dynamically generated from Faker + config
- `dim_account` — dynamically generated
- `dim_merchant` — dynamically generated

New:
- `dim_behavioral_dna` — auto-computed from transactions (12 dimensions)
- `dim_devices` — first-class entity for shadow link detection
- `dim_ip_addresses` — first-class entity for network fingerprinting
- `dim_locations` — first-class entity for geo analysis
- `dim_adaptive_config` — scoring weights, agent thresholds (readable by KQL)
- `fact_circular_flows` — detected circular fund loops
- `fact_entity_resolution` — merged/linked customer records

**2.3 Automated Dimension Upload**
- `dimension_generator.py` generates data from config (pool sizes, distributions)
- `uploader.py` pushes to Lakehouse via OneLake API or Spark
- No manual CSV upload to Fabric portal ever again

---

### LAYER 3: IQ Ontology Core (The Brain)

#### What Changes From V1
- V1: 4 entity types, 6D DNA hand-typed, no drift detection, no versioning
- V2: 10 entity types, 12D DNA auto-computed, multi-resolution, temporal versioning

#### 3.1 Expanded Entity Types

| Entity | Properties | Source |
|--------|-----------|--------|
| **Customer** | customer_id, name, risk_score, kyc_status, pep_flag, country_code, behavioral_dna | Lakehouse `dim_customer` |
| **Account** | account_id, type, balance, velocity_index, opened_date, dormancy_score | Lakehouse `dim_account` |
| **Transaction** | txn_id, amount, timestamp, channel, pattern_hash, risk_flag, geo_location | Eventhouse `fact_transactions` |
| **Merchant** | merchant_id, mcc_code, category, risk_tier, shell_score | Lakehouse `dim_merchant` |
| **Device** 🆕 | device_id, device_type, first_seen, last_seen, customer_ids[] | Lakehouse `dim_devices` |
| **IP Address** 🆕 | ip_address, geo_location, is_vpn, is_tor, customer_ids[] | Lakehouse `dim_ip_addresses` |
| **Location** 🆕 | location_id, city, state, country, risk_classification | Lakehouse `dim_locations` |
| **Alert** 🆕 | alert_id, agent_name, confidence, reason, status, disposition | Eventhouse `fact_alerts` |
| **Case** 🆕 | case_id, customer_id, alerts[], sar_id, status, assigned_analyst | Lakehouse `fact_cases` |
| **Beneficiary** 🆕 | beneficiary_id, name, relationship_to_sender, jurisdiction | Lakehouse `dim_beneficiaries` |

#### 3.2 Expanded Relationships

| Relationship | From → To | AML Significance |
|---|---|---|
| `owns` | Customer → Account | Basic ownership |
| `transacts` | Account → Account | Money flow edge |
| `uses_device` 🆕 | Customer → Device | Device fingerprinting |
| `connects_from` 🆕 | Customer → IP Address | Network fingerprinting |
| `located_at` 🆕 | Transaction → Location | Geo analysis |
| `beneficial_owner_of` 🆕 | Customer → Account | Hidden ownership |
| `related_to` 🆕 | Customer → Customer | Declared relationships |
| `suspected_linked` 🆕 | Customer → Customer | AI-discovered undeclared links |
| `employed_by` 🆕 | Customer → Merchant | Employee-merchant fraud |
| `triggered` 🆕 | Transaction → Alert | What triggered the alert |
| `escalated_to` 🆕 | Alert → Case | Investigation lifecycle |

#### 3.3 Behavioral DNA V2 — 12 Dimensions

| # | Dimension | What It Captures | How It's Calculated |
|---|-----------|-----------------|---------------------|
| 1 | Velocity | Transaction frequency | count(txns) per time window |
| 2 | Amount Profile | Average transaction size | avg(amount) |
| 3 | Temporal Pattern | Time-of-day concentration | stddev(hour_of_day) |
| 4 | Counterparty Diversity | Unique recipients/senders | countDistinct(counterparty) |
| 5 | Channel Mix | Digital vs physical ratio | sum(UPI+Mobile) / count(all) |
| 6 | Geographic Spread | Location entropy | countDistinct(location) |
| 7 | **Amount Entropy** 🆕 | Amount variation | entropy of amount distribution |
| 8 | **Round Number Ratio** 🆕 | Percentage round amounts | count(round_amounts) / count(all) |
| 9 | **Counterparty Recurrence** 🆕 | Repeated counterparties | max(count_per_counterparty) / count(all) |
| 10 | **Dormancy-Burst** 🆕 | Inactive then sudden burst | max_gap_between_txns / avg_gap |
| 11 | **Cross-Border Ratio** 🆕 | International transactions | count(foreign_txns) / count(all) |
| 12 | **Device Switching** 🆕 | Device diversity | countDistinct(device_id) / count(txns) |

#### 3.4 Multi-Resolution DNA

DNA is computed at **5 time windows** simultaneously:

| Window | Name | Purpose |
|--------|------|---------|
| 1 hour | DNA-1H | Detect burst activity in real-time |
| 24 hours | DNA-1D | Detect daily anomalies |
| 7 days | DNA-7D | Detect weekly pattern shifts |
| 30 days | DNA-30D | Behavioral baseline |
| 90 days | DNA-90D | Long-term profile |

**Drift Score** = distance(DNA-1D, DNA-30D baseline). If drift exceeds configurable threshold → trigger investigation.

#### 3.5 Temporal Versioning
- Every DNA calculation is timestamped and stored as a new row (not overwritten)
- Enables point-in-time queries: "What was this customer's DNA on May 15th?"
- Enables trend analysis: "Show me how this customer's behavior changed over 3 months"
- State transitions tracked: LOW → MEDIUM → HIGH risk evolution

#### 3.6 Entity Resolution
- Spark notebook compares DNA vectors between customers
- Fuzzy name matching (Levenshtein distance)
- Shared device/IP detection
- Customers with similar DNA + shared infrastructure → flagged as potential same person
- Merged into super-entity with combined risk assessment

---

### LAYER 4: Temporal Knowledge Graph

#### What Changes From V1
- V1: Spark DataFrame joins for cycle detection (slow, hard-coded mock fallback)
- V2: KQL native graph operators (sub-second) + optional Cosmos DB for complex traversals

#### Components to Build

**4.1 KQL Native Graph (Primary)**
- Use `make-graph` and `graph-match` operators directly in Eventhouse
- Sub-second circular flow detection for 3-10 hop cycles
- Community detection to find clusters of connected accounts
- Centrality analysis (PageRank) to find hub accounts

**4.2 Cosmos DB Graph API (Optional Enhancement)**
- Dedicated graph database for complex multi-hop traversals
- Store customer-account-device-IP relationships as persistent graph
- Native Gremlin queries for advanced pattern discovery
- Real-time edge updates as transactions arrive

**4.3 Graph Algorithms**

| Algorithm | AML Application | Implementation |
|-----------|----------------|----------------|
| Cycle Detection | Circular fund flows (A→B→C→A) | KQL `graph-match` with variable-length paths |
| Community Detection | Find fraud rings (clusters of tightly connected accounts) | Spark GraphFrames or Cosmos DB |
| Centrality (PageRank) | Find hub accounts that facilitate money movement | KQL or Spark |
| Shortest Path | Trace multi-hop fund flows for layering detection | KQL `graph-match` |
| Entity Resolution | Merge duplicate customers across datasets | Spark fuzzy matching |

---

### LAYER 5: AI Agent Swarm

#### What Changes From V1
- V1: 6 KQL functions with hard-coded thresholds and static confidence scores
- V2: 6 AI agents with statistical detection, config-driven thresholds, and reasoning capability

#### 5.1 The 6 Detection Agents (Enhanced)

**Agent 1: Structuring Sentinel**
- Detection: Multiple deposits below reporting threshold that collectively exceed it
- V1 flaw: Threshold hard-coded to ₹10L, confidence hard-coded to 0.95/0.85
- V2 fix: Threshold read from `dim_adaptive_config`, confidence calculated via z-score
- V2 enhancement: Use `autocluster` to discover structuring patterns automatically

**Agent 2: Velocity Anomaly Detector**
- Detection: Transaction volume/amount exceeding baseline DNA
- V1 flaw: Thresholds hard-coded (₹20L, ₹5L), confidence = 0.89
- V2 fix: Use `series_decompose_anomalies` for statistical anomaly detection
- V2 enhancement: Compare against multi-resolution DNA baselines (1D vs 30D drift)

**Agent 3: Geo-Temporal Analyzer**
- Detection: Impossible travel (same customer, distant locations, short time)
- V1 flaw: Time window hard-coded to 2 hours, confidence = 0.92
- V2 fix: Time window from config, confidence based on actual distance/time ratio
- V2 enhancement: Integrate with Location entity for geo-clustering

**Agent 4: Circular Flow Tracker**
- Detection: Round-trip money flows (A→B→C→A)
- V1 flaw: Hard-coded Spark joins, 3-hop only, confidence = 0.98
- V2 fix: KQL `make-graph` / `graph-match` for 3-10 hop detection, sub-second
- V2 enhancement: Detect near-circular flows (A→B→C→D where D is related to A)

**Agent 5: Shadow Link Discoverer**
- Detection: Hidden relationships via shared devices/IPs
- V1 flaw: Simple 1-hop detection, confidence = 0.94
- V2 fix: Multi-hop transitive closure (A shares device with B, B shares IP with C → A-B-C linked)
- V2 enhancement: Score link strength based on number of shared attributes

**Agent 6: Shell Entity Profiler**
- Detection: Shell companies, dormant-to-active accounts
- V1 flaw: Only checks MCC code 5999, confidence = 0.85
- V2 fix: Statistical shell score based on transaction diversity, counterparty concentration, merchant age
- V2 enhancement: Use `diffpatterns` to compare shell vs legitimate merchant profiles

#### 5.2 Swarm Orchestrator (Semantic Kernel)
- Each agent is a Semantic Kernel plugin/agent
- Orchestrator runs all 6 agents on a target customer
- Collects findings, resolves conflicts, calculates weighted consensus
- Agents can **reason and explain** — not just return data
- Orchestrator produces a consolidated investigation brief

#### 5.3 KQL AI Plugins Used

| KQL Plugin | Which Agent Uses It | What It Replaces |
|---|---|---|
| `series_decompose_anomalies` | Velocity, Structuring | Hard-coded thresholds |
| `autocluster` | All agents | Hard-coded pattern rules |
| `diffpatterns` | Shell Entity | Hard-coded MCC checks |
| `basket` | Structuring, Geo-Temporal | Hard-coded channel/location rules |
| `series_periods_detect` | Velocity | Hard-coded temporal analysis |
| `make-graph / graph-match` | Circular Flow, Shadow Link | Spark DataFrame joins |
| `evaluate python()` | All agents | Hard-coded confidence scores (uses sklearn IsolationForest) |

---

### LAYER 6: Adaptive Risk Scoring Engine

#### What Changes From V1
- V1: All 6 agents weighted 35.0, factor defaults hard-coded (0.10, 0.20, 0.15)
- V2: Weights from config table, factors computed statistically, tiers configurable

#### 6.1 5-Factor Risk Formula (Config-Driven)

| Factor | Weight (Default) | Source | V1 vs V2 |
|--------|-----------------|--------|-----------|
| F1: Agent Consensus | 35% | Average of agent confidence scores | V1: hard-coded 35 for each agent. V2: weighted average from config |
| F2: Graph Centrality | 20% | PageRank score from L4 graph | V1: hard-coded 0.10/0.95. V2: actual PageRank value |
| F3: DNA Deviation | 20% | Drift score (DNA-1D vs DNA-30D) | V1: hard-coded 0.20/0.85. V2: actual drift magnitude |
| F4: Historical Risk | 15% | Prior alerts, SARs, dispositions | V1: hard-coded 0.15/0.90. V2: actual historical data |
| F5: Watchlist Match | 10% | PEP/sanctions list match | V1: hard-coded 0.0/0.85. V2: actual watchlist query |

#### 6.2 Config Table: `dim_adaptive_config`

| config_key | config_value | last_updated | updated_by |
|---|---|---|---|
| `scoring.weight.f1` | 35.0 | 2026-06-01 | system_init |
| `scoring.weight.f2` | 20.0 | 2026-06-01 | system_init |
| `scoring.weight.f3` | 20.0 | 2026-06-01 | system_init |
| `scoring.weight.f4` | 15.0 | 2026-06-01 | system_init |
| `scoring.weight.f5` | 10.0 | 2026-06-01 | system_init |
| `scoring.tier.low` | 30.0 | 2026-06-01 | system_init |
| `scoring.tier.medium` | 60.0 | 2026-06-01 | system_init |
| `scoring.tier.high` | 80.0 | 2026-06-01 | system_init |
| `agent.structuring.threshold` | 1000000 | 2026-06-01 | system_init |
| `agent.velocity.sigma` | 3.0 | 2026-06-01 | system_init |
| `agent.geo.travel_minutes` | 120 | 2026-06-01 | system_init |

- KQL scoring function reads from this table using `externaldata()` or cross-database query
- L9 feedback loop updates these values automatically
- Change any value from Azure App Configuration → propagates to this table → scoring changes instantly

#### 6.3 Risk Tiers (Configurable)

| Score Range | Risk Level | Automated Action |
|---|---|---|
| 0–30 (configurable) | 🟢 LOW | Log only |
| 31–60 (configurable) | 🟡 MEDIUM | Enhanced monitoring |
| 61–80 (configurable) | 🟠 HIGH | Alert analyst + enhanced due diligence |
| 81–100 (configurable) | 🔴 CRITICAL | Auto-freeze + SAR generation + escalation |

---

### LAYER 7: AML Copilot

#### What Changes From V1
- V1: Manual Python scripts calling Azure OpenAI with hard-coded customer data
- V2: Fabric Data Agent for NL investigation + Azure AI Agent Service for autonomous investigation

#### 7.1 Fabric Data Agent (Interactive Copilot)
- Grounded in semantic model (all Lakehouse + Eventhouse tables)
- Analysts type plain English → agent auto-generates KQL/SQL → returns results
- Example interactions:
  - "Show all customers flagged by 2+ agents today"
  - "What's the risk trend for Raj Kumar this week?"
  - "Are there any new circular flows?"
  - "Compare DNA-1D vs DNA-30D for all HIGH risk customers"

#### 7.2 Azure AI Agent Service (Autonomous Investigator)
- Created in Azure AI Foundry
- Has tools: KQL query tool, Lakehouse SQL tool, Azure OpenAI
- Analyst says "Investigate customer CUST-RAJ-4892" → Agent autonomously:
  1. Queries transaction history
  2. Checks behavioral DNA and drift
  3. Finds shadow links (shared devices/IPs)
  4. Checks graph centrality
  5. Calculates risk score
  6. Generates investigation brief

#### 7.3 Structured SAR Generation
- Azure OpenAI with JSON mode / structured outputs
- SAR comes back as structured JSON with fields:
  - `executive_summary`
  - `financial_activity_analysis`
  - `red_flags[]` (each with type, description, evidence)
  - `recommended_actions[]`
  - `risk_assessment`
- Stored in `fact_sar_reports` Delta table with:
  - `generated_at` timestamp
  - `model_version` (gpt-4.1)
  - `prompt_hash` (for reproducibility)
  - `confidence_score`
- Content Safety validates every SAR before storing

---

### LAYER 8: Action & Compliance Gateway

#### What Changes From V1
- V1: Only a written guide, never actually built
- V2: Fully implemented with Fabric Activator + Logic Apps + Power Automate

#### 8.1 Fabric Activator Triggers

| Trigger Condition | Action |
|---|---|
| Risk score > 80 (CRITICAL) | Send Teams notification + Email + Create case |
| Circular flow detected | Log to `fact_alerts` + Notify compliance team |
| New shadow link discovered | Flag for review + Add to investigation queue |
| DNA drift > threshold | Enable enhanced monitoring |
| Structuring pattern detected | Alert analyst + Auto-generate SAR draft |

#### 8.2 Logic Apps — Complex Compliance Workflows

**Critical Alert Workflow**:
1. Risk score > 80 detected
2. → Freeze account via banking API call
3. → Generate SAR via Azure OpenAI
4. → Email compliance officer with SAR attached
5. → Create case in tracking system
6. → Log all actions to Purview audit trail
7. → If PEP involved → escalate to senior management

**Weekly Compliance Report Workflow**:
1. Scheduled every Monday 9am
2. → Aggregate all alerts from past week
3. → Generate summary via Azure OpenAI
4. → Attach all SARs generated
5. → Email to Chief Compliance Officer

**Disposition Workflow**:
1. Analyst submits TP/FP/ESCALATE
2. → Write to `fact_analyst_dispositions`
3. → Trigger L9 recalibration pipeline
4. → If ESCALATE → create new detection pattern

---

### LAYER 9: Self-Learning Feedback Loop

#### What Changes From V1
- V1: Not built at all
- V2: Fully implemented — analyst dispositions → auto-recalibration of everything

#### 9.1 Disposition Capture

`fact_analyst_dispositions` table:

| Column | Type | Description |
|--------|------|-------------|
| disposition_id | string | Unique ID |
| alert_id | string | Which alert was reviewed |
| customer_id | string | Which customer |
| agent_name | string | Which agent triggered it |
| disposition | string | TRUE_POSITIVE / FALSE_POSITIVE / ESCALATE |
| analyst_id | string | Who reviewed |
| timestamp | datetime | When reviewed |
| notes | string | Analyst comments |

#### 9.2 Auto-Recalibration (Scheduled Spark Notebook)

**What gets recalibrated**:

| What | How | Effect |
|---|---|---|
| **Agent weights** (F1) | Increase weight for agents with high TP rate, decrease for high FP rate | Better scoring accuracy |
| **Detection thresholds** | Relax thresholds for agents with high FP rate | Fewer false alarms |
| **DNA baselines** | Reset baseline for customers marked FP | Stop re-flagging the same customer |
| **Exclusion rules** | Create exclusion for patterns consistently marked FP | Known-good patterns not flagged |
| **Risk tier boundaries** | Adjust tier cutoffs based on TP/FP distribution | Better severity classification |

**Recalibration formula example**:
- If Structuring Sentinel has 80% TP rate → keep weight at 35%
- If Geo-Temporal Analyzer has 40% TP rate → reduce weight to 25%
- If Shadow Link has 95% TP rate → increase weight to 40%

Updated values written to `dim_adaptive_config` → KQL picks up new weights automatically.

#### 9.3 Feedback Paths

| Path | What Happens |
|---|---|
| L9 → L3 (Ontology) | DNA baselines updated, entity properties refined, new relationship types added |
| L9 → L5 (Agents) | Agent thresholds adjusted, new detection patterns learned |
| L9 → L6 (Scoring) | Factor weights recalibrated, tier boundaries shifted |

---

## 5. Config-Driven Architecture (Zero Hard-Coding)

### 5.1 config.yaml — All Parameters

```yaml
# ═══════════════════════════════════════════
# SENTINEL MESH V2 — Master Configuration
# ═══════════════════════════════════════════

# ── Azure Resources ──
azure:
  keyvault_url: "${AZURE_KEYVAULT_URL}"  # Only secret — rest fetched from Key Vault
  app_config_endpoint: "${AZURE_APP_CONFIG_ENDPOINT}"
  region: "centralindia"

# ── Data Simulator ──
simulator:
  customer_pool_size: 50
  account_pool_size: 80
  merchant_pool_size: 20
  locale: "en_IN"
  currency: "INR"
  normal_txn_amount_range: [500, 50000]
  streaming_interval_seconds: 2
  channels: ["Branch", "ATM", "Mobile", "UPI", "SWIFT", "POS"]
  channel_weights: [0.20, 0.15, 0.25, 0.25, 0.05, 0.10]

# ── Scenario Parameters (not hard-coded payloads) ──
scenarios:
  structuring:
    enabled: true
    deposit_count_range: [3, 8]
    individual_amount_range: [500000, 990000]
    total_threshold: 1000000
    time_spread_hours: 8
    geo_pool_size: 5

  circular_flow:
    enabled: true
    hop_count_range: [3, 7]
    amount_range: [1000000, 2000000]
    time_window_minutes: 60

  shadow_link:
    enabled: true
    shared_device_probability: 0.15
    shared_ip_probability: 0.10

  velocity_spike:
    enabled: true
    spike_multiplier: 5.0
    sigma_threshold: 3.0

  shell_merchant:
    enabled: true
    high_risk_mcc_codes: ["5999", "6051", "7995"]
    shell_score_threshold: 0.70

  dormant_activation:
    enabled: true
    dormancy_days: 90
    activation_txn_count: 5

# ── Risk Scoring ──
scoring:
  weights:
    f1_agent_consensus: 35
    f2_graph_centrality: 20
    f3_dna_deviation: 20
    f4_historical_risk: 15
    f5_watchlist_match: 10
  tiers:
    low: 30
    medium: 60
    high: 80
  recalibration_frequency_hours: 24

# ── Behavioral DNA ──
dna:
  dimensions: 12
  time_windows: ["1h", "1d", "7d", "30d", "90d"]
  drift_threshold: 2.5  # sigma
  refresh_frequency_minutes: 30

# ── Agent Thresholds ──
agents:
  structuring:
    amount_threshold: 1000000
    min_txn_count: 3
    time_window_hours: 24
  velocity:
    sigma_threshold: 3.0
    baseline_window_days: 30
  geo_temporal:
    max_travel_minutes: 120
  circular_flow:
    min_hops: 3
    max_hops: 10
    time_window_hours: 1
  shadow_link:
    max_hop_depth: 3
  shell_entity:
    shell_score_threshold: 0.70
    min_concentration_ratio: 0.80

# ── Alerting ──
alerting:
  critical_score_threshold: 80
  high_score_threshold: 60
  teams_webhook_secret_name: "teams-webhook-url"  # fetched from Key Vault
  email_recipients_secret_name: "alert-email-list"

# ── SAR Generation ──
sar:
  model: "gpt-4.1"
  max_tokens: 2000
  temperature: 0.2
  output_format: "structured_json"
  content_safety_enabled: true

# ── Fabric Resources ──
fabric:
  workspace: "SentinelMesh_AML_POC"
  eventhouse: "SentinelMesh_Eventhouse"
  lakehouse: "SentinelMesh_Lakehouse"
  eventstream: "es_transactions"
```

### 5.2 Secret Management

**Only in `.env`**:
```
AZURE_KEYVAULT_URL=https://sentinel-mesh-kv.vault.azure.net/
```

**Everything else in Azure Key Vault**:
| Secret Name | What It Stores |
|---|---|
| `aoai-api-key` | Azure OpenAI API key |
| `aoai-endpoint` | Azure OpenAI endpoint URL |
| `eventstream-conn-str` | Fabric Eventstream connection string |
| `fabric-sql-endpoint` | Fabric Lakehouse SQL endpoint |
| `teams-webhook-url` | Teams notification webhook |
| `alert-email-list` | Compliance officer email addresses |
| `cosmos-db-conn-str` | Cosmos DB connection (if used) |
| `redis-conn-str` | Redis cache connection |
| `app-config-conn-str` | App Configuration connection |

### 5.3 How Config Is Loaded (Unified)

```
.env (Key Vault URL only)
    ↓
Azure Key Vault (all secrets)
    ↓
Azure App Configuration (all runtime parameters)
    ↓
config.yaml (default values + structure)
    ↓
dim_adaptive_config table (L9-recalibrated values)
    ↓
config_loader.py merges all sources with priority:
    dim_adaptive_config > App Configuration > config.yaml > defaults
```

Priority order: L9-recalibrated values override App Configuration, which overrides YAML defaults. The system always uses the most up-to-date values.

---

## 6. Automation End-to-End

### 6.1 Fabric Data Pipeline — One-Click Orchestration

**Full Pipeline (runs everything end-to-end)**:

```
Step 1: Generate Dimensions
    → Run dimension_generator.py
    → Upload to Lakehouse via API

Step 2: Start Simulator (or verify running)
    → Check Container Apps health
    → Transactions flowing to Eventstream

Step 3: Verify Eventhouse Ingestion
    → KQL: fact_transactions | count
    → Assert count > 0

Step 4: Calculate Behavioral DNA
    → Execute L3 Spark notebook
    → Verify dim_behavioral_dna updated

Step 5: Run Graph Analysis
    → Execute L4 Spark notebook (or KQL graph queries)
    → Verify fact_circular_flows updated

Step 6: Run Agent Detection
    → Execute all 6 agent KQL functions
    → Write results to fact_alerts

Step 7: Calculate Risk Scores
    → Execute CalculateEnsembleRisk_v2()
    → Identify HIGH/CRITICAL customers

Step 8: Generate SARs
    → For each CRITICAL customer → Azure Function → OpenAI → fact_sar_reports

Step 9: Trigger Alerts
    → Activator checks risk scores
    → Teams/Email notifications sent

Step 10: Dashboard Refresh
    → Power BI DirectLake auto-refreshes
    → SignalR pushes updates to web dashboard
```

**DNA Refresh Pipeline (runs every 30 minutes)**:
```
Step 1: Execute L3 DNA notebook
Step 2: Compare new DNA vs baseline → calculate drift
Step 3: If drift > threshold → trigger agent re-scan
Step 4: Update dim_behavioral_dna
```

**Recalibration Pipeline (runs daily or on disposition)**:
```
Step 1: Read fact_analyst_dispositions (last 24h)
Step 2: Calculate per-agent TP/FP rates
Step 3: Recalculate optimal weights and thresholds
Step 4: Update dim_adaptive_config
Step 5: Log changes to audit trail (Purview)
```

---

## 7. Observability & Governance

### 7.1 Azure Monitor — Custom AML Metrics

| Metric | What It Tracks |
|---|---|
| `sentinel.detection_latency_ms` | Time from transaction → risk score |
| `sentinel.transactions_per_second` | Throughput |
| `sentinel.alerts_per_hour` | Alert volume |
| `sentinel.agent_hit_rate` | Per-agent detection frequency |
| `sentinel.false_positive_rate` | From L9 dispositions |
| `sentinel.sar_generation_time_ms` | SAR generation speed |
| `sentinel.dna_drift_magnitude` | Average DNA drift across customers |
| `sentinel.pipeline_execution_time_s` | End-to-end pipeline duration |

### 7.2 Microsoft Purview — Compliance

| Capability | Application |
|---|---|
| Data Lineage | Transaction → Eventhouse → Agent → Score → SAR (full visual trail) |
| Data Classification | Auto-tag PII (names, accounts, IPs) as sensitive |
| Access Policies | Role-based access to risk data |
| Audit Trail | Every query, every SAR, every disposition logged |

### 7.3 Defender for Cloud — Security
- Monitor Key Vault access patterns
- Detect unusual API calls to Azure OpenAI
- Alert on unauthorized access to customer data
- Security score for entire SENTINEL MESH deployment

---

## 8. Real-Time Dashboard

### 8.1 Components
- **Power BI with DirectLake** — primary analytics dashboard (charts, tables, heatmaps)
- **Custom HTML Dashboard** — live ops view with SignalR real-time push
- **SignalR** — pushes instant updates when alerts fire

### 8.2 Dashboard Panels

| Panel | Data Source | Update Frequency |
|---|---|---|
| Risk Heatmap (all customers) | `CalculateEnsembleRisk_v2()` | Every 10 seconds |
| Agent Alert Feed | `fact_alerts` | Real-time (SignalR) |
| Transaction Volume Sparkline | `fact_transactions` | Every 5 seconds |
| DNA Drift Monitor | `dim_behavioral_dna` | Every 30 minutes |
| Active Cases | `fact_cases` | Real-time |
| SAR Generation Log | `fact_sar_reports` | Real-time |
| System Health | Azure Monitor | Every 30 seconds |
| Circular Flow Graph | `fact_circular_flows` | Every minute |

---

## 9. API Layer

### 9.1 Azure API Management Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/api/transactions` | POST | Inject transactions |
| `/api/risk/{customer_id}` | GET | Get real-time risk score |
| `/api/risk/all` | GET | Get all customer risk scores |
| `/api/alerts` | GET | Get active alerts |
| `/api/alerts/{alert_id}/disposition` | POST | Submit TP/FP feedback |
| `/api/investigate/{customer_id}` | POST | Trigger AI investigation |
| `/api/sar/{case_id}` | GET | Get generated SAR |
| `/api/config/{key}` | GET/PUT | Read/update config |
| `/api/health` | GET | System health check |
| `/api/metrics` | GET | AML metrics dashboard data |

---

## 10. Implementation Phases

### Phase 1: Foundation (Days 1-3)
- [ ] Create Azure resources (Key Vault, App Config, Blob Storage)
- [ ] Set up config.yaml + config_loader.py
- [ ] Store all secrets in Key Vault
- [ ] Build dynamic dimension generator (Faker-based)
- [ ] Build config-driven simulator with scenario engine
- [ ] Set up Fabric Eventstream + Eventhouse (enhanced schema)
- [ ] Verify end-to-end data flow: Simulator → Eventstream → Eventhouse

### Phase 2: Intelligence Core (Days 4-6)
- [ ] Build 12D Behavioral DNA Spark notebook (auto-computed)
- [ ] Implement multi-resolution DNA (1H/1D/7D/30D/90D)
- [ ] Implement KQL graph queries (make-graph / graph-match)
- [ ] Rewrite L5 agents with KQL AI plugins (series_decompose, autocluster)
- [ ] Rewrite L6 scoring with config-driven weights from dim_adaptive_config
- [ ] Set up dim_adaptive_config table with initial values
- [ ] Verify: agents detect patterns with dynamic confidence scores

### Phase 3: AI Agents & Copilot (Days 7-9)
- [ ] Set up Azure AI Agent Service (investigation agent)
- [ ] Set up Semantic Kernel swarm orchestrator
- [ ] Build structured SAR generation via Azure OpenAI
- [ ] Set up Fabric Data Agent (grounded in semantic model)
- [ ] Set up Content Safety validation
- [ ] Verify: analyst can investigate via copilot, SARs are auto-generated

### Phase 4: Automation & Action (Days 10-12)
- [ ] Set up Fabric Activator triggers
- [ ] Build Logic Apps compliance workflows
- [ ] Set up Event Grid event routing
- [ ] Build Fabric Data Pipeline (full orchestration)
- [ ] Set up DNA refresh pipeline (scheduled)
- [ ] Deploy simulator to Azure Container Apps
- [ ] Set up API Management layer
- [ ] Verify: end-to-end automation — transaction in → alert out → SAR generated

### Phase 5: Feedback & Observability (Days 13-14)
- [ ] Build L9 feedback capture (fact_analyst_dispositions)
- [ ] Build recalibration Spark notebook
- [ ] Set up recalibration pipeline
- [ ] Set up Azure Monitor custom metrics
- [ ] Set up Microsoft Purview data lineage
- [ ] Set up Defender for Cloud
- [ ] Verify: submit disposition → thresholds auto-adjust

### Phase 6: Dashboard & Demo (Days 15-16)
- [ ] Build real-time Power BI dashboard (DirectLake)
- [ ] Build custom HTML dashboard with SignalR
- [ ] Create demo script (step-by-step)
- [ ] Full end-to-end rehearsal
- [ ] Verify everything works independently (no laptop dependency)

---

## 11. V1 vs V2 Comparison

| Aspect | V1 (Old) | V2 (New) |
|--------|----------|----------|
| **Hard-coded values** | 50+ across 12 files | Zero |
| **Config management** | None | Key Vault + App Config + config.yaml + Delta table |
| **Test data** | 5 hand-typed scenarios | Dynamic Faker + scenario templates |
| **Entity types** | 4 | 10 |
| **DNA dimensions** | 6 (hand-typed) | 12 (auto-computed) |
| **DNA resolution** | Single snapshot | 5 time windows |
| **Graph analysis** | Spark joins (slow) | KQL make-graph (sub-second) |
| **Agent confidence** | Magic numbers | Statistical z-scores |
| **Scoring weights** | All 35.0 | Config-driven + L9-recalibrated |
| **SAR generation** | Manual script | Autonomous AI agent |
| **Alerting** | Not built | Fabric Activator + Logic Apps |
| **Feedback loop** | Not built | Full L9 with auto-recalibration |
| **Dashboard** | Not built | Power BI + SignalR real-time |
| **API layer** | Not built | Azure API Management |
| **Observability** | Not built | Azure Monitor + Purview + Defender |
| **Deployment** | Local laptop | Azure Container Apps (cloud) |
| **AI agents** | None | 4 (Investigation, Swarm, SAR, Triage) |
| **Azure services** | 3 (Eventstream, Eventhouse, Lakehouse) | 20+ |
| **Automation** | Manual script execution | Fabric Pipeline (one-click) |
| **Security** | API keys in source code | Key Vault + Defender |
| **Portability** | Absolute Windows paths | Relative paths + cloud deployment |

---

> **This document is the single source of truth for SENTINEL MESH V2. All implementation will follow this plan. No hard-coded values. Full automation. AI-powered. Production-grade.**
