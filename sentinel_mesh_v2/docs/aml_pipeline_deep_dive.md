# 🛡️ Sentinel Mesh V2 — AML Pipeline Step-by-Step Execution Guide

This document is a focused study guide detailing the step-by-step lineage of the Sentinel Mesh V2 AML pipeline, beginning from local data ingestion through to network communities and identity resolution.

---

## 🗺️ Master Orchestration Sequence

The pipeline executes tasks in the following sequential order:

```
[Local Ingestion] ➔ [Eventstream] ➔ [fact_transactions] ➔ [dim_behavioral_dna] ➔ [fact_graph_communities] ➔ [fact_entity_resolution]
```

---

## 📥 Step 1: Local Ingestion & Transaction Ledger

When you run the command to stream transactions locally, they pass through a real-time data pathway:

### 1. Ingestion Commands
* **Run continuous streaming from the simulator:**
  ```powershell
  python -m sentinel_mesh_v2.data_simulator.simulator --mode stream
  ```
* **Inject a specific count (e.g. 50 transactions):**
  ```powershell
  python -m sentinel_mesh_v2.data_simulator.simulator --mode stream --count 50
  ```

### 2. Ingestion Routing
1. The local simulator streams JSON transaction payloads using Azure Event Hub protocols.
2. **Fabric Eventstream** captures these payloads with sub-second latency.
3. Eventstream maps the JSON keys to your Eventhouse columns and appends them directly into the **`fact_transactions`** table.

### 3. Database Schema: `fact_transactions`
The transaction ledger contains the following **19 columns**:

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `transaction_id` | `string` | Unique ledger ID |
| `customer_id` | `string` | Unique customer ID (e.g. `CUST-0022`) |
| `customer_name` | `string` | Customer's name at the time of transaction |
| `account_id` | `string` | The account number making the transfer |
| `amount` | `real` | Transaction value in INR |
| `timestamp` | `datetime` | Simulated transaction execution time (UTC) |
| `channel` | `string` | Channel (`Branch`, `ATM`, `Mobile`, `UPI`, `SWIFT`, `POS` - where **POS** means Point of Sale card tap/swipe merchant checkouts) |
| `device_id` | `string` | Login device hardware identifier |
| `ip_address` | `string` | Login client IP address |
| `geo_location` | `string` | Location of transaction (e.g. `Saket, Delhi`) |
| `merchant_id` | `string` | Target merchant ID (or `MERC-NONE`) |
| `mcc_code` | `string` | Merchant Category Code (e.g. `5411`) |
| `counterparty_account` | `string` | Recipient account number |
| `transaction_type` | `string` | Movement type (`deposit`, `withdrawal`, `transfer`, `payment`) |
| `ingestion_timestamp` | `datetime` | Time logged in Eventstream |
| `processing_status` | `string` | Current processing state (default: `pending`) |
| `risk_flag` | `string` | Transaction risk tag (`none`, `SUSPICIOUS`, `HIGH`) |
| `pattern_hash` | `string` | Unique hash identifier of the scenario |
| `scenario_id` | `string` | AML pattern tag (`normal`, `structuring`, `circular_flow`, etc.) |

---

### 4. Operational Context: Why do we need `scenario_id`, `pattern_hash`, and `risk_flag`?

To ensure clean evaluation, tracing, and multi-tier categorization, the simulation ledger utilizes three special fields:

* **`scenario_id` (The Ground Truth Label)**:
  * **Simulation Purpose**: Identifies the generated AML pattern (e.g., `structuring`, `circular_flow`, `normal`). It serves as the **hidden golden label** used to calculate True Positives (TP) and False Positives (FP). The mathematical engines do NOT query this field; they run completely blind to it.
  * **ML Pipeline Purpose**: In the training phase of our AML model pipeline, this column serves as the target label ($Y$) for supervised classification algorithms.
* **`pattern_hash` (The Instance Linkage Key)**:
  * Since complex laundering operations involve multiple hops or transactions, the simulator stamps all transactions belonging to a single scheme (e.g., a 4-hop circular loop) with the same `pattern_hash`. This allows analysts to query and trace the complete flow of a specific fraud ring event easily.
* **`risk_flag` (The First-Line Operational Gateway Flag)**:
  * Represents simple rules-based flags applied immediately at the transaction gateway (e.g., `none`, `SUSPICIOUS`, `HIGH` for large amounts). Downstream agents (Layer 5) and risk engines (Layer 6) combine this with advanced graph centrality and behavioral drift scores to compute the final `Composite Risk Score`.

---

## 🧬 Step 2: Layer 3 Behavioral DNA Refresh

Once transactions are saved, the orchestrator triggers the **`L3_Behavioral_DNA`** notebook to compute behavioral profiles.

### 1. The Real-Time Pre-Aggregation: `mv_daily_dna_stats` Materialized View
To avoid scanning millions of historical rows every time we evaluate a customer's baseline, the Eventhouse runs a KQL **Materialized View** that incrementally aggregates data as new records arrive.

#### 📜 Materialized View KQL Script:
```kql
.drop materialized-view mv_daily_dna_stats ifexists

.create materialized-view mv_daily_dna_stats on table fact_transactions
{
    fact_transactions
    | extend 
        is_digital = iif(channel in ("UPI", "Mobile"), 1, 0),
        is_round = iif(amount % 100 == 0, 1, 0),
        is_swift = iif(channel == "SWIFT", 1, 0),
        amount_sq = pow(amount, 2.0)
    | summarize 
        velocity = count(),
        sum_amount = sum(amount),
        sum_sq_amount = sum(amount_sq),
        geographic_spread = dcount(geo_location),
        counterparty_diversity = dcount(counterparty_account),
        digital_count = sum(is_digital),
        round_number_count = sum(is_round),
        device_count = dcount(device_id),
        cross_border_count = sum(is_swift),
        last_computed = max(timestamp)
        by customer_id, bin(timestamp, 1d)
}
```
* **KQL Constraints Handled**: KQL materialized views do not natively support standard deviation (`stdev()`), dynamic timespan variables, or conditional counts inside `summarize`. Standard deviation is calculated algebraically using `sum_sq_amount` ($\sum x^2$) and `sum_amount` ($\sum x$).
* **Performance Gain**: When computing a 30-day baseline, the database only scans a maximum of **30 daily summary rows** per customer instead of scanning every raw transaction, enabling sub-second execution at scale.

---

### 2. Behavioral DNA Vector: 12-Dimensional Profiling
Every customer has a dynamic spending profile represented as a 12-dimensional vector. The system calculates a **1-Day Current DNA Vector** and compares it to a **30-Day Baseline DNA Vector** to calculate a **Euclidean Drift Score ($\delta$)**.

The 12 dimensions are calculated and evaluated as follows:

1. **Velocity ($V_{vel}$)**:
   * *Calculation*: Total transaction count per day.
   * *Purpose*: Measures transactional frequency. Sudden spikes indicate compromised accounts or active mule accounts routing funds.
2. **Amount Profile ($V_{amt}$)**:
   * *Calculation*: Average transaction size (`avg(amount)`).
   * *Purpose*: Sets the financial scale of the customer. Massive increases flag anomalous transfer amounts.
3. **Temporal Pattern ($V_{temp}$)**:
   * *Calculation*: Standard deviation of transaction execution hours (`stdev(hour_of_day)`).
   * *Purpose*: Flags automated scripts/bots operating at exact intervals or unusual off-hours (e.g., midnight transfers).
4. **Counterparty Diversity ($V_{div}$)**:
   * *Calculation*: Distinct receiving accounts (`dcount(counterparty_account)`).
   * *Purpose*: Identifies dispersion (smurfs dispersing money to multiple people) or consolidation schemes.
5. **Channel Mix ($V_{chan}$)**:
   * *Calculation*: Ratio of digital app transactions (UPI/Mobile) to total (`countif(channel in ("UPI", "Mobile")) / total_count`).
   * *Note on TAB/WEB*: In the real world, `TAB` and `WEB` are digital channels. Classifying them as digital requires appending them to the KQL array check: `channel in ("UPI", "Mobile", "TAB", "WEB")`.
6. **Geographic Spread ($V_{geo}$)**:
   * *Calculation*: Distinct location count (`dcount(geo_location)`).
   * *Purpose*: Flags spatial dispersion indicating travel anomalies or shared account usage.
7. **Amount Entropy ($V_{ent}$)**:
   * *Calculation*: Variation coefficient (`stdev(amount) / avg(amount)`).
   * *Purpose*: Organic spending is highly variable (high entropy). Laundering paths are highly uniform (low entropy, e.g. routing exactly ₹4,99,000 every hop).
8. **Round Number Ratio ($V_{rnd}$)**:
   * *Calculation*: Ratio of transactions divisible by 100 to total transactions.
   * *Purpose*: Fraud structuring and cash layering almost always use round figures, unlike organic point-of-sale checkouts.
9. **Counterparty Recurrence ($V_{rec}$)**:
   * *Calculation*: Maximum transactions to a single recipient divided by total transactions.
   * *Purpose*: Identifies funneling concentration directed to a single controller or shell merchant.
10. **Dormancy Burst ($V_{bst}$)**:
    * *Calculation*: Maximum gap between transactions divided by average gap.
    * *Purpose*: Flags dormant "sleeper" accounts that suddenly wake up and execute rapid-fire transactions.
11. **Cross-Border Ratio ($V_{xbd}$)**:
    * *Calculation*: Ratio of international SWIFT transactions to total.
    * *Purpose*: Measures flight of capital offshore.
12. **Device Switching ($V_{dev}$)**:
    * *Calculation*: Distinct logged-in device IDs divided by total transaction count.
    * *Purpose*: Identifies account takeover (ATO) or mule networks logging in from multiple phone terminals.

---

### 3. Mathematical DNA Drift Score Calculation ($\delta$)
The drift score calculates the Euclidean distance between the 1-day vector ($\vec{V}_{1D}$) and the 30-day baseline vector ($\vec{V}_{30D}$). To normalize transaction amounts against the other [0-1] dimensions, the amount delta is scaled by the amount normalizer $C_{norm} = 10,000$:

$$\delta = \sqrt{(V_{vel,1D} - V_{vel,30D})^2 + \frac{(V_{amt,1D} - V_{amt,30D})^2}{C_{norm}^2} + (V_{chan,1D} - V_{chan,30D})^2 + (V_{geo,1D} - V_{geo,30D})^2 + (V_{rnd,1D} - V_{rnd,30D})^2}$$

#### Normalization for Risk Scoring ($F_3$):
The drift score $\delta$ is normalized to a `0-100` scale against a maximum limit of $10\sigma$:
$$F_3 = \min\left(100.0, \frac{\delta}{10.0} \times 100\right)$$

---

### 4. Historical Data Seeding & Fallback Strategy
* **Dimension Table History**: The dimension uploader seeds historical dates for registration: `dim_customer.created_date` spans back **3 years**, and `dim_account.opened_date` is matching.
* **Cold Start Fallbacks**: On day one, before a 30-day transaction history is accumulated, the KQL queries use conditional `iif` statements to bypass division-by-zero errors. It applies defaults (like `dna.default_recurrence_ratio` and `dna.default_dormancy_burst`) from `dim_adaptive_config` via `coalesce` functions to act as initial baselines.

---

### 5. Database Schema: `dim_behavioral_dna` (16 Columns)
* `customer_id` (string): Unique customer ID.
* `time_window` (string): Set to `"dynamic"` to represent a sliding observation window check.
* **12 Profile Dimensions:** `velocity`, `amount_profile`, `temporal_pattern`, `counterparty_diversity`, `channel_mix`, `geographic_spread`, `amount_entropy`, `round_number_ratio`, `counterparty_recurrence`, `dormancy_burst`, `cross_border_ratio`, `device_switching`.
* `drift_score` (real): Total Euclidean drift deviation score.
* `computed_at` (datetime): Snapshot timestamp.

---

## 🕸️ Step 3: Layer 4 Knowledge Graph Analysis

Next, the orchestrator triggers the **`L4_Graph_Analysis`** Spark notebook to map the transaction network:

### 1. Mathematical Graph Construction (NetworkX)
* **Graph Type**: Directed Graph ($G = (V, E)$), built using the Python **`networkx`** library.
* **Nodes ($V$)**: Bank accounts (`account_id`).
* **Directed Edges ($E$)**: Represent directed money transfers from a source account to a target account.
* **Edge Weight ($W$)**: The cumulative sum of transaction amounts transferred between the account pair.
* **Data Selection**: Filters out self-transfers (`ACC-SELF`) and generic non-merchant cash outs (`ACC-MERC-NONE`) to ensure only relevant counterparty routes are mapped.

---

### 2. Louvain Community Detection (Identifying Fraud Rings)
* **Algorithm Logic**: Louvain modularity optimization requires an undirected graph. The driver node converts the directed graph into an undirected copy (`G.to_undirected()`) and partitions the nodes to maximize network modularity.
* **Modularity Formula**: Measures the density of relative connections inside communities compared to random links.
* **Typology Detection**: In accordance with `dim_adaptive_config` parameters, any community with **3 or more connected accounts** (`community_size >= 3`) is flagged as a potential **Coordinated Fraud Ring / Smurfing Loop** participating in split layering operations.

---

### 3. PageRank Centrality & Degrees (Identifying Mule Nodes)
* **Algorithm Logic**: Runs PageRank on the directed graph (since money direction flow is critical for tracing source-to-destination paths).
* **Parameters**: Evaluated with a default damping factor ($\alpha = 0.85$) and maximum iterations limit of `100`, weighted by amount.
* **Network Metrics Collected**:
  * **PageRank Score**: Relative network influence metric (Decimal value, e.g., `0.024`).
  * **`in_degree`**: Counts incoming link connections. High values flag **Consolidator Accounts** (gathering funds from multiple sources).
  * **`out_degree`**: Counts outgoing link connections. High values flag **Distributor Accounts** (spreading funds to multiple targets).

---

### 4. What is `is_hub` and How does the Risk Engine use it?
* **Definition**: A boolean indicator flag (`true` or `false`) computed to isolate key money routing hubs in the network.
* **Calculation**: 
  1. The notebook computes the statistical percentile of all PageRank scores.
  2. It reads the hub threshold percentile (default: **`90th percentile`**) from `dim_adaptive_config`.
  3. If an account's PageRank score falls in the top 10% of the entire transaction graph, `is_hub` is flagged as **`true`**.
* **Layer 6 Risk Engine Integration**: In the downstream risk scoring view `generate_composite_alerts()`, the Centrality risk factor $F_2$ is normalized from PageRank. However, if `is_hub == true` for a customer's account, **the system overrides normal scaling and instantly boosts their Centrality risk factor $F_2$ to `100.0` (maximum risk)**. This ensures that network routing hubs are immediately flagged, regardless of whether their behavioral DNA drift looks normal.

---

### 5. Database Schema: `fact_graph_communities` (9 Columns)
This table acts as a snapshot records ledger of the network structure:

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `customer_id` | `string` | Unique customer ID |
| `account_id` | `string` | Unique account ID (A customer may own multiple accounts, resulting in multiple rows) |
| `community_id` | `int` | Modularity partition ID assigned by Louvain algorithm |
| `community_size` | `int` | Total count of accounts participating in this community |
| `pagerank_score` | `real` | Centrality probability score calculated by directed PageRank |
| `in_degree` | `int` | Count of unique incoming money transfer connections |
| `out_degree` | `int` | Count of unique outgoing money transfer connections |
| `is_hub` | `bool` | High-risk hub flag (True if PageRank $\ge$ 90th percentile) |
| `computed_at` | `datetime` | Notebook calculation timestamp |

---

## 🔍 Step 4: Layer 4 Entity Resolution

After graph analysis, the orchestrator triggers the **`Entity_Resolution`** Spark notebook to check for duplicate/fraudulent profiles.

### 1. Source Data Tables Queried
The engine pulls profile details from four primary tables:
* **`dim_customer`**: Used to get the customer names and cities (`name`, `city`).
* **`dim_device`**: Provides list mappings of customer IDs sharing physical hardware (`device_id`, `customer_ids`).
* **`dim_ip_address`**: Provides list mappings of customer IDs sharing network IPs (`ip_address`, `customer_ids`).
* **`dim_behavioral_dna`**: Provides the latest 12D behavioral DNA profile vectors.

---

### 2. The Multi-Dimensional Matching Calculations

The engine calculates similarity scores across three independent dimensions:

#### A. Shared Infrastructure Score
It intersects customer devices and IPs to calculate a normalized infrastructure similarity:
$$\text{Infrastructure Similarity} = \min\left(\frac{\text{Shared Device Count} + \text{Shared IP Count}}{5.0}, 1.0\right)$$

#### B. Fuzzy Name Similarity (Levenshtein Distance)
For customer profiles residing within the same city, it computes character-edit similarities:
$$\text{Name Similarity} = 1.0 - \left( \frac{\text{Levenshtein Distance}(A, B)}{\max(\text{Length}(A), \text{Length}(B))} \right)$$
*Pairs are kept if name similarity is $\ge 0.85$ (85%).*

#### C. Behavioral DNA Similarity (Vector Cosine Similarity)
To find "behavioral clones" (accounts transacting in the exact same manner, indicating bot networks), the engine calculates the Cosine Similarity of their 12D DNA vectors:
$$\text{DNA Similarity} = \frac{\vec{A} \cdot \vec{B}}{\|\vec{A}\| \|\vec{B}\|} = \frac{\sum_{i=1}^{12} A_i B_i}{\sqrt{\sum_{i=1}^{12} A_i^2} \sqrt{\sum_{i=1}^{12} B_i^2}}$$
*Pairs are flagged if DNA similarity is $\ge 0.90$ (90%).*

#### D. Aggregated Match Score
The final combined match score is the maximum value of the three similarities:
$$\text{Match Score} = \max\left(\text{Name Similarity}, \text{DNA Similarity}, \text{Infrastructure Similarity}\right)$$

---

### 3. Match Typology Classifications (`match_type`)
Pairs are assigned a match typology based on the following criteria:
* **`IDENTITY_THEFT_SUSPECT`**: Customer names are highly similar ($\ge 85\%$) **AND** they share physical logins (devices or IPs).
* **`FUZZY_NAME_MATCH`**: Names are similar ($\ge 85\%$) and they reside in the same city, but they do not share any device/IP logs.
* **`SHARED_DEVICE_AND_IP`**: The profiles share both physical device logins and network IP addresses.
* **`SHARED_DEVICE`**: The profiles log in using the same hardware device.
* **`SHARED_IP`**: The profiles connect from the same network IP.
* **`BEHAVIORAL_CLONE`**: The profiles have near-identical transacting behaviors (DNA similarity $\ge 90\%$) but have zero shared device or IP footprints (suggesting different burner phones utilizing the same automated script).

---

### 4. Database Schema: `fact_entity_resolution` (11 Columns)
The resolved matching pairs are written to the `fact_entity_resolution` table:

| Column Name | Data Type | Detailed Description |
| :--- | :--- | :--- |
| `primary_customer_id` | `string` | The ID of Customer A (the primary master profile) |
| `primary_customer_name` | `string` | The display name of Customer A |
| `linked_customer_id` | `string` | The ID of Customer B (the matched duplicate/synthetic profile) |
| `linked_customer_name` | `string` | The display name of Customer B |
| `match_type` | `string` | The linkage classification (e.g., `IDENTITY_THEFT_SUSPECT`, `BEHAVIORAL_CLONE`) |
| `match_score` | `real` | The highest similarity score (ranges from `0.0` to `1.0`) |
| `shared_devices` | `dynamic` | A JSON array containing the shared `device_id` strings (e.g. `["DEV-MOB-00022"]`) |
| `shared_ips` | `dynamic` | A JSON array containing the shared IP address strings (e.g. `["192.168.1.22"]`) |
| `dna_similarity` | `real` | The raw Cosine Similarity value between the two DNA vectors |
| `resolved_at` | `datetime` | The calculation timestamp when the Spark job finished the run |
| `status` | `string` | Workflow status (always initialized to `"RESOLVED"`) |

---

## 🚀 Step 5: Layer 5 AI Agent Swarm (Detection)

Once identity context is refreshed, the database triggers **6 parallel detection agents** implemented as native KQL functions to scan for laundering patterns:

### 1. Swarm Agent Detections Detail

#### A. Agent 1: Structuring Sentinel (`agent_structuring_detect()`)
* **Detection Goal**: Flags cash splitting patterns designed to bypass the ₹10 Lakhs CTR reporting threshold.
* **KQL Logic**: Scans `fact_transactions` over a 1-day window (`bin(timestamp, 1d)`) for deposit-related transaction types.
* **Trigger Conditions**:
  * $\text{Total Cumulative Amount} \ge \text{CTR limit}$ (₹10 Lakhs).
  * $\text{Total Transaction Count} > 1$ (split activity).
  * $\text{Maximum Single Transaction Amount} < \text{CTR limit}$ (proving evasion).
* **Confidence Formula**:
  $$\text{Confidence} = \min\left(0.99, \frac{\text{Total Cumulative Amount}}{1.5 \times \text{CTR Limit}}\right)$$

#### B. Agent 2: Velocity Anomaly Detector (`agent_velocity_detect()`)
* **Detection Goal**: Flags sudden, out-of-character transaction volume or value bursts.
* **KQL Logic**: Reads `dim_behavioral_dna` for the latest calculated customer profiles.
* **Trigger Conditions**:
  * $\text{Euclidean DNA Drift Score } \delta \ge \text{Sigma Threshold}$ (default: $3.0\sigma$).
* **Confidence Formula**:
  $$\text{Confidence} = \min\left(0.95, \frac{\text{Drift Score } \delta}{1.8 \times \text{Drift Limit}}\right)$$

#### C. Agent 3: Geo-Temporal Analyzer (`agent_geotemporal_detect()`)
* **Detection Goal**: Flags "impossible travel" logins from distinct physical locations in short timeframes.
* **KQL Logic**: Uses serialization (`serialize`, `prev()`) to compare consecutive transaction locations and timestamps for each customer.
* **Trigger Conditions**:
  * Customer's location changes between consecutive transactions.
  * Time interval between locations $\le \text{Allowed travel window}$ (default: 120 minutes).
* **Confidence Formula**:
  $$\text{Confidence} = \min\left(0.95, 1.0 - \frac{\text{Actual Interval (mins)}}{\text{Allowed Limit (120 mins)}}\right)$$

#### D. Agent 4: Circular Flow Tracker (`agent_circularflow_detect()`)
* **Detection Goal**: Identifies circular layering networks where funds return to the sender.
* **KQL Logic**: Wraps the Layer 4 graph matching function `detect_circular_flows()`.
* **Trigger Conditions**:
  * Customer account lies in a closed routing loop of 3 to 10 hops ($A \to B \to C \to A$) completed within **2 hours**.
  * Loop amount consistency ratio $\ge 80\%$.
* **Confidence Formula**:
  $$\text{Confidence} = \frac{\text{Minimum Hop Amount}}{\text{Maximum Hop Amount}}$$

#### E. Agent 5: Shadow Link Discoverer (`agent_shadowlink_detect()`)
* **Detection Goal**: Flags hidden relationships established via shared device or IP footprints.
* **KQL Logic**: Queries matched customer-to-attribute links from `detect_shadow_links()`.
* **Trigger Conditions**:
  * Different customer profiles sharing login devices or network IP addresses.
* **Confidence Formula**:
  $$\text{Confidence} = \text{Link Strength} = \min\left(1.0, \frac{\text{Shared Infrastructure Count}}{\text{Max Norm Factor (3.0)}}\right) \times 2.55$$

#### F. Agent 6: Shell Entity Profiler (`agent_shell_detect()`)
* **Detection Goal**: Flags high-volume payments routed to front/shell merchants.
* **KQL Logic**: Joins `fact_transactions` with `dim_merchant` (Merchant Master).
* **Trigger Conditions**:
  * Transactions routed to merchants with a registered shell score $\ge 0.70$.
* **Confidence Formula**:
  $$\text{Confidence} = \text{Merchant's Shell Score}$$

---

### 2. Unified Agent Output Schema (5 Columns)
To enable the virtual union query in Layer 6, all 6 agents compile and return a unified schema containing:

| Column Name | Data Type | Detailed Description |
| :--- | :--- | :--- |
| `customer_id` | `string` | Unique customer ID |
| `customer_name` | `string` | Display name of the customer |
| `agent_name` | `string` | Name of the triggering agent (e.g. `Structuring Sentinel`) |
| `confidence` | `real` | Scaled confidence score (ranges from `0.0` to `1.0`) |
| `reason` | `string` | Structured text detailing the exact mathematical triggers |
| `related_transactions` | `dynamic` | JSON array listing transaction IDs, device hashes, or IP details |

---

---

## ⚖️ Step 6: Layer 6 Composite Risk Scoring

After the Layer 5 swarm detection agents identify anomalous patterns, the orchestrator triggers the risk-scoring engine to calculate a holistic customer threat rating. This is executed using the following KQL control command:

```kql
.set-or-append fact_alerts <| generate_composite_alerts()
```

### 1. Core Architecture: Separating Certainty (Confidence) from Severity (Risk Score)

To minimize false positives while ensuring high-priority threats are surfaced immediately, the risk engine divides its evaluation into two distinct dimensions:
1. **`confidence` (Certainty Index, range: `0.0` to `1.0`):** Indicates how mathematically certain the system is that a suspicious pattern has actually occurred. This is calculated purely as the arithmetic mean of the confidence values of all triggering swarm agents:
   $$\text{Confidence} = \frac{1}{N} \sum_{i=1}^{N} \text{Confidence}(\text{Agent}_i)$$
2. **`risk_score` (Overall Threat Rating, range: `0.0` to `100.0`):** Measures the absolute threat level the customer poses to the institution. It combines behavioral anomalies with infrastructure graph network positions, customer profiles, and history.

---

### 2. The 5-Factor Weighted Scoring Formula

The composite risk score is calculated as a weighted sum of five distinct risk dimensions ($F_1$ through $F_5$):

$$\text{Composite Risk Score} = (F_1 \times w_{f1}) + (F_2 \times w_{f2}) + (F_3 \times w_{f3}) + (F_4 \times w_{f4}) + (F_5 \times w_{f5})$$

Where each factor is scaled dynamically from `0` to `100`:

| Factor | Name | Formula / Scaling Logic | Default Weight ($w_i$) | Purpose |
| :---: | :--- | :--- | :---: | :--- |
| **$F_1$** | **Agent Consensus** | $\text{Average Agent Confidence} \times 100$ | **`35%`** ($0.35$) | Rewards multi-agent consensus; higher if multiple agents detect anomalous behavior with high certainty. |
| **$F_2$** | **Graph Centrality** | $\min\left(\frac{\text{PageRank Score}}{\max(\text{PageRank})} \times 100, 100\right)$ | **`20%`** ($0.20$) | Penalizes accounts positioned at network hubs, highlighting potential money distributors or mules. |
| **$F_3$** | **Behavioral DNA Drift** | $\min\left(\frac{\text{Drift Score}}{10.0} \times 100, 100\right)$ | **`20%`** ($0.20$) | Measures how much the customer's current behavior deviates from their established historical baseline. |
| **$F_4$** | **Historical Alerts** | $\min\left(\frac{\text{AlertCount (last 30d)}}{5.0} \times 100, 100\right)$ | **`15%`** ($0.15$) | Escalates risk for repeat offenders; capped at 5 recent alerts. |
| **$F_5$** | **PEP/Watchlist Match** | $\text{if } \text{pep\_flag} == \text{true} \text{ then } 100.0 \text{ else } 0.0$ | **`10%`** ($0.10$) | Adds a baseline threat penalty (+10 points) for Politically Exposed Persons (PEPs) or watchlist profiles. |

---

### 3. Adaptive Configurations (`dim_adaptive_config`)

Rather than hardcoding limits, the engine dynamically queries weight allocations and severity thresholds from the **`dim_adaptive_config`** metadata table. If a key is missing, KQL's `coalesce` function applies the defaults:

* **Scoring Weights:** `scoring.weight.f1` (35.0), `scoring.weight.f2` (20.0), `scoring.weight.f3` (20.0), `scoring.weight.f4` (15.0), `scoring.weight.f5` (10.0)
* **Risk Tier Thresholds:**
  * **`LOW` Risk Tier:** $\text{Composite Risk Score} \le 30.0$
  * **`MEDIUM` Risk Tier:** $30.0 < \text{Composite Risk Score} \le 60.0$
  * **`HIGH` Risk Tier:** $60.0 < \text{Composite Risk Score} \le 80.0$
  * **`CRITICAL` Risk Tier:** $\text{Composite Risk Score} > 80.0$

---

### 4. Step-by-Step Mathematical Walkthroughs (Examples)

#### 🔬 Scenario A: Aarav Mehta (`CUST-0026`) - Coordinated Structuring & High Drift
Aarav is a retail customer who triggers two Layer 5 swarm detection agents. His network centrality is moderate, but his spending pattern is completely out of character.

1. **Layer 5 Agent Input ($F_1$):**
   * `agent_structuring_detect()`: Confidence = **`0.80`** (Frequent deposits of 49,000 INR to bypass reporting thresholds).
   * `agent_velocity_detect()`: Confidence = **`0.90`** (Transaction count is 3.5x their usual daily velocity).
   * **Consensus Confidence:** $\text{Avg} = \frac{0.80 + 0.90}{2} = 0.85$.
   * **Scaled Factor $F_1$:** $0.85 \times 100 = \mathbf{85.0}$
2. **Graph Centrality ($F_2$):**
   * Aarav's Louvain PageRank score = **`0.015`**.
   * The maximum PageRank score in the network (`max_pr_val`) = **`0.050`**.
   * **Scaled Factor $F_2$:** $\min\left(\frac{0.015}{0.050} \times 100, 100\right) = \mathbf{30.0}$
3. **DNA Drift Score ($F_3$):**
   * Aarav's latest Euclidean baseline drift score = **`4.5`**.
   * **Scaled Factor $F_3$:** $\min\left(\frac{4.5}{10.0} \times 100, 100\right) = \mathbf{45.0}$
4. **Historical Alerts ($F_4$):**
   * Aarav has triggered **`1`** active alert in the previous 30 days.
   * **Scaled Factor $F_4$:** $\min\left(\frac{1}{5.0} \times 100, 100\right) = \mathbf{20.0}$
5. **Watchlist Match ($F_5$):**
   * Aarav is not a Politically Exposed Person (`pep_flag = false`).
   * **Scaled Factor $F_5$:** $\mathbf{0.0}$

**Calculating the Composite Score:**
$$\text{Composite Score} = (85.0 \times 0.35) + (30.0 \times 0.20) + (45.0 \times 0.20) + (20.0 \times 0.15) + (0.0 \times 0.10)$$
$$\text{Composite Score} = 29.75 + 6.00 + 9.00 + 3.00 + 0.00 = \mathbf{47.75}$$

**Result:** Since $30.0 < 47.75 \le 60.0$, the alert is generated with a **`MEDIUM`** risk tier. Aarav's transaction anomalies are highly certain ($85\%$), but because he has no watchlist matches and limited network centrality, he is not pushed into high risk.

---

#### 🔬 Scenario B: Kritika Brar (`CUST-0011`) - High-Risk PEP with Network Mule Tendencies
Kritika is a Politically Exposed Person (PEP) whose account starts acting as a transaction hub, receiving deposits from multiple distinct accounts.

1. **Layer 5 Agent Input ($F_1$):**
   * `agent_velocity_detect()`: Confidence = **`0.80`** (Large wire transfer spike).
   * **Consensus Confidence:** $\text{Avg} = 0.80$.
   * **Scaled Factor $F_1$:** $0.80 \times 100 = \mathbf{80.0}$
2. **Graph Centrality ($F_2$):**
   * Kritika's PageRank score is **`0.045`** (with network max = `0.050`), meaning she is heavily linked to a coordinated group.
   * **Scaled Factor $F_2$:** $\min\left(\frac{0.045}{0.050} \times 100, 100\right) = \mathbf{90.0}$
3. **DNA Drift Score ($F_3$):**
   * Her behavioral drift score = **`8.0`** (Completely different transaction behavior compared to baseline).
   * **Scaled Factor $F_3$:** $\min\left(\frac{8.0}{10.0} \times 100, 100\right) = \mathbf{80.0}$
4. **Historical Alerts ($F_4$):**
   * She has **`2`** active alerts logged over the past month.
   * **Scaled Factor $F_4$:** $\min\left(\frac{2}{5.0} \times 100, 100\right) = \mathbf{40.0}$
5. **Watchlist Match ($F_5$):**
   * Kritika is on the PEP watchlist (`pep_flag = true`).
   * **Scaled Factor $F_5$:** $\mathbf{100.0}$

**Calculating the Composite Score:**
$$\text{Composite Score} = (80.0 \times 0.35) + (90.0 \times 0.20) + (80.0 \times 0.20) + (40.0 \times 0.15) + (100.0 \times 0.10)$$
$$\text{Composite Score} = 28.00 + 18.00 + 16.00 + 6.00 + 10.00 = \mathbf{78.00}$$

**Result:** Since $60.0 < 78.00 \le 80.0$, the alert is categorized as **`HIGH`** severity.
* **Tuning Analysis:** Notice that if Kritika were *not* a PEP, her score would have been $68.00$ (High). If she triggered just one more historical alert (raising $F_4$ to $60.0$), her score would rise to $81.00$, pushing her immediately into the **`CRITICAL`** tier, flagging her for immediate automated SAR generation in Step 7.

---

### 5. Database Schema: `fact_alerts` (14 Columns)

The resulting record generated and appended to `fact_alerts` has the following schema structure:

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `alert_id` | `string` | Unique identifier UUID for the alert |
| `customer_id` | `string` | Target customer ID (e.g. `CUST-0026`) |
| `customer_name` | `string` | Customer name |
| `agent_name` | `string` | Consolidated agents string (e.g. `"2 Agents Consensus"`) |
| `confidence` | `real` | Average confidence of triggered agents (scale `0.0 - 1.0`) |
| `reason` | `string` | Consolidated agent trigger reasons joined by `" \| "` |
| `risk_score` | `real` | Overall calculated composite risk score (scale `0.0 - 100.0`) |
| `risk_tier` | `string` | Alert severity tier (`LOW`, `MEDIUM`, `HIGH`, `CRITICAL`) |
| `status` | `string` | Current alert workflow status (defaults to `"ACTIVE"`) |
| `disposition` | `string` | Analyst resolution audit state (`PENDING`, `TRUE_POSITIVE`, `FALSE_POSITIVE`) |
| `triggered_at` | `datetime` | Log ingestion UTC timestamp |
| `resolved_at` | `datetime` | Resolution timestamp (`null` until resolved by analyst) |
| `related_transactions` | `dynamic` | JSON array containing IDs of transaction rows that triggered the agents |
| `cross_correlations` | `dynamic` | Packed evidence JSON containing DNA baseline drift, network graph metrics, and watchlist statuses |

#### 📋 Example Database Row JSON representation:
```json
{
  "alert_id": "8c7f99be-7581-42a1-9a3d-4c3e6a9f471e",
  "customer_id": "CUST-0026",
  "customer_name": "Aarav Mehta",
  "agent_name": "2 Agents Consensus",
  "confidence": 0.85,
  "reason": "Velocity agent detected amount 3x above average DNA velocity | Structuring agent observed 4 cash transactions under 50k INR",
  "risk_score": 47.75,
  "risk_tier": "MEDIUM",
  "status": "ACTIVE",
  "disposition": "PENDING",
  "triggered_at": "2026-06-10T01:22:19Z",
  "resolved_at": null,
  "related_transactions": [
    "TX-88392",
    "TX-88395",
    "TX-88401",
    "TX-88405"
  ],
  "cross_correlations": {
    "agent_list": [
      "Structuring Agent",
      "Velocity Agent"
    ],
    "pagerank_score": 0.015,
    "dna_drift_score": 4.5,
    "historical_alerts_30d": 1,
    "pep_watchlist_match": false
  }
}
```

---

## 📝 Step 7: Layer 7 AI SAR Generation

The final step in the core detection pipeline is the **`L7_SAR_Generator`** PySpark notebook, which automates the compliance filing process by drafting professional Suspicious Activity Reports (SAR) for Financial Intelligence Units (FIU).

### 1. Ingestion Pipeline & Execution Logic
1. **Trigger Condition**: Executes automatically on success of Layer 6 risk alert generation as the final node in the Data Factory pipeline `SentinelMesh_L4_Orchestrator`.
2. **Alert Filtering**: The notebook queries the Eventhouse to identify customers who have triggered active `HIGH` or `CRITICAL` risk alerts, but do not yet have a record in the SAR ledger (resolving double-filings via a KQL `leftanti` join):
   ```kql
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
   ```
3. **Azure OpenAI Integration**: For each identified customer, the engine formats a comprehensive forensic profile context containing network centrality (PageRank), behavioral DNA drift, and PEP watchlist matches, passing it to `gpt-4o` using JSON mode (`response_format={"type": "json_object"}`).

---

### 2. OpenAI System and User Prompt Design
To ensure reports meet strict banking regulatory standards, the engine enforces structured compliance prompting:

* **System Prompt**:
  ```text
  You are an expert AML Compliance Officer and Forensic Auditor. Your task is to write a highly detailed, professional Suspicious Activity Report (SAR) for financial intelligence units (FIU). Use formal banking terminology.
  ```
* **User Prompt (Structured Schema)**:
  ```text
  Generate a Suspicious Activity Report (SAR) for the following customer alert profile:
  - Customer ID: {customer_id}
  - Customer Name: {customer_name}
  - Calculated Risk Score: {risk_score}/100 ({risk_tier} Risk)
  - Triggered Agents: {agent_list}
  - Primary Trigger Details: {reason}
  
  --- Supporting Evidence ---
  - PageRank Centrality: {pagerank} (centrality score in money flows)
  - Behavioral DNA Drift: {drift} sigma deviation from baseline DNA
  - PEP/Sanctions Watchlist Match: {pep_match}
  
  Please output your response strictly as a JSON object with the following keys. Ensure no markdown packaging, just raw JSON:
  {
      "executive_summary": "A 2-3 sentence summary of the case and why it is suspicious.",
      "suspicious_activity": "A comprehensive analysis of the transaction structuring, loop transfers, or shared infrastructure, detailing the indicators of crime.",
      "red_flags": ["A list of key red flags observed (e.g. Structuring, Loop Funds, PEP Match)"],
      "recommended_actions": ["A list of specific recommended actions (e.g. freeze account, escalate to FIU)"],
      "risk_assessment": "Assessment of the threat level, likelihood of money laundering, or terrorist financing, and systemic risk."
  }
  ```

---

### 3. Database Schema: `fact_sar_reports` (15 Columns)
The generated compliance documents are saved to the `fact_sar_reports` table:

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `sar_id` | `string` | Unique identifier format: `SAR-[timestamp]-[customer_id]` |
| `customer_id` | `string` | The target customer ID |
| `customer_name` | `string` | The customer's display name |
| `case_id` | `string` | The original alert ID (`alert_id`) from `fact_alerts` |
| `executive_summary` | `string` | Summarized incident overview generated by Azure OpenAI |
| `suspicious_activity` | `string` | Comprehensive transaction analysis narrative |
| `red_flags` | `string` | JSON array of observed compliance red flags |
| `recommended_actions` | `string` | JSON array of analyst actions (e.g., `"Freeze Account"`) |
| `risk_assessment` | `string` | Qualitative narrative assessing AML/CFT threat |
| `risk_score` | `real` | The numerical composite risk score |
| `model_version` | `string` | Model identifier used (e.g. `"gpt-4o"`) |
| `prompt_hash` | `string` | MD5 hash of the prompt payload for audit compliance tracking |
| `generated_at` | `datetime` | UTC timestamp when the report was written |
| `validated` | `bool` | Human analyst validation flag (initialized to `false`) |
| `content_safety_score` | `real` | Safety classification score (initialized to `0.0`) |

---

---

## ⚡ Step 8: Layer 8 Action & Compliance Gateway

Following the creation of alert entries and SAR drafts, the **Action Gateway** executes downstream operations using **Fabric Activator**, **Power Automate**, and **Microsoft Purview**.

### 1. Gateway Routing Rules
The gateway operates as an event-driven system checking new inserts in `fact_alerts`:
1. **Real-Time Analyst Notification**:
   * **Rule**: When `risk_tier` matches `HIGH` or `CRITICAL`.
   * **Action**: Fabric Activator posts an interactive card to the security team's Microsoft Teams channel containing customer details, composite score, triggered agents, and a hyperlink to the **AML GPT Web Command Center** for investigation.
2. **Automated Transaction Blocking & Account Hold**:
   * **Rule**: When `risk_score` $\ge 80.0$ (`CRITICAL` tier) OR a 100% confirmed `BEHAVIORAL_CLONE` synthetic group is identified.
   * **Action**: Dispatches a secure webhook payload to the core banking platform (API gateway) to mark target accounts `dim_account.status = "FROZEN"`, blocking all outgoing debit transfers.
3. **Purview Audit Logging**:
   * **Rule**: All gateway actions (freezes, alerts sent, SAR hashes written).
   * **Action**: Commits a permanent, read-only audit footprint to Microsoft Purview detailing action timestamp, user/agent ID, and compliance status.

---

---

## 🧠 Step 9: Layer 9 Self-Learning Feedback Loop

The **`L9_Recalibration`** Spark notebook acts as the self-improving engine of the Sentinel Mesh framework. It analyzes human decisions to automatically calibrate detection thresholds and composite scoring weights, reducing false positives while capturing novel laundering techniques.

### 1. Analyst Feedback Collection: `fact_analyst_dispositions`
When analysts investigate alerts in the Command Center, they log decisions to this table:
* `disposition_id` (string): Unique disposition ID.
* `alert_id` (string): Linkage key matching `fact_alerts.alert_id`.
* `agent_name` (string): The agent that triggered the alert (e.g., `"Velocity Anomaly"`).
* `disposition` (string): The analyst's choice:
  * **`TRUE_POSITIVE`**: Actual suspicious activity confirmed.
  * **`ESCALATE`**: Passed to secondary investigative unit (treated as True Positive).
  * **`FALSE_POSITIVE`**: Legitimate activity flagged in error.
* `analyst_id` (string): Auditor ID.
* `timestamp` (datetime): Entry log time.

---

### 2. Mathematical Performance Evaluation
The engine groups feedback by agent over a rolling 90-day window to calculate metrics:

* **True Positive Rate (TPR / Precision)**:
  $$\text{TPR}_{\text{agent}} = \frac{\sum(\text{TRUE\_POSITIVE} + \text{ESCALATE})}{\sum(\text{TRUE\_POSITIVE} + \text{ESCALATE} + \text{FALSE\_POSITIVE})}$$
* **False Positive Rate (FPR)**:
  $$\text{FPR}_{\text{agent}} = \frac{\sum(\text{FALSE\_POSITIVE})}{\sum(\text{TRUE\_POSITIVE} + \text{ESCALATE} + \text{FALSE\_POSITIVE})}$$

---

### 3. Threshold Self-Tuning Rules
If an agent accumulates $\ge 5$ feedback reviews, the recalibration notebook applies self-tuning limits:

1. **Velocity Anomaly Threshold Tuning ($\sigma$)**:
   * If $\text{FPR} > 30\%$, the agent is noisy. The notebook increments the sigma limit to suppress future false alarms:
     $$\sigma_{new} = \min\left(5.0, \sigma_{old} + 0.5\right)$$
   * If $\text{FPR} < 10\%$, the agent is highly precise. The notebook decrements the sigma limit to capture subtler behavioral anomalies:
     $$\sigma_{new} = \max\left(2.0, \sigma_{old} - 0.2\right)$$
2. **Structuring Sentinel Rule Tuning**:
   * If $\text{FPR} > 25\%$, deposits under 50,000 INR are causing noise. The notebook increments the minimum transaction count threshold and increases the transaction value threshold by 10%:
     $$\text{txn\_count}_{new} = \min\left(6, \text{txn\_count}_{old} + 1\right)$$
     $$\text{amount\_threshold}_{new} = \text{amount\_threshold}_{old} \times 1.1$$
3. **Geo-Temporal Corridor Tuning**:
   * If $\text{FPR} > 30\%$, travel alerts are flagging legitimate flight routes. The system tightens the maximum allowed travel corridors by subtracting 15 minutes:
     $$\text{max\_travel\_minutes}_{new} = \max\left(60.0, \text{max\_travel\_minutes}_{old} - 15.0\right)$$

---

### 4. Dynamic Risk Weight Recalibration & Normalization
The engine adjusts the 5 risk factor weights ($w_{f1}$ through $w_{f5}$) based on the global agent precision $P_{global}$ calculated as the sum of all agent TPs divided by total evaluations:

#### Case A: Noisy Swarm ($P_{global} < 60\%$)
The system dampens the influence of the agent consensus factor ($F_1$) and shifts scoring emphasis to the structural network graph ($F_2$) and behavioral DNA deviations ($F_3$):
$$w_{f1, new} = \max(15.0, w_{f1, old} - 5.0)$$
$$w_{f2, new} = \min(40.0, w_{f2, old} + 2.5)$$
$$w_{f3, new} = \min(40.0, w_{f3, old} + 2.5)$$

#### Case B: Highly Accurate Swarm ($P_{global} > 85\%$)
The system shifts weight to consensus ($F_1$) to prioritize automated swarm agreements:
$$w_{f1, new} = \min(50.0, w_{f1, old} + 5.0)$$
$$w_{f2, new} = \max(10.0, w_{f2, old} - 2.5)$$
$$w_{f3, new} = \max(10.0, w_{f3, old} - 2.5)$$

#### Weight Normalization:
To guarantee that the weights sum to exactly 100% and avoid floating-point drift, the factors are normalized and rounded to 1 decimal place:
$$w_{i, \text{norm}} = \text{round}\left( \frac{w_{i, new}}{\sum_{j=1}^5 w_{j, new}} \times 100.0, 1 \right)$$
The regulatory PEP Watchlist weight ($w_{f5, \text{norm}}$) acts as the balancing variable to absorb any rounding differences:
$$w_{f5, \text{norm}} = 100.0 - (w_{f1, \text{norm}} + w_{f2, \text{norm}} + w_{f3, \text{norm}} + w_{f4, \text{norm}})$$

The recalibrated values are written back to `dim_adaptive_config` in the Eventhouse database, so that subsequent runs of the scoring engine automatically employ the updated thresholds.

---

### 5. Database Schema: `dim_adaptive_config` (5 Columns)

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `config_key` | `string` | Unique key name (e.g. `agent.velocity.sigma_threshold`) |
| `config_value` | `real` | Current value of the config parameter |
| `description` | `string` | Explanatory description of what the key controls |
| `last_updated` | `datetime` | UTC timestamp of the last recalibration write |
| `updated_by` | `string` | Notebook task name (`"L9_Recalibration_Engine"`) |

