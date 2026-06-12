# 🛡️ SENTINEL MESH V2 — Layer 5 & 6 Implementation Walkthrough

This document records the step-by-step setup, commands, configurations, and visualizations completed for **Layer 5 (AI Agent Swarm)** and **Layer 6 (Adaptive Risk Scoring Engine)** using native Microsoft Fabric Eventhouse capabilities.

---

## 📅 Summary of Achievements
1. **6 KQL Swarm Agents Deployed**: Written and compiled 6 statistical/graph-driven detection functions in `L5_swarm_agents_v2.kql` (Structuring, Velocity, Travel, Loop, Shadow Link, and Shell Profiling).
2. **Adaptive Composite Risk Engine Deployed**: Created the 5-factor weighted risk scoring function `generate_composite_alerts()` in `L6_risk_scoring_v2.kql`, which evaluates consensus, centrality, DNA drift, history, and watchlist status.
3. **Pipeline Automation Closed**: Added `Generate_Risk_Alerts` (KQL Script activity) to the Data Factory pipeline, completing the end-to-end lineage:
   `Run_Behavioral_DNA` ➔ `Run_Graph_Analysis` ➔ `Run_Entity_Resolution` ➔ `Generate_Risk_Alerts`.
4. **Dashboard Alerts Feed**: Integrated a live alerts feed tile on the Real-Time Dashboard (`SentinelMesh_L4_Insights`) displaying composite alerts, risk scores, and risk tiers (Low, Medium, High, Critical) under swarm consensus.

---

## 🛠️ Step-by-Step Actions & Commands

### STEP 1: Deploy KQL Agent Swarm (Layer 5)
We ran the script [L5_swarm_agents_v2.kql](file:///c:/Users/vinay/Documents/hack2future/sentinel_mesh_v2/kql/L5_swarm_agents_v2.kql) to create the 6 detection functions:
* **`agent_structuring_detect()`** — detects deposits designed to bypass ₹10L reporting threshold.
* **`agent_velocity_detect()`** — compares current volume against DNA baseline drift.
* **`agent_geotemporal_detect()`** — impossible travel scan using serialized window tracking.
* **`agent_circularflow_detect()`** — loops extracted from Layer 4 graph.
* **`agent_shadowlink_detect()`** — shared device/IP fingerprint linkages.
* **`agent_shell_detect()`** — transactions routed to merchants with high shell scores.

---

### STEP 2: Deploy Composite Risk Scoring (Layer 6)
We ran the script [L6_risk_scoring_v2.kql](file:///c:/Users/vinay/Documents/hack2future/sentinel_mesh_v2/kql/L6_risk_scoring_v2.kql) to create the risk scoring function:
* **`generate_composite_alerts()`** — evaluates the 5-Factor Risk Formula:
  $$\text{Composite Risk} = (F_1 \times W_1) + (F_2 \times W_2) + (F_3 \times W_3) + (F_4 \times W_4) + (F_5 \times W_5)$$
  * *F1 (Consensus)*: Average confidence of all triggered agents (uses `make_set()` to deduplicate agent names).
  * *F2 (Centrality)*: PageRank score normalized against the maximum PageRank in the dataset.
  * *F3 (DNA Deviation)*: Normalized drift score from the DNA engine.
  * *F4 (Historical Risk)*: Count of alerts in the last 30 days.
  * *F5 (Watchlist)*: Customer PEP status (100 if true, 0 if false).

---

### STEP 3: Persist Alerts via Pipeline
Added a KQL Script activity `Generate_Risk_Alerts` running the command:
```kql
.set-or-append fact_alerts <| generate_composite_alerts()
```
This appends all calculated alerts to the `fact_alerts` table at the end of the pipeline run.

---

### STEP 4: Real-Time Alerts Feed Dashboard Tile
* **Tile 5: Composite Risk Alerts Feed (Table)**:
  ```kql
  fact_alerts
  | project customer_name, agent_name, risk_score, risk_tier, status, triggered_at
  | order by risk_score desc
  ```
  *(Displays active alert statuses and risk classifications dynamically).*
