# 🛡️ SENTINEL MESH V2 — Layer 4 Implementation Walkthrough

This document records the step-by-step setup, commands, configurations, and visualizations completed for **Layer 4 (Temporal Knowledge Graph & Entity Resolution)** using native Microsoft Fabric services.

---

## 📅 Summary of Achievements
1. **DDL Tables Created**: Provisioned `fact_shadow_links`, `fact_graph_communities`, and `fact_entity_resolution` in the Eventhouse.
2. **KQL Graph Functions Updated**: Deployed dynamic KQL functions for detecting circular loops (`detect_circular_flows()`) and shared-infrastructure connections (`detect_shadow_links()`).
3. **Graph Analysis Spark Notebook**: Built a PySpark notebook that runs Louvain Community Detection (to find fraud rings) and PageRank Centrality (to find money mules), storing results in `fact_graph_communities`.
4. **Entity Resolution Spark Notebook**: Created a PySpark notebook that runs fuzzy name matching (Levenshtein), shared device/IP intersection, and 12-dimensional Behavioral DNA Cosine Similarity to link duplicate or synthetic identities.
5. **Fabric Data Pipeline Orchestration**: Configured `SentinelMesh_L4_Orchestrator` to automate notebook execution sequentially, parameterized dynamically with Eventhouse connection parameters.
6. **Real-Time Visual Dashboard**: Built a native Fabric Real-Time Dashboard (`SentinelMesh_L4_Insights`) featuring 4 visual tiles (Circular Flow paths, Shadow Links, Community Sizes Bar Chart, and Resolved Linked Entities).

---

## 🛠️ Step-by-Step Actions & Commands

### STEP 1: Eventhouse Graph Tables DDL Setup
We ran the DDL setup script in the KQL query editor to create the output tables:
```kql
// Create table fact_graph_communities
.create table fact_graph_communities (
    customer_id: string,
    account_id: string,
    community_id: int,
    community_size: int,
    pagerank_score: real,
    in_degree: int,
    out_degree: int,
    is_hub: bool,
    computed_at: datetime
)

// Create table fact_entity_resolution
.create table fact_entity_resolution (
    primary_customer_id: string,
    primary_customer_name: string,
    linked_customer_id: string,
    linked_customer_name: string,
    match_type: string,
    match_score: real,
    shared_devices: dynamic,
    shared_ips: dynamic,
    dna_similarity: real,
    resolved_at: datetime,
    status: string
)

// Create table fact_shadow_links
.create table fact_shadow_links (
    customer_a_id: string,
    customer_a_name: string,
    customer_b_id: string,
    customer_b_name: string,
    shared_count: int,
    shared_entities: dynamic,
    link_strength: real,
    detected_at: datetime,
    status: string
)
```

---

### STEP 2: Configure Adaptive Settings
We seeded the algorithm parameters into `dim_adaptive_config` to avoid hardcoding:
```kql
.set-or-append dim_adaptive_config <|
    union
    (print config_key="graph.community.min_size", config_value=3.0, description="Minimum accounts in a cluster to flag as potential fraud ring", last_updated=now(), updated_by="system_init"),
    (print config_key="graph.pagerank.damping_factor", config_value=0.85, description="PageRank damping factor", last_updated=now(), updated_by="system_init"),
    (print config_key="graph.pagerank.max_iterations", config_value=100.0, description="PageRank max iterations", last_updated=now(), updated_by="system_init"),
    (print config_key="graph.hub.percentile_threshold", config_value=90.0, description="Percentile above which a node is flagged as a hub (mule)", last_updated=now(), updated_by="system_init"),
    (print config_key="entity.resolution.name_threshold", config_value=0.85, description="Fuzzy match name similarity threshold", last_updated=now(), updated_by="system_init"),
    (print config_key="entity.resolution.dna_threshold", config_value=0.90, description="Behavioral DNA Cosine similarity threshold", last_updated=now(), updated_by="system_init"),
    (print config_key="entity.resolution.min_shared_infra", config_value=1.0, description="Minimum shared devices/IPs to link customers", last_updated=now(), updated_by="system_init")
```

---

### STEP 3: Automated Pipeline Orchestration
We created a Data Factory pipeline:
1. **Activity 1 (`Run_Graph_Analysis`)**: Notebook running [L4_graph_analysis.py](file:///c:/Users/vinay/Documents/hack2future/sentinel_mesh_v2/notebooks/L4_graph_analysis.py) to calculate clusters and hubs.
2. **Activity 2 (`Run_Entity_Resolution`)**: Notebook running [entity_resolution.py](file:///c:/Users/vinay/Documents/hack2future/sentinel_mesh_v2/notebooks/entity_resolution.py) to resolve synthetic customers.
3. Connected on success: `Run_Graph_Analysis` ➔ `Run_Entity_Resolution`.
4. Configured with connection Base Parameters: `EVENTHOUSE_CLUSTER_URI` and `EVENTHOUSE_DATABASE`.

---

### STEP 4: Real-Time Dashboard Configuration
We built the dashboard visuals utilizing the following KQL queries:

* **Tile 1: Circular Fund loops (Table)**:
  ```kql
  detect_circular_flows()
  | extend Path = array_strcat(cycle_accounts, " ➔ ")
  | project flow_id, Path, Hops = hop_count, Amount = total_amount, DetectedAt = detected_at
  | order by Amount desc
  ```

* **Tile 2: Shadow Links (Table)**:
  ```kql
  detect_shadow_links()
  | project CustomerA = customer_a_name, CustomerB = customer_b_name, SharedCount = shared_count, SharedEntities = shared_entities, LinkStrength = link_strength, Status = status
  | order by LinkStrength desc
  ```

* **Tile 3: Louvain Clusters (Column Chart)**:
  ```kql
  fact_graph_communities
  | where community_id >= 0
  | summarize ClusterSize = max(community_size) by Community = strcat("Cluster #", tostring(community_id))
  | order by ClusterSize desc
  ```
  *(Enabled X/Y-Axis Titles and Data Labels on the column chart).*

* **Tile 4: Resolved Entities (Table)**:
  ```kql
  fact_entity_resolution
  | project PrimaryCustomer = primary_customer_name, LinkedCustomer = linked_customer_name, MatchType = match_type, MatchScore = match_score, ResolvedAt = resolved_at
  | order by MatchScore desc
  ```
