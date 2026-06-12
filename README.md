# 🛡️ SENTINEL MESH — Cognitive AML Detection Mesh

SENTINEL MESH is a 9-layer cognitive architecture for real-time Anti-Money Laundering (AML) detection, built natively on Microsoft Fabric. It integrates real-time Eventstreams, Eventhouse (KQL DB), Lakehouse (Delta tables), a Temporal Knowledge Graph, and a Cognitive Agent Swarm to detect complex money laundering patterns with sub-second latency.

---

## 📁 Workspace Directory Structure

The project has been organized into a clean, professional structure:

```text
hack2future/
├── docs/                             # All documentation, slides, and diagrams
│   ├── sentinel_mesh_blueprint.md    # Markdown architecture blueprint
│   ├── architecture_diagram.html     # Interactive 9-layer interactive flow
│   ├── architecture_overview.html    # Interactive workspace diagram
│   ├── roadmap_diagram.html          # Interactive sprint plan
│   └── simple_flow_diagram.html      # Simplified data flow layout
│
├── sentinel_mesh_v2/                 # The active codebase for the real-time POC
│   ├── data_simulator/               # Local transaction streaming simulator
│   │   ├── simulator.py              # Streams normal & AML pattern transactions
│   │   └── requirements.txt          # Simulator python dependencies
│   │
│   ├── kql/                          # Real-Time Intelligence DDL & agent queries
│   │   ├── L2_eventhouse_ddl.kql     # Schema for fact_transactions
│   │   ├── L5_swarm_agents_v2.kql    # Structuring & Velocity KQL agents
│   │   └── L6_risk_scoring_v2.kql    # Risk orchestration function
│   │
│   └── notebooks/                    # Spark Notebooks for semantic layers
│       ├── L3_behavioral_dna.py      # DNA Vector calculation
│       ├── L4_graph_analysis.py      # Temporal graph queries
│       ├── L9_recalibration.py       # Auto-recalibration logic
│       └── entity_resolution.py      # Entity resolution models
│
├── .env                              # Local credentials (git-ignored)
└── README.md                         # This file
```

---

## 🚀 How to Run the Simulator
1. Ensure your `.env` contains your Eventstream Connection String:
   ```env
   FABRIC_EVENTSTREAM_CONN_STR="Endpoint=sb://..."
   ```
2. Run the simulator to stream live transactions:
   ```bash
   python sentinel_mesh_v2/data_simulator/simulator.py --mode stream
   ```

---

## 📅 Sprint Roadmap
* **Phase 1 (Complete)**: Workspace Setup & Ingestion (Fabric workspace & Eventstreams active).
* **Phase 2 (Active)**: Storage & Synapse (Eventhouse DDL and ingestion mapping).
* **Phase 3 (Next)**: Ontology & DNA (Spark vector calculation).
* **Phase 4**: Temporal Graph (GQL queries).
* **Phase 5**: Agent Swarm (KQL detection agents).
* **Phase 6**: AML Copilot & Action Gateway (Fabric Activator).
