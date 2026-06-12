# 🛡️ SENTINEL MESH — Cognitive AML Detection Mesh

SENTINEL MESH is a 9-layer cognitive architecture for real-time Anti-Money Laundering (AML) detection, built natively on Microsoft Fabric. It integrates real-time Eventstreams, Eventhouse (KQL DB), Lakehouse (Delta tables), a Temporal Knowledge Graph, and a Cognitive Agent Swarm to detect complex money laundering patterns with sub-second latency.

---

## 📁 Workspace Directory Structure

The project has been organized into a clean, professional structure:

```text
hack2future/
├── docs/                             # All documentation, slides, and diagrams
│   ├── SENTINEL_MESH_Blueprint.pptx  # PowerPoint presentation
│   ├── SENTINEL_MESH_Deep_Dive.docx  # 47-page technical whitepaper
│   ├── sentinel_mesh_blueprint.md    # Markdown architecture blueprint
│   ├── architecture_diagram.html     # Interactive 9-layer interactive flow
│   ├── architecture_overview.html    # Interactive workspace diagram
│   ├── roadmap_diagram.html          # Interactive sprint plan
│   └── simple_flow_diagram.html      # Simplified data flow layout
│
├── generators/                       # Python scripts to compile documents & slides
│   ├── generate_blueprint_ppt.py     # Generates the PowerPoint file
│   └── generate_sentinel_doc.py      # Generates the 47-page Word document
│
├── src/                              # The active codebase for the real-time POC
│   ├── data_simulator/               # Local transaction streaming simulator
│   │   ├── simulator.py              # Streams normal & AML pattern transactions
│   │   └── requirements.txt          # Simulator python dependencies
│   │
│   ├── kql/                          # Real-Time Intelligence DDL & agent queries
│   │   ├── L2_eventhouse_ddl.kql     # Schema for fact_transactions
│   │   ├── L5_swarm_agents.kql       # Structuring & Velocity KQL agents
│   │   └── L6_risk_scoring.kql       # Risk orchestration function
│   │
│   └── notebooks/                    # Spark Notebooks for semantic layers
│       ├── L3_IQ_Ontology_Setup.ipynb # DNA Vector calculation & ontology mapping
│       └── L4_Fabric_Graph_GQL.ipynb  # Temporal graph queries (circular flows)
│
├── .env                              # Local credentials (git-ignored)
├── implimentation_guide.md           # Master step-by-step implementation guide
├── task.md                           # Implementation task checklist
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
   python src/data_simulator/simulator.py --mode stream
   ```

---

## 📅 Sprint Roadmap
* **Phase 1 (Complete)**: Workspace Setup & Ingestion (Fabric workspace & Eventstreams active).
* **Phase 2 (Active)**: Storage & Synapse (Eventhouse DDL and ingestion mapping).
* **Phase 3 (Next)**: Ontology & DNA (Spark vector calculation).
* **Phase 4**: Temporal Graph (GQL queries).
* **Phase 5**: Agent Swarm (KQL detection agents).
* **Phase 6**: AML Copilot & Action Gateway (Fabric Activator).
