# 🔍 SENTINEL MESH V2 — Anomaly Detection Plan
## Hybrid AI-Powered Detection: KQL Plugins + AI Agents + Swarm Orchestration

---

## 1. Detection Philosophy

> **Rules detect what you KNOW is suspicious.**
> **AI Agents detect what IS suspicious — including things you didn't know to look for.**

SENTINEL MESH V2 uses a **3-layer hybrid approach** where each layer does what it's best at:

```
┌─────────────────────────────────────────────────────────┐
│                                                         │
│   LAYER 1: KQL AI PLUGINS                              │
│   → Scans ALL transactions in real-time (sub-second)    │
│   → Statistical anomaly detection (math-based)          │
│   → Runs on every single transaction automatically      │
│                                                         │
│                        ↓ flags anomalies                │
│                                                         │
│   LAYER 2: SEMANTIC KERNEL SWARM                       │
│   → 6 specialized AI agents analyze flagged entities    │
│   → Each agent reasons about its domain                 │
│   → Orchestrator builds consensus (weighted vote)       │
│                                                         │
│                        ↓ high/critical scores           │
│                                                         │
│   LAYER 3: AZURE AI INVESTIGATION AGENT                │
│   → Deep autonomous investigation                       │
│   → Explains reasoning in natural language              │
│   → Generates SAR narrative                             │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Why Hybrid?

| Single Approach | Problem |
|---|---|
| Only KQL rules | Catches known patterns, misses novel fraud, can't explain decisions |
| Only AI agents on every transaction | Too slow — can't process thousands of transactions per second |
| Only ML models | Black box — regulators need explainable decisions |

**Hybrid solves all three**: KQL handles volume (fast), AI agents handle reasoning (smart), and the combination provides explainability (trustworthy).

---

## 2. Where Anomaly Detection Happens (By Layer)

```
L3 ──→ DNA DRIFT DETECTION
         "This customer's behavior changed from their baseline"

L4 ──→ GRAPH ANOMALY DETECTION
         "This customer's relationships are structurally suspicious"

L5 ──→ PATTERN-SPECIFIC DETECTION (6 AI Agents)
         "This customer matches specific fraud techniques"

L6 ──→ ENSEMBLE DECISION
         "Combining all evidence — is this truly anomalous?"

L7 ──→ INVESTIGATION & EXPLANATION
         "Here's WHY this is anomalous, in human language"
```

---

## 3. Detection Layer 1: KQL AI Plugins (The Math Engine)

### What It Does
- Runs on **every transaction** that enters Eventhouse
- Uses built-in KQL machine learning operators
- Sub-second detection — no external service calls
- Purely statistical — no hard-coded thresholds

### 3.1 `series_decompose_anomalies` — Time-Series Anomaly Detection

**Purpose**: Detects when a customer's transaction pattern deviates statistically from their historical baseline.

**How it works**:
- Takes the last 30 days of transactions for each customer
- Builds a time-series (hourly or daily aggregates)
- Decomposes into: baseline trend + seasonal pattern + residual noise
- If residual exceeds a configurable number of standard deviations → **anomaly flagged**

**What it catches**:
- Sudden transaction volume spikes
- Unusual transaction amounts compared to personal history
- Activity during abnormal hours
- Any deviation from the customer's own established pattern

**What it replaces from V1**:
- Hard-coded `where max_txn_amount > 2000000` → replaced by statistical deviation
- Hard-coded `confidence = 0.89` → replaced by actual anomaly score magnitude

**Key advantage**: Personalized per customer. ₹10L is anomalous for a student but normal for a jeweler. The math knows the difference.

### 3.2 `autocluster` — Automatic Pattern Discovery

**Purpose**: Groups suspicious transactions and discovers what they have in common — without any predefined rules.

**How it works**:
- Takes a set of transactions (e.g., all flagged transactions from last 24 hours)
- Automatically finds clusters of similar transactions
- Reports what attributes the cluster shares (channel, location, time, amount range)

**What it catches**:
- "85% of flagged transactions used UPI from Bangalore between 2-4 PM" → pattern nobody programmed
- "All circular flows in the last week involved accounts opened within 30 days" → new insight
- Unknown fraud patterns that don't match any existing agent rules

**What it replaces from V1**:
- Hard-coded structuring rule (total > 10L and count >= 3) → ML discovers the actual pattern

**Key advantage**: Discovers patterns you didn't know to look for. Novel fraud techniques are caught automatically.

### 3.3 `diffpatterns` — Suspicious vs Normal Comparison

**Purpose**: Compares suspicious customers against normal customers and identifies the distinguishing features automatically.

**How it works**:
- Splits customers into two groups: flagged (suspicious) and normal
- Statistically compares every attribute between the groups
- Reports which attributes are significantly different

**What it catches**:
- "Suspicious customers have 4x more counterparties than normal"
- "Flagged transactions have 90% round number amounts vs 30% for normal"
- "Suspicious accounts were opened 3x more recently than normal"

**What it replaces from V1**:
- Hard-coded agent WHERE clauses → ML identifies the real distinguishing factors

**Key advantage**: Continuously recalculates as new data arrives. The distinguishing factors evolve with the data.

### 3.4 `basket` — Frequent Pattern Mining

**Purpose**: Finds which combinations of attributes frequently appear together in suspicious activity.

**How it works**:
- Analyzes suspicious transactions as itemsets
- Finds frequent combinations of (channel + location + amount_range + time_of_day)
- Reports combinations that appear more often than expected

**What it catches**:
- "Branch + Bangalore + ₹9-10L + Morning" appears in 70% of structuring cases
- "Mobile + Shared_Device + Transfer + Evening" appears in 80% of shadow link cases
- Multi-attribute fraud signatures discovered automatically

**What it replaces from V1**:
- Hard-coded merchant IDs (MERC-SHELL-9999) → statistical identification of suspicious merchant profiles

### 3.5 `series_periods_detect` — Periodic Behavior Detection

**Purpose**: Detects if a customer has suspicious periodic (repeating) transaction patterns.

**How it works**:
- Analyzes transaction time-series for periodic cycles
- Detects if money moves in regular intervals (e.g., every 72 hours, every Tuesday)

**What it catches**:
- Money rotating in a circle every 3 days (layering scheme)
- Regular structured deposits at the same time each week
- Automated money movement patterns (bot-driven laundering)

**What it replaces from V1**:
- No periodic detection existed in V1 — entirely new capability

### 3.6 `make-graph` / `graph-match` — Structural Anomaly Detection

**Purpose**: Detects anomalous graph structures — circular flows, hub accounts, isolated clusters.

**How it works**:
- Builds a graph from transactions: account → sends_to → account
- Uses `graph-match` to find patterns: cycles (A→B→C→A), stars (one account connected to many), chains (A→B→C→D→E)
- Variable-length path matching (3 to 10 hops)

**What it catches**:
- Circular fund flows of any length (not just 3 hops)
- Hub accounts that facilitate money movement for many others
- Isolated clusters of accounts that only transact with each other

**What it replaces from V1**:
- Slow Spark DataFrame joins → sub-second KQL native graph
- Hard-coded 3-hop only → dynamic 3-10 hop detection

### 3.7 `evaluate python()` — Inline ML Models

**Purpose**: Run scikit-learn or other ML models directly inside KQL for advanced anomaly detection.

**How it works**:
- KQL prepares the feature set (transaction aggregates per customer)
- Passes to inline Python which runs Isolation Forest (unsupervised anomaly detection)
- Results return to KQL for further processing

**What it catches**:
- Multi-dimensional anomalies that no single KQL plugin can detect
- Complex interactions between features (e.g., high velocity + low counterparty diversity + new account = suspicious combination)

**Key advantage**: Full ML power without leaving KQL. No separate infrastructure needed.

---

## 4. Detection Layer 2: Semantic Kernel Swarm (The Reasoning Engine)

### What It Does
- Activated when KQL plugins flag a customer as potentially anomalous
- 6 specialized AI agents each analyze from their domain perspective
- Each agent **reasons** about the data — not just filters it
- An orchestrator collects all opinions and builds consensus

### 4.1 The 6 AI Agents

**Agent 1: Structuring Sentinel**
- **Mandate**: Detect smurfing — multiple transactions below reporting threshold
- **How it reasons**: "This customer made 6 deposits in 8 hours. Each is ₹7-9.5L (below the ₹10L CTR threshold). Total is ₹50L. The amounts are not round numbers but cluster just below the threshold. The locations span across the city. This is consistent with deliberate structuring to avoid reporting."
- **Data sources**: KQL transaction aggregates + `series_decompose_anomalies` output
- **Confidence**: Calculated from z-score of total amount deviation + number of sub-threshold deposits

**Agent 2: Velocity Anomaly Detector**
- **Mandate**: Detect when transaction velocity/volume deviates from behavioral DNA baseline
- **How it reasons**: "This customer's 30-day average is 2 transactions/day totaling ₹5K. Today they made 15 transactions totaling ₹50L. This is a 4.2σ deviation from their behavioral baseline. The burst started at 9 AM and concentrated in an 8-hour window."
- **Data sources**: DNA-1D vs DNA-30D comparison + `series_decompose_anomalies` scores
- **Confidence**: Directly from anomaly score magnitude (higher deviation = higher confidence)

**Agent 3: Geo-Temporal Analyzer**
- **Mandate**: Detect impossible travel and geo-anomalies
- **How it reasons**: "This customer made a branch deposit in Koramangala at 9:15 AM and an ATM withdrawal in Whitefield at 9:45 AM. These locations are 25 km apart. Reaching there in 30 minutes during Bangalore morning traffic is improbable. Combined with the transaction pattern, this suggests multiple people using the same account."
- **Data sources**: Transaction locations + timestamps + distance calculation
- **Confidence**: Based on distance/time ratio vs realistic travel speed

**Agent 4: Circular Flow Tracker**
- **Mandate**: Detect round-trip money flows indicating layering
- **How it reasons**: "Account A sent ₹12.5L to Account B, which sent ₹12.5L to Account C, which sent ₹12.5L back to Account A — all within 10 seconds. The amounts are identical and the timing is near-instantaneous. This is a textbook circular flow pattern used for layering."
- **Data sources**: KQL `make-graph` / `graph-match` results
- **Confidence**: Based on cycle length, amount consistency, and timing tightness

**Agent 5: Shadow Link Discoverer**
- **Mandate**: Detect hidden relationships between supposedly unrelated customers
- **How it reasons**: "Customer Raj Kumar and Customer Priya Sharma share the same mobile device (DEV-MOBI-1102) and the same IP address (192.168.4.150). They have no declared relationship. Combined, they moved ₹78L in 2 days. The shared infrastructure suggests coordinated activity by related parties who are concealing their connection."
- **Data sources**: Device entity table + IP entity table + transaction data
- **Confidence**: Based on number of shared attributes × transaction volume between linked customers

**Agent 6: Shell Entity Profiler**
- **Mandate**: Detect shell companies and suspicious merchant activity
- **How it reasons**: "Merchant MERC-SHELL-9999 has a shell score of 0.89. 95% of its transaction volume comes from just 3 customers. It was registered 45 days ago and immediately started processing high-value transactions. The MCC code (5999 — Miscellaneous) is commonly associated with shell operations. This merchant profile is consistent with a front for money laundering."
- **Data sources**: Merchant dimension data + transaction concentration analysis + `diffpatterns` comparison
- **Confidence**: Based on shell score + counterparty concentration + merchant age

### 4.2 Swarm Orchestrator

**What it does**:
1. Receives findings from all 6 agents
2. Deduplicates overlapping findings
3. Cross-correlates: "Structuring + Shadow Link on the same customer = higher risk than either alone"
4. Calculates weighted consensus using config-driven weights
5. Produces a consolidated risk assessment

**Consensus Logic**:

| Scenario | Orchestrator Decision |
|---|---|
| 1 agent flags, low confidence | MEDIUM monitoring — could be noise |
| 2+ agents flag same customer | HIGH risk — corroborated evidence |
| 3+ agents flag with high confidence | CRITICAL — multiple independent signals agree |
| Agents disagree (some flag, some clear) | Weigh by confidence — strongest signals win |
| Structuring + Shadow Link together | Escalate — coordinated scheme detected |
| Circular Flow + Shell Entity together | Escalate — layering through shell company |

**Output**: A structured JSON with:
- Customer ID
- Risk score (0-100)
- Risk tier (LOW/MEDIUM/HIGH/CRITICAL)
- Contributing agents and their individual findings
- Cross-correlation analysis
- Recommended action

---

## 5. Detection Layer 3: Azure AI Investigation Agent (The Explainer)

### What It Does
- Activated only for HIGH and CRITICAL risk customers (not every transaction)
- Receives the orchestrator's output + raw data access
- Conducts an **autonomous deep investigation**
- Produces a human-readable investigation brief
- Generates a structured SAR narrative if needed

### 5.1 Investigation Process

When triggered for a customer, the agent autonomously:

**Step 1: Profile Review**
- Pulls customer profile from dim_customer
- Checks KYC status, PEP flag, country risk
- Reviews account details (age, type, dormancy)

**Step 2: Transaction Analysis**
- Queries full transaction history from Eventhouse
- Identifies the specific transactions that triggered the alert
- Calculates total volume, frequency, patterns

**Step 3: Behavioral DNA Check**
- Compares DNA-1D (current) vs DNA-30D (baseline)
- Identifies which DNA dimensions drifted most
- Determines if this is a sudden change or gradual shift

**Step 4: Relationship Investigation**
- Checks for shadow links (shared devices, IPs)
- Checks graph centrality (is this a hub account?)
- Identifies connected entities and their risk scores
- Maps the full network of related customers

**Step 5: Historical Context**
- Checks for prior alerts on this customer
- Checks for prior SARs
- Checks for prior analyst dispositions (TP/FP)
- Determines if this is a recurring pattern or first-time flag

**Step 6: Synthesis & Explanation**
- Combines all findings into a coherent narrative
- Explains WHY the customer is suspicious (not just that they are)
- Quantifies the risk with specific evidence
- Recommends specific action (monitor / investigate / freeze / file SAR)

### 5.2 Investigation Output

The agent produces a structured investigation brief:

```
INVESTIGATION BRIEF — CUST-RAJ-4892 (Raj Kumar)

RISK ASSESSMENT: HIGH (Score: 78.4/100)

EXECUTIVE SUMMARY:
Raj Kumar conducted 6 structured deposits totaling ₹50L on a single 
day, each kept below the ₹10L reporting threshold. He shares a mobile 
device with Priya Sharma, who deposited ₹28L the previous day. Combined 
suspicious activity across linked customers totals ₹78L in 48 hours.

AGENTS THAT FLAGGED:
1. Structuring Sentinel (confidence: 0.92) — 6 sub-threshold deposits
2. Velocity Anomaly (confidence: 0.88) — 4.2σ deviation from baseline
3. Shadow Link (confidence: 0.94) — shared device DEV-MOBI-1102

BEHAVIORAL DNA DRIFT:
- Velocity: 2 txns/day → 6 txns/day (3x increase)
- Amount: ₹5K avg → ₹833K avg (166x increase)  
- Geo spread: 1 location → 5 locations (5x increase)
- Channel mix: 100% UPI → 50% Branch + 33% Mobile + 17% UPI

NETWORK ANALYSIS:
- Connected to: Priya Sharma (CUST-PS-1190) via shared device
- Combined network volume: ₹78L in 48 hours
- No declared relationship between customers

RECOMMENDED ACTION:
1. File Suspicious Activity Report (SAR)
2. Enhanced Due Diligence on both linked customers
3. Monitor all accounts for 30 days
4. Consider account freeze pending investigation
```

### 5.3 SAR Generation

If risk is CRITICAL (score > 80), the agent automatically generates a structured SAR:

**SAR Structure** (JSON format — machine-readable + human-readable):
- `executive_summary` — one paragraph overview
- `subject_information` — customer details, accounts, relationships
- `suspicious_activity_description` — what happened, timeline, amounts
- `red_flags` — list of specific red flags with evidence
- `behavioral_analysis` — DNA drift analysis
- `network_analysis` — connected entities and relationships
- `supporting_evidence` — specific transaction IDs, timestamps, amounts
- `recommended_actions` — regulatory filing, account actions, monitoring

**Validation**: Azure AI Content Safety checks every SAR before storing to ensure:
- No hallucinated facts (all claims tied to real data)
- No bias in language
- Compliance-grade professional tone

**Storage**: SAR saved to `fact_sar_reports` Delta table with full metadata (model version, prompt hash, generation timestamp)

---

## 6. How All Three Layers Work Together

### End-to-End Detection Flow

```
TRANSACTION ARRIVES IN EVENTHOUSE
           │
           ▼
    ┌──────────────┐
    │  KQL PLUGINS │  ← Runs on EVERY transaction (sub-second)
    │              │
    │  series_decompose_anomalies → "statistical outlier?"
    │  autocluster → "matches suspicious cluster?"
    │  make-graph → "part of circular flow?"
    │  basket → "suspicious attribute combination?"
    │              │
    │  Result: anomaly_score per customer
    └──────┬───────┘
           │
           │ If anomaly_score > threshold
           ▼
    ┌──────────────┐
    │  6 AI AGENTS │  ← Runs on FLAGGED customers only (2-3 seconds)
    │              │
    │  Agent 1: Structuring → "smurfing detected? confidence?"
    │  Agent 2: Velocity → "DNA drift detected? how much?"
    │  Agent 3: Geo-Temporal → "impossible travel? distance/time?"
    │  Agent 4: Circular Flow → "money loop? how many hops?"
    │  Agent 5: Shadow Link → "shared devices/IPs? with whom?"
    │  Agent 6: Shell Entity → "shell merchant? concentration?"
    │              │
    │  Each returns: finding + confidence + reason
    └──────┬───────┘
           │
           ▼
    ┌──────────────┐
    │ ORCHESTRATOR │  ← Coordinates all agents (1-2 seconds)
    │              │
    │  Deduplicate findings
    │  Cross-correlate (structuring + shadow link = escalate)
    │  Calculate weighted consensus score
    │  Determine risk tier (LOW/MEDIUM/HIGH/CRITICAL)
    │              │
    │  Result: risk_score + risk_tier + reasons
    └──────┬───────┘
           │
           │ If risk_tier = HIGH or CRITICAL
           ▼
    ┌──────────────────┐
    │ INVESTIGATION    │  ← Runs on HIGH/CRITICAL only (5-10 seconds)
    │ AGENT            │
    │                  │
    │  Profile review → Transaction analysis → DNA check
    │  → Relationship mapping → Historical context
    │  → Synthesis → Investigation brief
    │                  │
    │  If CRITICAL: auto-generate SAR
    │  Content Safety validates output
    └──────┬───────────┘
           │
           ▼
    ┌──────────────┐
    │   ACTIONS    │
    │              │
    │  → Store alert in fact_alerts
    │  → Store SAR in fact_sar_reports (if generated)
    │  → Fabric Activator → Teams/Email notification
    │  → Logic Apps → Account freeze workflow (if CRITICAL)
    │  → Dashboard updates via SignalR
    │  → Purview audit trail logged
    └──────────────┘
```

### Timing Breakdown

| Step | What | Time | Runs On |
|---|---|---|---|
| KQL AI Plugins | Statistical scan | < 1 second | Every transaction |
| 6 AI Agents | Domain-specific analysis | 2-3 seconds | Flagged customers only |
| Orchestrator | Consensus + scoring | 1-2 seconds | Flagged customers only |
| Investigation Agent | Deep investigation + SAR | 5-10 seconds | HIGH/CRITICAL only |
| **Total (worst case)** | **End-to-end** | **< 15 seconds** | **CRITICAL customers** |

For normal transactions: detection completes in **under 1 second**.
For suspicious transactions: full investigation completes in **under 15 seconds**.

---

## 7. What Makes This Different From V1

| Aspect | V1 (Old) | V2 (New) |
|---|---|---|
| **Detection method** | Static KQL WHERE clauses | KQL AI plugins (statistical ML) |
| **Thresholds** | Hard-coded (₹10L, ₹20L) | Config-driven + auto-adjusted by L9 |
| **Confidence scores** | Magic numbers (0.95, 0.89) | Calculated from statistical deviation |
| **Pattern discovery** | Only catches patterns you coded | `autocluster` discovers unknown patterns |
| **Explanation** | None — just data tables | AI agent writes natural language explanation |
| **Graph detection** | Spark joins (slow, 3-hop only) | KQL make-graph (sub-second, 3-10 hops) |
| **Personalization** | Same rules for everyone | Each customer compared to their OWN baseline |
| **Multi-agent reasoning** | No coordination | 6 agents debate + orchestrator decides |
| **SAR generation** | Manual script with hard-coded data | Autonomous AI agent with structured output |
| **Learning** | Never improves | L9 feedback recalibrates everything |

---

## 8. What Each Layer Catches (That Others Miss)

### KQL Plugins Catch:
- ✅ Statistical anomalies in transaction volume/amount
- ✅ Unknown patterns via autocluster
- ✅ Periodic suspicious behavior
- ❌ Cannot explain why something is suspicious
- ❌ Cannot investigate across multiple data sources

### AI Agents Catch:
- ✅ Domain-specific fraud techniques (structuring, layering, shell companies)
- ✅ Multi-source correlation (transactions + DNA + graph + devices)
- ✅ Reasoning about WHY something is suspicious
- ❌ Too slow to run on every transaction
- ❌ Cannot discover completely unknown patterns

### Investigation Agent Catches:
- ✅ Connected entity networks (shadow links, beneficial owners)
- ✅ Historical context (prior alerts, SARs, dispositions)
- ✅ Full narrative explanation for regulators
- ❌ Only activated for high-risk cases (by design)

### Together They Catch Everything:
- ✅ Known fraud patterns (agents)
- ✅ Unknown fraud patterns (autocluster)
- ✅ Statistical anomalies (series_decompose)
- ✅ Graph anomalies (make-graph)
- ✅ Behavioral drift (DNA comparison)
- ✅ Hidden relationships (shadow link agent)
- ✅ Coordinated schemes (orchestrator cross-correlation)
- ✅ Explainable decisions (investigation agent)

---

## 9. Implementation Priority

| Order | Component | Effort | Impact |
|---|---|---|---|
| **1st** | KQL `series_decompose_anomalies` in Velocity Agent | Low | Replaces hard-coded thresholds with real statistics |
| **2nd** | KQL `make-graph` / `graph-match` for Circular Flow | Low | Replaces slow Spark joins with sub-second detection |
| **3rd** | Config-driven scoring weights (dim_adaptive_config) | Low | Fixes the all-35.0 bug |
| **4th** | KQL `autocluster` for pattern discovery | Low | Adds entirely new capability (unknown pattern detection) |
| **5th** | KQL `diffpatterns` for suspicious vs normal comparison | Low | Identifies real distinguishing features |
| **6th** | Azure AI Agent for investigation + explanation | Medium | The wow factor for judges |
| **7th** | Semantic Kernel swarm orchestrator | Medium | Multi-agent coordination and consensus |
| **8th** | Structured SAR generation with Content Safety | Medium | Compliance-grade AI output |
| **9th** | L9 feedback → auto-recalibration | Medium | Self-learning system |
| **10th** | `evaluate python()` with Isolation Forest | Low | Advanced unsupervised ML inside KQL |

---

## 10. Key Metrics to Track

| Metric | What It Tells You | Target |
|---|---|---|
| **Detection Latency** | Time from transaction → risk score | < 2 seconds |
| **Investigation Latency** | Time from flag → full investigation brief | < 15 seconds |
| **True Positive Rate** | % of alerts that are real fraud | > 85% |
| **False Positive Rate** | % of alerts that are noise | < 15% |
| **Pattern Discovery Rate** | New patterns found by autocluster per week | Track over time |
| **Agent Agreement Rate** | How often 3+ agents agree | Higher = more reliable |
| **DNA Drift Detection Rate** | % of behavioral shifts caught | > 90% |
| **SAR Quality Score** | Content Safety + analyst review rating | > 4/5 |

---

> **This anomaly detection system is not a rule engine with AI sprinkled on top. It's an AI-native detection system with rules as a safety net. The AI discovers, reasons, and explains. The rules catch the obvious. Together, they miss nothing.**
