# 🛡️ SENTINEL MESH V2 — Layer 7 AML GPT Guide

This document guides you through running and using the **Interactive AML GPT (Layer 7)**, which provides natural language query (NLQ) capabilities directly to your Fabric Eventhouse database.

---

## 📅 Summary of Capabilities
1. **Plain English to KQL Translation**: Uses Azure OpenAI `gpt-4o` to automatically parse and compile compliance questions into syntactically valid KQL queries.
2. **Fabric Eventhouse Hot-Path Connectivity**: Uses the official `azure-kusto-data` library and AAD Device Code Login to query Eventhouse tables securely from outside the Fabric portal.
3. **Smart Data Synthesis**: Summarizes complex JSON database records back into polished compliance investigator briefings.


---

## 🛠️ How to Run the AML GPT

### STEP 1: Run Verification (Syntax Check)
Run this check to confirm that the script compiles successfully without any syntax errors:
```bash
python -m py_compile copilot/aml_gpt.py
```

### STEP 2: Execute the Script
Trigger the interactive terminal application:
```bash
python copilot/aml_gpt.py
```

### STEP 3: Authenticate in the Browser
When running for the first time, you will see a message:
```text
To sign in, use a web browser to open the page https://microsoft.com/devicelogin and enter the code CXXXXXXXX to authenticate.
```
1. Open [https://microsoft.com/devicelogin](https://microsoft.com/devicelogin) in your browser.
2. Enter the code shown in the terminal.
3. Sign in using your Microsoft account (the one associated with your Fabric Workspace).
4. Once authenticated, the terminal will print:
   ```text
   ✅ Eventhouse connection verified successfully!
   ```

---

## 🔬 Test Scenarios (What to Ask)

Here are examples of natural language questions you can type into the prompt:

### Scenario A: Check Active Alerts
* **Question**: `Which customers triggered alerts today and what are their risk tiers?`
* **Translated KQL**: `fact_alerts | where triggered_at >= ago(1d) | project customer_name, risk_tier, risk_score | limit 10`
* **Response**: A formatted report showing the names, scores, and risk classifications of qualifying customers.

### Scenario B: Drill Down on a Specific Customer
* **Question**: `Show me all transactions for Triya Raj exceeding 500000`
* **Translated KQL**: `fact_transactions | where customer_name contains "Triya Raj" and amount > 500000 | project timestamp, account_id, amount, channel, geo_location | limit 10`
* **Response**: A detailed transaction journal explaining Triya Raj's structuring patterns.

### Scenario C: Investigate generated SAR drafts
* **Question**: `What actions are recommended in Triya Raj's SAR report?`
* **Translated KQL**: `fact_sar_reports | where customer_name contains "Triya Raj" | project recommended_actions | limit 1`

---

