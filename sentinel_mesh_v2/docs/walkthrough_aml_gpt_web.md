# 🛡️ SENTINEL MESH V2 — Layer 7 AML GPT Web Command Center Guide

This document explains how to launch, authenticate, and test the **AML GPT Web Command Center** interface, designed to showcase a premium, visual single-page application (SPA) during hackathon presentations or compliance audits.

---

## 🛠️ How to Launch the Web Command Center

### STEP 1: Execute the Web Server
Launch the FastAPI server from the workspace root:
```bash
python copilot/aml_gpt_web.py
```

### STEP 2: Authenticate the Connection
1. When the server launches, it will initialize the Kusto connection. If this is the first session, it will print a browser authentication prompt in the terminal:
   ```text
   To sign in, use a web browser to open the page https://microsoft.com/devicelogin and enter the code CXXXXXXXX to authenticate.
   ```
2. Open the page on your PC, type the device code, and sign in using your Microsoft Fabric credentials.
3. Once authenticated, the server console will show:
   ```text
   🔄 Establishing connection to Eventhouse. A browser login prompt may appear...
   INFO:     Started server process [12440]
   INFO:     Waiting for application startup.
   INFO:     Application startup complete.
   INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
   ```

### STEP 3: Open the Workspace Dashboard
* **On your PC**: Open [http://localhost:8000](http://localhost:8000) in your web browser.
* **On your Mobile Phone (Same Wi-Fi)**: Open `http://<YOUR_PC_LOCAL_IP>:8000` (e.g., `http://192.168.1.15:8000`) in your phone's browser.
* **On your Mobile Phone (Cellular/External Network)**: Expose the local port using `ngrok` or a similar tunneling service:
  ```bash
  ngrok http 8000
  ```
  Then open the generated HTTPS tunnel link (e.g. `https://xxxx.ngrok-free.app`) on your mobile browser.

---

## 🔬 Layout & Key Interactive Features

The Web Command Center contains three primary visual modules:

### 1. ⚠️ Live Alerts Feed (Left Sidebar)
* Automatically queries the latest 15 alerts in real-time from `fact_alerts`.
* Lists alerts with custom risk indicators (`CRITICAL` in flashing red, `HIGH` in orange, `MEDIUM` in gold, `LOW` in green).
* **Interactive Trigger**: Clicking any alert card in the sidebar automatically populates the chat prompt with a request to inspect that specific customer's records.

### 2. 🤖 AML GPT Chat Console (Right Main Pane)
* Type natural language queries in plain English.
* Shows a **Thinking Dot Animation** while translating prompts.
* Automatically renders:
  1. **KQL Code Block**: Displays the compiled KQL query executed against the Eventhouse hot-path.
  2. **Interactive Data Table**: Renders the rows returned by Eventhouse inside a sleek, scrollable table.
  3. **Executive Summary**: Displays the AI-synthesized explanation in clean formatting.

---

## 🎯 Sample Walkthrough Demo Script

1. **Ask a broad question**:
   * *"Which customers triggered alerts today?"*
   * Observe the compiled KQL query, the alerts table, and the compliance brief.
2. **Drill down on location data**:
   * *"who has more risk score who is from banglore"*
   * Observe how the model translates this query using the `city` column of the `dim_customer` table.
