# Automated-Food-Manufacturing-Production-Intelligence-System-
I built an end-to-end Python automation pipeline that replaces an entire 3-hour manual workflow with a 3-second autonomous process.
# Automated Food Manufacturing Intelligence System

## The Industry Problem
Having worked in the food manufacturing industry in the Philippines for 10 years, I have seen a universal bottleneck: **Data bottlenecks and Excel fatigue.**

Even in facilities with SAP or ERP systems, the daily workflow is painfully manual:
1. Production clerks fill out paper monitoring forms.
2. Encoders manually input this into Excel or SAP.
3. Data is exported as raw CSVs and emailed to Supervisors/Managers.
4. Managers spend hours manually cleaning data, manipulating spreadsheets, calculating descriptive statistics, and updating lagging Excel trackers.
5. Finally, charts are screenshotted and pasted into PowerPoint for weekly management meetings.

All of this happens while production managers are simultaneously trying to put out daily operational fires and attend meetings. The result? Lost data, lagging spreadsheets, delayed decision-making, and hundreds of wasted man-hours.

## The Solution
I built this **Automated Food Manufacturing Production Intelligence System** using Python to completely eliminate the manual reporting workflow. It transforms raw, messy data into executive insights in *seconds*, autonomously.

### How It Works (The Automated Workflow)
1. **The "Drop Zone" (Watchdog):** Encoders simply drag-and-drop the daily SAP/Excel CSV export into a specific folder. That’s their only job.
2. **The ETL Pipeline:** A background Python script instantly detects the file, cleans the data, fixes formatting errors, and safely stores it in a secure relational database (SQLite). The raw file is then automatically timestamped and archived to prevent data loss.
3. **Statistical Process Control (SPC) AI:** The system automatically calculates historical 3-Sigma control limits. If a daily production run exceeds acceptable defect/reject rates, it generates a control chart and immediately emails the QA Managers.
4. **Automated Push Reporting:** At the end of the process, the system generates an HTML email with embedded charts summarizing the daily yield and worst-performing SKUs, sending it directly to management's inbox.
5. **Live Executive Dashboard:** A Tableau-grade, interactive web dashboard (built with Streamlit) allows executives to view daily actuals, date-range historical trends, and cumulative running totals without ever opening Excel.

## Enterprise-Grade Reliability
This system was built with standard Software Engineering principles to ensure it never crashes, even with years of data:
* **Idempotency:** The database is strictly programmed to reject duplicate data. If an encoder accidentally uploads the same file twice, the system safely ignores the duplicates.
* **Scalability:** Unlike Excel, which lags and crashes after 50,000 rows, this system utilizes a relational SQL database capable of handling millions of production rows instantly.
* **Format-Proofing:** The data pipeline automatically detects and fixes corrupted Excel date formats (e.g., converting mixed `3/9/2024` and `2024-04-18` formats into chronological standard times).

## Tech Stack
* **Python** (Core Logic & Automation)
* **SQLite & Pandas** (Data Engineering & Storage)
* **Plotly** (Data Visualization)
* **Streamlit** (Interactive Web Dashboard)
* **Watchdog** (Event-driven file monitoring)
