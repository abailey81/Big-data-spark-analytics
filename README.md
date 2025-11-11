# Big Data Analytics with PySpark 

A complete, production-style analytics project built with **Apache Spark**, covering batch processing, graph analytics, and real-time streaming.  
This repository demonstrates how to design modular big-data pipelines, clean large datasets, perform advanced transformations, and generate insights at scale.

---

##  Project Overview

The project is divided into **four major analytical modules**, each originally designed as a standalone task.  
All questions have been re-implemented and extended into clean, professional Python modules.

| Module | Description | Core Dataset / Topic |
|:-------|:-------------|:---------------------|
| **Task 1 – NYC Yellow Taxi (Batch Analytics)** | Cleansing, aggregating, and analysing multi-gigabyte taxi trip data to derive operational KPIs. | NYC Yellow Taxi data (CSV) |
| **Task 2 – Ethereum Transactions (Exploratory Analytics)** | Parsing and summarising blockchain transactions; analysing gas price dynamics and user activity. | Ethereum transaction records |
| **Task 3 – NYC Taxi Network (Graph Analytics)** | Building a directed graph of taxi zones to study travel flows and influential pickup areas. | NYC Taxi Zone lookup |
| **Task 4 – NASA Logs (Streaming Analytics)** | Real-time ingestion of NASA web logs to detect request spikes and anomalies over sliding windows. | NASA HTTP request logs |

---

##  Repository Structure



---

##  Task Summaries & Questions

Below are the **analytical objectives** rephrased from the original coursework instructions.  
Each “Question X” corresponds to a dedicated, runnable script under the relevant `src/` folder.

---

###  **Task 1 – NYC Yellow Taxi (Batch Analytics)**  
**Goal:** Use PySpark DataFrames to perform cleaning, transformation, and exploratory analysis.

**Questions implemented:**
1. Load large CSV files into Spark and inspect schema consistency.  
2. Remove missing or invalid records (zero fare, negative trip distance, etc.).  
3. Compute key KPIs – total revenue, average trip distance, peak hours.  
4. Identify busiest pickup and drop-off zones.  
5. Aggregate monthly trip counts and average fares.  
6. Calculate correlation between fare amount and trip distance.  
7. Save clean data partitions in Parquet format.

➡️ All results and code live in: `src/nyc_taxi_batch/nyc_taxi_batch_01-07.py`

---

###  **Task 2 – Ethereum Transactions (Exploratory Analytics)**  
**Goal:** Analyse blockchain transaction data to uncover time and value patterns.

**Questions implemented:**
1. Load `september_2015_document.csv` and `october_2015_document.csv`.  
2. Parse timestamps into Spark SQL datetime format.  
3. Compute the number of transactions per day.  
4. Determine min/max/average gas price.  
5. Generate histograms of transaction value and gas price distributions.  
6. Compare monthly transaction trends between September and October.  

➡️ Results visualised under `report/figures/ethereum/`.

---

###  **Task 3 – NYC Taxi Network (Graph Analytics)**  
**Goal:** Construct and analyse a graph representation of taxi trips.

**Questions implemented:**
1. Create vertices (zones) and edges (trip connections).  
2. Compute in-degree and out-degree for each zone.  
3. Identify the top 10 most connected zones.  
4. Run PageRank to detect influential pickup areas.  
5. Find the shortest path between two selected zones.  
6. Visualise the network structure with degree weighting.

➡️ GraphFrames used throughout; outputs stored in `report/figures/nyc_graph/`.

---

###  **Task 4 – NASA Logs (Streaming Analytics)**  
**Goal:** Build a Spark Structured Streaming pipeline for NASA web server logs.

**Questions implemented:**
1. Stream log data from directory using a 10-second micro-batch interval.  
2. Parse timestamp, host, and status fields using regex and Spark schema.  
3. Count requests per host over a sliding time window.  
4. Detect the most frequent status codes in the last 60 seconds.  
5. Identify anomalies in request rate using thresholds.  
6. Write aggregated output to the console and checkpoint directory.

➡️ Streaming pipeline scripts: `src/nasa_streaming/nasa_streaming_01-06.py`.

---

##  Setup & Execution

### Local run (macOS)
```bash
git clone git@github.com:abailey81/Big-data-spark-analytics.git
cd Big-data-spark-analytics

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
