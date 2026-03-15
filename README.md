<div align="center">

# Big Data Analytics with PySpark

### Distributed Data Processing Across 4 Analytical Domains

*Batch processing &middot; graph analytics &middot; real-time streaming &middot; blockchain analysis*

<br>

[![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![GraphFrames](https://img.shields.io/badge/GraphFrames-0.8-4285F4?style=for-the-badge)](https://graphframes.github.io/)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)

[![GitHub stars](https://img.shields.io/github/stars/abailey81/Big-data-spark-analytics?style=social)](https://github.com/abailey81/Big-data-spark-analytics/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/abailey81/Big-data-spark-analytics?style=social)](https://github.com/abailey81/Big-data-spark-analytics/network/members)

---

**Production-style Apache Spark analytics** demonstrating batch processing, graph analytics, exploratory analysis, and real-time streaming on NYC taxi data, Ethereum blockchain records, and NASA server logs.

<br>

[Modules](#analytical-modules) &middot; [Architecture](#architecture) &middot; [Datasets](#datasets) &middot; [Getting Started](#getting-started)

</div>

<br>

## Highlights

<table>
<tr>
<td align="center" width="25%">
<br>
<strong>Batch Analytics</strong>
<br><br>
Multi-gigabyte NYC taxi data cleansing, aggregation, and KPI extraction with PySpark DataFrames
<br><br>
</td>
<td align="center" width="25%">
<br>
<strong>Graph Analytics</strong>
<br><br>
Directed travel network with PageRank, degree analysis, and shortest path via GraphFrames
<br><br>
</td>
<td align="center" width="25%">
<br>
<strong>Blockchain EDA</strong>
<br><br>
Ethereum transaction parsing, gas price dynamics, and monthly trend comparison
<br><br>
</td>
<td align="center" width="25%">
<br>
<strong>Real-Time Streaming</strong>
<br><br>
Spark Structured Streaming on NASA logs with watermarking and sliding windows
<br><br>
</td>
</tr>
</table>

---

## Analytical Modules

| # | Module | Dataset | Questions | Key Operations |
|:-:|:-------|:--------|:---------:|:---------------|
| 1 | **NYC Yellow Taxi** (Batch) | Multi-GB taxi trip CSV from S3 | 7 | Schema inference, joins, aggregation, window functions, Parquet output |
| 2 | **Ethereum Transactions** (EDA) | Blockchain blocks + transactions | 6 | Timestamp parsing, gas price histograms, monthly trend comparison |
| 3 | **NYC Taxi Network** (Graph) | Taxi zone lookup + Green Taxi trips | 6 | Vertex/edge creation, PageRank, degree analysis, shortest path |
| 4 | **NASA Logs** (Streaming) | HTTP request logs via socket | 6 | Micro-batch ingestion, watermarking, sliding window anomaly detection |

---

## Architecture

```
                         +------------------+
                         |   S3 Data Lake   |    NYC Taxi CSVs, Ethereum blocks,
                         |  (AWS / MinIO)   |    Zone lookups
                         +--------+---------+
                                  |
                    +-------------+-------------+
                    |                           |
             +------v------+            +------v------+
             |   PySpark   |            |   Socket    |    NASA log stream
             | Batch/Graph |            |  Streaming  |    (port 5551)
             +------+------+            +------+------+
                    |                           |
          +---------+---------+          +------v------+
          |         |         |          |  Structured |
     +----v---+ +--v----+ +--v-----+    |  Streaming  |
     | Task 1 | |Task 2 | | Task 3 |    |  Pipeline   |
     | Batch  | |  EDA  | | Graph  |    +------+------+
     +----+---+ +--+----+ +--+-----+           |
          |        |          |                 |
          +--------+----------+---------+-------+
                                        |
                              +---------v---------+
                              |   Report Output   |
                              | Figures + Console  |
                              +-------------------+
```

---

## Datasets

| Module | Source | Format | Key Columns |
|:-------|:-------|:------:|:------------|
| NYC Taxi (Batch) | NYC Yellow Taxi 2023 | CSV (S3) | pickup/dropoff datetime, fare, distance, tip, passenger count |
| Ethereum | Ethereum Blockchain | CSV (S3) | timestamp, block hash, gas price, transaction value |
| NYC Taxi (Graph) | Zone Lookup + Green Taxi | CSV (S3) | LocationID, Borough, Zone, PU/DO LocationID |
| NASA Logs | NASA HTTP Logs | Socket stream | hostname, timestamp, method, URL, response code, bytes |

---

<details>
<summary><strong>Task 1 &mdash; NYC Yellow Taxi (Batch Analytics)</strong></summary>

<br>

**Goal:** Cleanse, aggregate, and analyse multi-gigabyte taxi trip data to derive operational KPIs.

| # | Question | Technique |
|:-:|:---------|:----------|
| 1 | Load large CSV files and inspect schema consistency | Schema inference, `printSchema()` |
| 2 | Remove invalid records (zero fare, negative distance) | Filtering, date range validation |
| 3 | Join with zone lookup tables for location enrichment | Left joins on LocationID |
| 4 | Create routes and compute monthly aggregations | GroupBy, window functions |
| 5 | Aggregate statistics by route and month | Average tips, passenger counts |
| 6 | Filter routes with zero average tips per passenger | Conditional aggregation |
| 7 | Identify top 10 highest-tipping routes by month | Ranking, orderBy |

</details>

<details>
<summary><strong>Task 2 &mdash; Ethereum Transactions (Exploratory Analytics)</strong></summary>

<br>

**Goal:** Parse and summarise blockchain transaction data; analyse gas price dynamics and user activity.

| # | Question | Technique |
|:-:|:---------|:----------|
| 1 | Load blocks and transactions datasets | Schema inference, display |
| 2 | Parse UNIX timestamps to readable format | `from_unixtime()`, date formatting |
| 3 | Transform and display timestamp samples | Column aliasing |
| 4 | Join blocks and transactions on block hash | Inner join, record counting |
| 5 | Generate value and gas price histograms | Histogram visualization |
| 6 | Compare monthly transaction trends (Sep vs Oct 2015) | Monthly aggregation, comparison plots |

</details>

<details>
<summary><strong>Task 3 &mdash; NYC Taxi Network (Graph Analytics)</strong></summary>

<br>

**Goal:** Construct and analyse a directed graph of taxi trips to study travel flows and influential zones.

| # | Question | Technique |
|:-:|:---------|:----------|
| 1 | Create vertices (zones) and edges (trip connections) | GraphFrames construction |
| 2 | Compute in-degree and out-degree per zone | Degree analysis |
| 3 | Display graph triplets with location metadata | Triplet filtering |
| 4 | Filter connected vertices with same Borough | Graph pattern matching |
| 5 | Run PageRank to find influential pickup areas | PageRank algorithm |
| 6 | Visualise network with degree weighting | Network visualization |

</details>

<details>
<summary><strong>Task 4 &mdash; NASA Logs (Streaming Analytics)</strong></summary>

<br>

**Goal:** Build a Spark Structured Streaming pipeline for NASA web server logs.

| # | Question | Technique |
|:-:|:---------|:----------|
| 1 | Stream log data via socket (10s micro-batch) | Socket source, `readStream` |
| 2 | Add watermarking for late-arriving data | 3-second watermark threshold |
| 3 | Parse log fields (host, time, method, URL, status) | Regex parsing, `explode`/`split` |
| 4 | Count requests per host over sliding window | Windowed aggregation |
| 5 | Detect most frequent status codes (last 60s) | Time-window grouping |
| 6 | Identify request rate anomalies | Threshold-based detection, console sink |

</details>

---

## Getting Started

```bash
git clone https://github.com/abailey81/Big-data-spark-analytics.git
cd Big-data-spark-analytics

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run individual task scripts
python tasks/task1/Question1.py
python tasks/task2/Question1.py
python tasks/task3/Question1.py
python tasks/task4/Question1.py
```

> **Note:** Task scripts require S3 credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) and a configured Spark environment. Streaming tasks (Task 4) require the NASA log socket emulator.

---

## Project Structure

```
Big-data-spark-analytics/
├── src/
│   └── bdsa/
│       ├── __init__.py
│       └── ingest.py               # Data ingestion utilities
├── tasks/
│   ├── task1/                      # NYC Yellow Taxi (7 questions)
│   │   └── Question[1-7].py
│   ├── task2/                      # Ethereum Transactions (6 questions)
│   │   └── Question[1-6].py
│   ├── task3/                      # NYC Taxi Network (6 questions)
│   │   └── Question[1-6].py
│   └── task4/                      # NASA Logs Streaming (6 questions)
│       └── Question[1-6].py
├── report/                         # Output figures and results
├── notebooks/                      # Jupyter notebooks
├── docs/                           # Documentation
├── requirements.txt                # Python dependencies
├── CONTRIBUTING.md                 # Contribution guidelines
├── SECURITY.md                     # Security policy
├── LICENSE                         # MIT License
└── README.md
```

---

<div align="center">

**[MIT License](LICENSE)**

Built with Apache Spark, PySpark, GraphFrames, and Matplotlib

</div>
