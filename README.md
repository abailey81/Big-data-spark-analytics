# Big Data Analytics with PySpark

Production-style data engineering and analytics in Apache Spark: batch processing, graph analytics, and structured streaming.  
This repo consolidates four tasks into a single, professional project.

## Contents
- Task 1 — Batch analytics (NYC Taxi): schema checks, cleaning, joins, aggregations, KPIs
- Task 2 — Exploratory analytics (e.g., Ethereum or similar): time handling, summaries, visuals
- Task 3 — Graph analytics (zone network): GraphFrames PageRank and shortest paths
- Task 4 — Streaming analytics (NASA logs): Structured Streaming with windows and watermarks

## Repository Layout
.
├── tasks/
│   ├── task1/                      # all files for task 1
│   ├── task2/                      # all files for task 2
│   ├── task3/                      # all files for task 3
│   └── task4/                      # all files for task 4
├── notebooks/                      # cleaned notebooks (add here)
├── report/                         # final PDF or slides
├── docs/                           # any additional docs you add
├── requirements.txt                # minimal environment
├── .gitignore                      # repo hygiene
├── .gitattributes                  # cleaner notebook diffs
└── README.md

## Quickstart (macOS)
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
jupyter notebook

## Notes
- data/ is git-ignored to keep the repo clean; keep large/raw files locally.
- Add your cleaned notebooks to notebooks/ with professional names:
  - notebooks/nyc_taxi_batch.ipynb
  - notebooks/zone_graph_analysis.ipynb
  - notebooks/nasa_streaming_windows.ipynb

## License
MIT (see LICENSE)
