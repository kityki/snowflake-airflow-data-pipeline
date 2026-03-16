# ✈️ Automated Snowflake & Airflow Data Pipeline

## Project Overview

This project is a fully automated ELT (Extract, Load, Transform) data pipeline designed to process airline flight data. It leverages **Apache Airflow** for orchestration and **Snowflake** for cloud compute, storage, and data warehousing.

The pipeline extracts raw CSV data, loads it into a Snowflake staging area, and uses Change Data Capture (CDC) to incrementally clean and transform the data into a production-ready Star Schema.

## 🏗️ Architecture & Data Flow

The architecture strictly follows the ELT paradigm, separating orchestration from execution. Airflow acts as the manager, while Snowflake handles the heavy lifting.

1. **Stage 1: Raw Ingestion**
   - Airflow uses the `SnowflakeOperator` to upload raw CSV files into a Snowflake Internal Stage.
   - A `COPY INTO` command maps the data into a `RAW_AIRLINE_DATA` table.
2. **Stage 2: Integration & Cleaning**
   - A **Snowflake Stream** tracks inserts on the raw table (Change Data Capture).
   - A Stored Procedure cleans the incoming data and loads it into `CLEAN_AIRLINE_DATA`, ensuring only _new_ records are processed to optimize compute costs.
3. **Stage 3: Core Star Schema**
   - A second Stream watches the clean table.
   - Using an `INSERT ALL` command within a Stored Procedure, the pipeline distributes the flat data into a normalized **Star Schema** for fast OLAP querying:
     - `DIM_PASSENGER` (Passenger demographics)
     - `DIM_AIRPORT` (Location data)
     - `FACT_FLIGHT_ACTIVITY` (Flight events and ticket types)

## 🛠️ Technologies Used

- **Data Warehouse:** Snowflake (Internal Stages, Streams, Stored Procedures, RLS)
- **Orchestration:** Apache Airflow (Python, DAGs, SnowflakeOperator)
- **Language:** SQL, Python 3.8
- **Version Control:** Git & GitHub (Feature Branching, Pull Requests)

## 📊 Dashboard & Visualization

_(Below is a snapshot of the Snowflake dashboard built on top of the Fact and Dimension tables)_

![Snowflake Dashboard](https://github.com/kityki/snowflake-airflow-data-pipeline/blob/main/images/snowflake_dashboard.png?raw=true)

## 🔐 Security & Best Practices Implemented

- **Airflow Connections:** Credentials are stored securely within Airflow's metadata database, keeping hardcoded passwords out of the repository.
- **Row-Level Security (RLS):** Implemented Snowflake roles (`BUSINESS_CLASS_MANAGER`) to restrict data access based on user privileges.
- **Git Hygiene:** utilized `.gitignore` to prevent massive Airflow logs and sensitive `.env` variables from being pushed to the public repository.
