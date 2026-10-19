"""
IESO Generation Delta Lake Querying via DuckDB
Targeted for: Data & Integration Developer (AtkinsRéalis / CANDU Nuclear Engineering)

This script validates the Silver Delta Lake curated table produced by Azure Data Factory.
It verifies ACID logs (_delta_log/), reads partitioned Snappy Parquet files,
and performs high-performance in-process OLAP analytics directly on Azure Data Lake Storage Gen2.
"""

import os
import sys
from pathlib import Path
import duckdb

def load_env():
    """Load key-value pairs from .env if present without external dependencies."""
    env_file = Path(__file__).resolve().parent.parent / ".env"
    if env_file.exists():
        with open(env_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ.setdefault(key.strip(), val.strip())

load_env()

STORAGE_ACCOUNT = os.environ.get("AZURE_STORAGE_ACCOUNT", "adlsiesodeve3qhu9")
CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER", "silver")
TABLE_PATH = os.environ.get("AZURE_DELTA_TABLE_PATH", "ieso_generation")
STORAGE_KEY = os.environ.get("AZURE_STORAGE_KEY", "")

def run_analytics():
    print("=" * 80)
    print("IESO Nuclear & Ontario Grid Generation Analytics (Delta Lake via DuckDB)")
    print("=" * 80)

    if not STORAGE_KEY:
        print("[ERROR] AZURE_STORAGE_KEY is not set.")
        print("Please configure AZURE_STORAGE_KEY in your environment or in a local .env file.")
        print("See .env.example for guidance.")
        sys.exit(1)

    con = duckdb.connect()

    # Load Azure & Delta extensions
    con.execute("INSTALL azure; LOAD azure;")
    con.execute("INSTALL delta; LOAD delta;")
    print("[OK] DuckDB Azure & Delta extensions loaded successfully.")

    # Configure secure connection to ADLS Gen2
    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={STORAGE_ACCOUNT};"
        f"AccountKey={STORAGE_KEY};"
        f"EndpointSuffix=core.windows.net"
    )
    con.execute(f"""
        CREATE OR REPLACE SECRET adls_secret (
            TYPE AZURE,
            CONNECTION_STRING '{conn_str}'
        );
    """)
    print(f"[OK] Connected to Azure Data Lake Storage Gen2 ({STORAGE_ACCOUNT}).")

    remote_delta_path = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{TABLE_PATH}"
    print(f"[OK] Scanning Delta Lake Table: {remote_delta_path}\n")

    # 1. Generation summary by fuel type across all historical data
    summary_query = f"""
        SELECT 
            FuelType,
            COUNT(*) AS TotalHourlyRecords,
            ROUND(MIN(OutputMW), 1) AS MinOutputMW,
            ROUND(AVG(OutputMW), 1) AS AvgOutputMW,
            ROUND(MAX(OutputMW), 1) AS MaxOutputMW,
            ROUND(SUM(OutputMW), 1) AS TotalGenerationMWh
        FROM delta_scan('{remote_delta_path}')
        GROUP BY FuelType
        ORDER BY TotalGenerationMWh DESC;
    """

    print("--- 1. Ontario Electricity Generation Summary by Fuel Type ---")
    df_summary = con.execute(summary_query).df()
    print(df_summary.to_string(index=False))

    # 2. Specialized CANDU Nuclear baseload stability inspection
    nuclear_query = f"""
        SELECT 
            Date,
            Hour,
            OutputMW AS Nuclear_MW,
            Quality,
            Timestamp
        FROM delta_scan('{remote_delta_path}')
        WHERE FuelType = 'NUCLEAR'
        ORDER BY Date DESC, Hour DESC
        LIMIT 10;
    """
    print("\n--- 2. CANDU Nuclear Baseload Generation Records (Top 10 Most Recent) ---")
    df_nuclear = con.execute(nuclear_query).df()
    print(df_nuclear.to_string(index=False))

    # 3. Overall grid mix percentage (Nuclear share of clean electricity)
    share_query = f"""
        WITH Totals AS (
            SELECT 
                FuelType,
                SUM(OutputMW) AS TotalMWh
            FROM delta_scan('{remote_delta_path}')
            GROUP BY FuelType
        ),
        Overall AS (
            SELECT SUM(TotalMWh) AS AllGeneration FROM Totals
        )
        SELECT 
            t.FuelType,
            ROUND(t.TotalMWh, 1) AS TotalMWh,
            ROUND((t.TotalMWh * 100.0 / o.AllGeneration), 2) AS PercentageOfGrid
        FROM Totals t, Overall o
        ORDER BY PercentageOfGrid DESC;
    """
    print("\n--- 3. Ontario Total Grid Fuel Mix Breakdown (%) ---")
    df_share = con.execute(share_query).df()
    print(df_share.to_string(index=False))

if __name__ == "__main__":
    run_analytics()
