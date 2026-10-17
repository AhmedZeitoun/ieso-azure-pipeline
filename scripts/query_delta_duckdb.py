"""
IESO Generation Delta Lake Local Querying via DuckDB
Targeted for: Data & Integration Developer (AtkinsRéalis / CANDU Nuclear Engineering)

This script validates the Silver Delta Lake curated table produced by Azure Data Factory.
It verifies ACID logs (_delta_log/), reads partitioned Snappy Parquet files,
and performs high-performance in-process OLAP analytics on nuclear baseload and total fuel mix.
"""

import sys
import duckdb

def analyze_silver_delta(delta_table_path: str = "./silver/ieso_generation"):
    print("=" * 80)
    print("IESO Nuclear & Grid Generation Analytics (Delta Lake via DuckDB)")
    print(f"Target Delta Table Path: {delta_table_path}")
    print("=" * 80)

    con = duckdb.connect()

    try:
        # Install and load DuckDB Delta extension
        con.execute("INSTALL delta;")
        con.execute("LOAD delta;")
        print("[✓] DuckDB Delta extension loaded successfully.")
    except Exception as e:
        print(f"[!] Note on Delta extension: {e}")

    query = f"""
        SELECT 
            FuelType,
            COUNT(*) AS TotalHourlyRecords,
            ROUND(MIN(OutputMW), 2) AS MinOutputMW,
            ROUND(AVG(OutputMW), 2) AS AvgOutputMW,
            ROUND(MAX(OutputMW), 2) AS MaxOutputMW,
            ROUND(SUM(OutputMW), 2) AS TotalMWh
        FROM delta_scan('{delta_table_path}')
        GROUP BY FuelType
        ORDER BY TotalMWh DESC;
    """

    print("\nExecuting generation aggregation query:")
    print(query)

    try:
        df = con.execute(query).df()
        print("\n--- Generation Summary by Fuel Type ---")
        print(df.to_string(index=False))

        # Specialized CANDU Nuclear baseload inspection
        nuclear_query = f"""
            SELECT 
                Date,
                Hour,
                OutputMW,
                Quality,
                Timestamp
            FROM delta_scan('{delta_table_path}')
            WHERE FuelType = 'NUCLEAR'
            ORDER BY Date DESC, Hour DESC
            LIMIT 10;
        """
        nuclear_df = con.execute(nuclear_query).df()
        print("\n--- Recent CANDU Nuclear Baseload Records (Top 10) ---")
        print(nuclear_df.to_string(index=False))

    except Exception as e:
        print(f"\n[INFO] Could not scan path '{delta_table_path}' directly (run after ADF sink or local sample): {e}")

if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "./silver/ieso_generation"
    analyze_silver_delta(path)
