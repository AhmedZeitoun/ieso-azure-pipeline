# IESO Azure Pipeline

An ETL and data pipeline for parsing and processing Independent Electricity System Operator (IESO) power generation reports into Azure.

## Overview
- **Data Source**: IESO hourly generator output by fuel type (XML reports).
- **Processing**: Extracts hourly generation outputs across fuel types (Nuclear, Hydro, Gas, Wind, Solar, Biofuel).
- **Target Architecture**: Azure (Azure Blob Storage, Data Factory / Functions, Synapse / SQL).
