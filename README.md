# IESO Power Generation Integration Pipeline (Azure & Delta Lake)

[![Cloud](https://img.shields.io/badge/Cloud-Microsoft%20Azure-blue?logo=microsoftazure)](https://azure.microsoft.com)
[![IaC](https://img.shields.io/badge/IaC-Terraform-623CE4?logo=terraform)](https://www.terraform.io)
[![Data Factory](https://img.shields.io/badge/ETL-Azure%20Data%20Factory-0078D4?logo=azuredatafactory)](https://azure.microsoft.com/services/data-factory/)
[![Storage](https://img.shields.io/badge/Lakehouse-Delta%20Lake%20%7C%20ADLS%20Gen2-00A4EF)](https://delta.io)
[![Target Domain](https://img.shields.io/badge/Sector-CANDU%20Nuclear%20%7C%20AtkinsR%C3%A9alis-004B87)](https://www.atkinsrealis.com)

An enterprise-grade, event-driven data lakehouse pipeline ingesting public hourly power generation reports from Ontario's Independent Electricity System Operator (IESO). Built for the **Data & Integration Developer** context at **AtkinsRéalis (CANDU nuclear engineering)** to monitor nuclear baseload output vs Ontario grid mix.

---

## 🏛️ End-to-End Pipeline Architecture

```text
[IESO Public XML Endpoint]
        │
        ▼ (Scheduled Recurrence: Every 6 hrs)
[Azure Logic App (Consumption)]
        │ (Streams raw unaltered XML payload)
        ▼
[ADLS Gen2: bronze/ieso_raw/*.xml]
        │
        ▼ (Pipeline Trigger / Event)
[Azure Data Factory (ADF Pipeline)]
        │
        ├── Activity 1: GetMetadata (Validates file presence & metadata)
        │
        └── Activity 2: Mapping Data Flow (Managed Spark Runtime)
                ├── Source: ADLS Gen2 XML Dataset (bronze/ieso_raw/)
                ├── Flatten Transform (foldDown): Unrolls nested hierarchy
                │     DocBody.DailyData -> HourlyData -> FuelTotal
                ├── Derived Column:
                │     OutputMW  = toDecimal(Output, 10, 2)
                │     Hour      = toInteger(Hour)
                │     FuelType  = upper(Fuel)
                │     Timestamp = toTimestamp(concat(Date, ' ', lpad(Hour, 2, '0'), ':00:00'))
                ├── Assert Transform:
                │     assert(OutputMW >= 0, 'invalid_mw', 'Output MW must be non-negative')
                └── Sink: Inline Delta format -> ADLS Gen2 (silver/ieso_generation/)
                            (Snappy Parquet + _delta_log/ commits, partitioned by FuelType)
        │
        ▼
[Serving & Analytics Layer]
  - Queryable via DuckDB (local/in-process OLAP)
  - Microsoft Fabric / Power BI Direct Lake
```

---

## ☁️ Azure Cloud Resources & Architecture Breakdown

| Resource | Service / SKU | Purpose | Security & Cost Model |
| :--- | :--- | :--- | :--- |
| **Resource Group** | `rg-ieso-integration-dev` | Logical containment in `canadacentral` | Zero cost |
| **Data Lake Storage** | `ADLS Gen2 (StorageV2)` | Bronze (raw XML) & Silver (Delta Lake) | Hierarchical Namespace (HNS) enabled; Pay-per-GB (~$0.02/GB/mo) |
| **Azure Data Factory** | ADF v2 (`adf-ieso-integration-dev`) | Orchestration & Managed Spark Data Flows | System-Assigned Managed Identity (zero stored credentials) |
| **RBAC Assignment** | `Storage Blob Data Contributor` | Grants ADF engine direct access to storage | Keyless Azure Entra ID RBAC |
| **Azure Logic App** | Consumption Tier | Serverless scheduled fetch from IESO endpoint | Pay-per-trigger (~$0.000025/run); $0 when idle |

---

## 📂 Repository Structure

```text
ieso-azure-pipeline/
├── infra/                          # Terraform Infrastructure as Code (Phase 2)
│   ├── main.tf                     # RG, ADLS Gen2, ADF with MI, RBAC, Linked Service, Skeleton
│   ├── providers.tf                # azurerm (~> 3.100) & random providers
│   ├── variables.tf                # Regional & naming variables
│   ├── outputs.tf                  # Endpoints & IDs for verification
│   └── terraform.tfvars.example    # Configuration values
├── scripts/                        # Consumption & Verification (Phase 5)
│   └── query_delta_duckdb.py       # Local DuckDB script to query silver Delta Lake
├── run.ipynb                       # Local exploratory prototyping notebook
└── README.md
```

---

## ⚡ Step-by-Step Execution Guide

### 1. Local Schema Inspection
The raw IESO feed `PUB_GenOutputbyFuelHourly.xml` features a deep multi-level XML structure:
```xml
<Document>
  <DocBody>
    <DailyData>
      <Day>2026-01-01</Day>
      <HourlyData>
        <Hour>1</Hour>
        <FuelTotal>
          <Fuel>NUCLEAR</Fuel>
          <EnergyValue>
            <OutputQuality>0</OutputQuality>
            <Output>9602</Output>
          </EnergyValue>
        </FuelTotal>
      ...
```
ADF Data Flow unrolls this hierarchy to tabular format:
`Date | Hour | FuelType | OutputMW | Quality | Timestamp`.

### 2. Infrastructure as Code (Terraform)
*Note: Run only when ready to deploy.*
```bash
cd infra
terraform init
terraform plan
# terraform apply
```

### 3. Logic App Ingestion Setup
Import `logic_app/workflow.json` into an Azure Logic App (Consumption) resource:
- **Trigger**: Recurrence (Every 6 hours).
- **Action**: HTTP GET `https://reports-public.ieso.ca/public/GenOutputbyFuelHourly/PUB_GenOutputbyFuelHourly.xml`.
- **Sink**: ADLS Gen2 `/bronze/ieso_raw/ieso_hourly_@{utcNow('yyyyMMdd_HHmmss')}.xml`.

### 4. ADF Studio Deployment & Delta Lake Sink
1. Connect ADF to this Git repository or import JSON files under `adf/`.
2. Run Debug mode on `pl_ieso_bronze_to_silver`.
3. Confirm that the Silver layer contains:
   - `silver/ieso_generation/_delta_log/00000000000000000000.json` (ACID transaction logs)
   - `silver/ieso_generation/FuelType=NUCLEAR/*.snappy.parquet`
   - `silver/ieso_generation/FuelType=HYDRO/*.snappy.parquet`
   - `silver/ieso_generation/FuelType=GAS/*.snappy.parquet`
   - `silver/ieso_generation/FuelType=WIND/*.snappy.parquet`

### 5. Local OLAP Validation with DuckDB
Query the curated Delta Lake partition directly:
```bash
python scripts/query_delta_duckdb.py ./silver/ieso_generation
```
