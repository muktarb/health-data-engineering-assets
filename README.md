# Health Data ETL Pipeline

A modular, production-ready ETL (Extract, Transform, Load) pipeline for processing population health surveillance data using Python and SQL. Designed for epidemiological analysis, data warehousing, and integration into BI reporting workflows.

---
Author: Muktar Ahmed, PhD | Postdoctoral Research Fellow, Flinders University  
Contact: Muktar.Ahmed@flinders.edu.au
---

## Overview

This project demonstrates a complete data engineering workflow for health data:

```
Raw Data Sources → Ingestion → Validation → Transformation → Data Warehouse → Reporting
```

The pipeline processes population-level health indicator data (e.g. chronic disease prevalence, mortality rates, demographic breakdowns) from multiple source formats, applies quality checks and standardisation, and outputs analysis-ready datasets suitable for loading into a data warehouse or BI platform.

## Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌──────────────────┐     ┌────────────────┐
│  Data Sources   │────▶│  Ingestion   │────▶│  Transformation  │────▶│  Data Output   │
│                 │     │              │     │                  │     │                │
│ • CSV/Excel     │     │ • Schema     │     │ • Standardise    │     │ • Parquet      │
│ • API endpoints │     │   detection  │     │ • Derive metrics │     │ • SQL Database │
│ • JSON feeds    │     │ • Type       │     │ • Aggregate      │     │ • CSV export   │
│                 │     │   coercion   │     │ • Join/link      │     │ • Audit logs   │
└─────────────────┘     │ • Logging    │     │ • Validate       │     └────────────────┘
                        └──────────────┘     └──────────────────┘
```

## Key Features

- **Multi-format ingestion** — reads CSV, Excel, JSON, and API responses with automatic schema detection
- **Data quality framework** — null checks, range validation, duplicate detection, referential integrity, with configurable thresholds
- **Standardised transformations** — age group banding, ICD code mapping, geographic standardisation, rate calculations
- **Metadata management** — automatic capture of column lineage, transformation history, and data dictionary generation
- **Audit logging** — every pipeline run produces a timestamped log with row counts, validation results, and error summaries
- **Modular design** — each pipeline stage is independently testable and configurable via YAML

## Technical Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.10+ |
| Data processing | pandas, NumPy |
| Database | SQLite (demo) / SQL Server / PostgreSQL |
| Configuration | YAML |
| Testing | pytest |
| Output formats | Parquet, CSV, SQL |

## Project Structure

```
health-data-etl-pipeline/
├── README.md
├── requirements.txt
├── config/
│   └── pipeline_config.yaml
├── src/
│   ├── __init__.py
│   ├── ingest.py            # Data ingestion from multiple sources
│   ├── validate.py          # Data quality checks and validation
│   ├── transform.py         # Standardisation and derivation
│   ├── load.py              # Output to warehouse/files
│   ├── metadata.py          # Metadata capture and data dictionary
│   └── utils.py             # Logging, config parsing, helpers
├── tests/
│   ├── test_ingest.py
│   ├── test_validate.py
│   └── test_transform.py
├── data/
│   ├── raw/                 # Sample input data (synthetic)
│   └── processed/           # Pipeline output
├── logs/                    # Audit logs
└── docs/
    └── data_dictionary.md
```

## Quick Start

```bash
# Clone the repository
git clone https://github.com/muktarb/health-data-etl-pipeline.git
cd health-data-etl-pipeline

# Install dependencies
pip install -r requirements.txt

# Run the pipeline
python -m src.main --config config/pipeline_config.yaml

# Run tests
pytest tests/ -v
```

## Sample Output

The pipeline produces:
1. **Cleaned dataset** — standardised, validated, analysis-ready health data in Parquet/CSV
2. **Data quality report** — summary of validation checks, pass/fail rates, flagged records
3. **Data dictionary** — auto-generated metadata for all output columns
4. **Audit log** — timestamped record of pipeline execution

## Use Cases

- Processing population health survey data for epidemiological analysis
- Building staging layers for a health data warehouse
- Automating data quality checks on incoming health datasets
- Preparing data feeds for Power BI / Tableau dashboards

## Author

**Muktar Ahmed, PhD**
Research Fellow in Data Science | Flinders University
[LinkedIn](https://linkedin.com/in/muktar-beshir-ahmed-96935b51) | [Google Scholar](https://scholar.google.com/citations?user=AlbIWxwAAAAJ)

## License

MIT License
