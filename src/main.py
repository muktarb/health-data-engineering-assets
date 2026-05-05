"""
Health Data ETL Pipeline
========================
A modular ETL pipeline for processing population health surveillance data.
Demonstrates data ingestion, validation, transformation, and loading
for epidemiological analysis and data warehousing.

Author: Muktar Ahmed, PhD
"""

import pandas as pd
import numpy as np
import logging
import yaml
import os
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ═══════════════════════════════════════════════════════════════
# CONFIGURATION & LOGGING
# ═══════════════════════════════════════════════════════════════

def setup_logging(log_dir: str = "logs") -> logging.Logger:
    """Configure audit logging for pipeline runs."""
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"pipeline_run_{timestamp}.log")

    logger = logging.getLogger("health_etl")
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")
    )
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter("%(levelname)-8s | %(message)s")
    )

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


def load_config(config_path: str) -> dict:
    """Load pipeline configuration from YAML."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


# ═══════════════════════════════════════════════════════════════
# STAGE 1: INGESTION
# ═══════════════════════════════════════════════════════════════

class DataIngestor:
    """Ingest health data from multiple source formats."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def ingest(self, source_path: str, file_format: str = "auto") -> pd.DataFrame:
        """
        Read data from a source file with automatic format detection.

        Parameters
        ----------
        source_path : str
            Path to the source data file.
        file_format : str
            File format ('csv', 'excel', 'json', 'auto').

        Returns
        -------
        pd.DataFrame
            Raw ingested data.
        """
        if file_format == "auto":
            file_format = Path(source_path).suffix.lower().replace(".", "")

        readers = {
            "csv": pd.read_csv,
            "xlsx": pd.read_excel,
            "xls": pd.read_excel,
            "json": pd.read_json,
            "parquet": pd.read_parquet,
        }

        if file_format not in readers:
            raise ValueError(f"Unsupported format: {file_format}")

        df = readers[file_format](source_path)
        self.logger.info(
            f"INGESTED | {source_path} | {len(df):,} rows × {len(df.columns)} cols | format={file_format}"
        )
        return df

    def ingest_multiple(self, source_configs: List[dict]) -> Dict[str, pd.DataFrame]:
        """Ingest multiple data sources and return as a dictionary."""
        datasets = {}
        for config in source_configs:
            name = config["name"]
            path = config["path"]
            fmt = config.get("format", "auto")
            datasets[name] = self.ingest(path, fmt)
        return datasets


# ═══════════════════════════════════════════════════════════════
# STAGE 2: VALIDATION
# ═══════════════════════════════════════════════════════════════

class DataValidator:
    """Apply data quality checks to health datasets."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.report = []

    def validate(self, df: pd.DataFrame, rules: dict) -> Tuple[pd.DataFrame, dict]:
        """
        Run validation checks and return cleaned data + quality report.

        Parameters
        ----------
        df : pd.DataFrame
            Data to validate.
        rules : dict
            Validation rules from config.

        Returns
        -------
        Tuple of (cleaned DataFrame, quality report dict).
        """
        initial_rows = len(df)
        self.report = []

        # 1. Null checks
        if "required_columns" in rules:
            for col in rules["required_columns"]:
                if col in df.columns:
                    null_count = df[col].isna().sum()
                    null_pct = (null_count / len(df)) * 100
                    status = "PASS" if null_pct <= rules.get("null_threshold_pct", 5) else "FAIL"
                    self.report.append({
                        "check": "null_check",
                        "column": col,
                        "null_count": int(null_count),
                        "null_pct": round(null_pct, 2),
                        "status": status,
                    })
                    self.logger.info(
                        f"VALIDATE | null_check | {col} | {null_count:,} nulls ({null_pct:.1f}%) | {status}"
                    )

        # 2. Duplicate detection
        if "unique_keys" in rules:
            dupes = df.duplicated(subset=rules["unique_keys"], keep="first")
            dupe_count = dupes.sum()
            self.report.append({
                "check": "duplicate_check",
                "keys": rules["unique_keys"],
                "duplicate_count": int(dupe_count),
                "status": "PASS" if dupe_count == 0 else "WARN",
            })
            self.logger.info(
                f"VALIDATE | duplicate_check | keys={rules['unique_keys']} | {dupe_count:,} duplicates"
            )
            if rules.get("drop_duplicates", True):
                df = df[~dupes].copy()

        # 3. Range validation
        if "range_checks" in rules:
            for check in rules["range_checks"]:
                col = check["column"]
                if col in df.columns:
                    min_val = check.get("min")
                    max_val = check.get("max")
                    mask = pd.Series(True, index=df.index)
                    if min_val is not None:
                        mask &= df[col] >= min_val
                    if max_val is not None:
                        mask &= df[col] <= max_val
                    out_of_range = (~mask).sum()
                    self.report.append({
                        "check": "range_check",
                        "column": col,
                        "range": f"[{min_val}, {max_val}]",
                        "out_of_range_count": int(out_of_range),
                        "status": "PASS" if out_of_range == 0 else "WARN",
                    })
                    self.logger.info(
                        f"VALIDATE | range_check | {col} | range=[{min_val},{max_val}] | {out_of_range:,} violations"
                    )

        final_rows = len(df)
        self.logger.info(
            f"VALIDATE | COMPLETE | {initial_rows:,} → {final_rows:,} rows ({initial_rows - final_rows:,} removed)"
        )

        quality_report = {
            "initial_rows": initial_rows,
            "final_rows": final_rows,
            "rows_removed": initial_rows - final_rows,
            "checks": self.report,
            "overall_status": "PASS" if all(c["status"] == "PASS" for c in self.report) else "REVIEW",
        }
        return df, quality_report


# ═══════════════════════════════════════════════════════════════
# STAGE 3: TRANSFORMATION
# ═══════════════════════════════════════════════════════════════

class DataTransformer:
    """Standardise and derive health analytics variables."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def standardise_age_groups(self, df: pd.DataFrame, age_col: str = "age") -> pd.DataFrame:
        """Create standard 5-year age bands used in population health reporting."""
        bins = list(range(0, 100, 5)) + [120]
        labels = [f"{i}-{i+4}" for i in range(0, 95, 5)] + ["95+"]
        df["age_group"] = pd.cut(df[age_col], bins=bins, labels=labels, right=False)
        self.logger.info(f"TRANSFORM | age_group created from '{age_col}' | {len(labels)} bands")
        return df

    def standardise_sex(self, df: pd.DataFrame, sex_col: str = "sex") -> pd.DataFrame:
        """Standardise sex/gender coding to M/F/Other."""
        mapping = {
            "male": "M", "m": "M", "1": "M", "man": "M",
            "female": "F", "f": "F", "2": "F", "woman": "F",
            "other": "Other", "x": "Other", "3": "Other",
        }
        df[sex_col] = df[sex_col].astype(str).str.strip().str.lower().map(mapping).fillna("Unknown")
        self.logger.info(f"TRANSFORM | sex standardised | values: {df[sex_col].value_counts().to_dict()}")
        return df

    def calculate_crude_rate(
        self, df: pd.DataFrame, count_col: str, pop_col: str, per: int = 100_000
    ) -> pd.DataFrame:
        """Calculate crude rate per population (e.g. per 100,000)."""
        rate_col = f"{count_col}_rate_per_{per // 1000}k"
        df[rate_col] = (df[count_col] / df[pop_col]) * per
        df[rate_col] = df[rate_col].round(2)
        self.logger.info(f"TRANSFORM | crude rate calculated | {rate_col}")
        return df

    def calculate_proportion(
        self, df: pd.DataFrame, numerator_col: str, denominator_col: str
    ) -> pd.DataFrame:
        """Calculate proportion as a percentage."""
        prop_col = f"{numerator_col}_pct"
        df[prop_col] = ((df[numerator_col] / df[denominator_col]) * 100).round(2)
        self.logger.info(f"TRANSFORM | proportion calculated | {prop_col}")
        return df

    def standardise_geography(
        self, df: pd.DataFrame, geo_col: str, mapping: Optional[dict] = None
    ) -> pd.DataFrame:
        """Standardise geographic region names."""
        if mapping:
            df[geo_col] = df[geo_col].map(mapping).fillna(df[geo_col])
            self.logger.info(f"TRANSFORM | geography standardised | {geo_col}")
        return df

    def add_metadata_columns(self, df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        """Add pipeline metadata columns for audit trail."""
        df["_source"] = source_name
        df["_ingested_at"] = datetime.now().isoformat()
        df["_pipeline_version"] = "1.0.0"
        self.logger.info(f"TRANSFORM | metadata columns added | source={source_name}")
        return df


# ═══════════════════════════════════════════════════════════════
# STAGE 4: LOADING
# ═══════════════════════════════════════════════════════════════

class DataLoader:
    """Load processed data to target destinations."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def to_parquet(self, df: pd.DataFrame, output_path: str) -> None:
        """Save to Parquet format (optimised for data warehouse ingestion)."""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_parquet(output_path, index=False, engine="pyarrow")
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        self.logger.info(f"LOAD | parquet | {output_path} | {len(df):,} rows | {size_mb:.2f} MB")

    def to_csv(self, df: pd.DataFrame, output_path: str) -> None:
        """Save to CSV format."""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        self.logger.info(f"LOAD | csv | {output_path} | {len(df):,} rows")

    def to_sql(self, df: pd.DataFrame, table_name: str, connection_string: str) -> None:
        """Load to SQL database table."""
        from sqlalchemy import create_engine
        engine = create_engine(connection_string)
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        self.logger.info(f"LOAD | sql | table={table_name} | {len(df):,} rows")

    def save_quality_report(self, report: dict, output_path: str) -> None:
        """Save data quality report as JSON."""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        self.logger.info(f"LOAD | quality_report | {output_path}")

    def generate_data_dictionary(self, df: pd.DataFrame, output_path: str) -> None:
        """Auto-generate a data dictionary from the processed dataset."""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        dictionary = []
        for col in df.columns:
            entry = {
                "column_name": col,
                "data_type": str(df[col].dtype),
                "non_null_count": int(df[col].notna().sum()),
                "null_count": int(df[col].isna().sum()),
                "unique_values": int(df[col].nunique()),
                "sample_values": df[col].dropna().head(3).tolist(),
            }
            if pd.api.types.is_numeric_dtype(df[col]):
                entry["min"] = float(df[col].min()) if df[col].notna().any() else None
                entry["max"] = float(df[col].max()) if df[col].notna().any() else None
                entry["mean"] = round(float(df[col].mean()), 2) if df[col].notna().any() else None
            dictionary.append(entry)

        with open(output_path, "w") as f:
            json.dump(dictionary, f, indent=2, default=str)
        self.logger.info(f"LOAD | data_dictionary | {output_path} | {len(dictionary)} columns documented")


# ═══════════════════════════════════════════════════════════════
# PIPELINE ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════

class HealthDataPipeline:
    """
    Orchestrate the full ETL pipeline.

    Usage
    -----
    >>> pipeline = HealthDataPipeline(config_path="config/pipeline_config.yaml")
    >>> pipeline.run()
    """

    def __init__(self, config_path: str):
        self.config = load_config(config_path)
        self.logger = setup_logging(self.config.get("log_dir", "logs"))
        self.ingestor = DataIngestor(self.logger)
        self.validator = DataValidator(self.logger)
        self.transformer = DataTransformer(self.logger)
        self.loader = DataLoader(self.logger)

    def run(self) -> None:
        """Execute the full ETL pipeline."""
        self.logger.info("=" * 60)
        self.logger.info("PIPELINE START")
        self.logger.info("=" * 60)

        start_time = datetime.now()

        try:
            # ── Stage 1: Ingest ──
            self.logger.info("── STAGE 1: INGESTION ──")
            datasets = self.ingestor.ingest_multiple(self.config["sources"])

            for name, df in datasets.items():
                # ── Stage 2: Validate ──
                self.logger.info(f"── STAGE 2: VALIDATION [{name}] ──")
                df, quality_report = self.validator.validate(
                    df, self.config.get("validation", {})
                )

                # ── Stage 3: Transform ──
                self.logger.info(f"── STAGE 3: TRANSFORMATION [{name}] ──")
                transform_config = self.config.get("transformations", {})

                if "age_column" in transform_config:
                    df = self.transformer.standardise_age_groups(df, transform_config["age_column"])
                if "sex_column" in transform_config:
                    df = self.transformer.standardise_sex(df, transform_config["sex_column"])
                if "rate_calculation" in transform_config:
                    rc = transform_config["rate_calculation"]
                    df = self.transformer.calculate_crude_rate(df, rc["count"], rc["population"])

                df = self.transformer.add_metadata_columns(df, name)

                # ── Stage 4: Load ──
                self.logger.info(f"── STAGE 4: LOADING [{name}] ──")
                output_dir = self.config.get("output_dir", "data/processed")

                self.loader.to_csv(df, os.path.join(output_dir, f"{name}_processed.csv"))
                self.loader.save_quality_report(
                    quality_report, os.path.join(output_dir, f"{name}_quality_report.json")
                )
                self.loader.generate_data_dictionary(
                    df, os.path.join(output_dir, f"{name}_data_dictionary.json")
                )

            elapsed = (datetime.now() - start_time).total_seconds()
            self.logger.info("=" * 60)
            self.logger.info(f"PIPELINE COMPLETE | {elapsed:.1f}s elapsed")
            self.logger.info("=" * 60)

        except Exception as e:
            self.logger.error(f"PIPELINE FAILED | {type(e).__name__}: {e}")
            raise


# ═══════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Health Data ETL Pipeline")
    parser.add_argument(
        "--config", default="config/pipeline_config.yaml", help="Path to pipeline config"
    )
    args = parser.parse_args()

    pipeline = HealthDataPipeline(args.config)
    pipeline.run()
