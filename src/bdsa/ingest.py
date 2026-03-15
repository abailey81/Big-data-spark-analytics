"""Data ingestion utilities for the PySpark analytics pipeline.

Provides helper functions for validating the Spark environment
and managing data directories across all analytical modules.
"""

from pathlib import Path


def validate_spark_home() -> bool:
    """Check whether the SPARK_HOME environment variable is set."""
    import os
    return bool(os.environ.get("SPARK_HOME"))


def ensure_data_dir(base: str = "data") -> Path:
    """Create and return the data directory path, creating it if needed."""
    p = Path(base)
    p.mkdir(parents=True, exist_ok=True)
    return p


if __name__ == "__main__":
    print("SPARK_HOME set?", validate_spark_home())
    print("Data dir:", ensure_data_dir().resolve())
