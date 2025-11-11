from pathlib import Path

def validate_spark_home():
    """Basic environment sanity check."""
    import os
    return bool(os.environ.get("SPARK_HOME"))

def ensure_data_dir(base: str = "data") -> Path:
    p = Path(base)
    p.mkdir(parents=True, exist_ok=True)
    return p

if __name__ == "__main__":
    print("SPARK_HOME set?", validate_spark_home())
    print("Data dir:", ensure_data_dir().resolve())
