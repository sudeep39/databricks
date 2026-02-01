#!/usr/bin/env python3
import sys
import subprocess

try:
    import distutils  # type: ignore
except ImportError:
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "setuptools"])
    except subprocess.CalledProcessError:
        pass
    try:
        import distutils  # type: ignore
    except ImportError as e:
        sys.stderr.write(f"Failed to install/import distutils: {e}\n")


def main(csv_path):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("csv_read").getOrCreate()
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
    df.select(df.columns[:5]).show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    csv_path = (
        "/Volumes/workspace/default/kaggle/health-insurance/BenefitsCostSharing.csv"
    )
    main(csv_path)
