#!/usr/bin/env python3
"""
Databricks-ready data loading module with Kaggle integration.

This module handles downloading datasets from Kaggle and loading them to Databricks Volumes or DBFS.
"""

import logging
import os
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class DatabricksKaggleLoader:
    """Handles downloading Kaggle datasets and loading them to Databricks."""

    def __init__(
        self,
        databricks_path: str = "/Volumes/workspace/default/kaggle",
        kaggle_credentials_path: Optional[str] = None,
    ):
        """
        Initialize the Databricks Kaggle data loader.

        Args:
            databricks_path: Path in Databricks Volumes or DBFS where files will be stored
                           (e.g., "/Volumes/workspace/default/kaggle" or "/dbfs/mnt/data/kaggle")
            kaggle_credentials_path: Path to kaggle.json credentials file (optional)
        """
        self.databricks_path = databricks_path
        self.spark = SparkSession.builder.appName("kaggle_loader").getOrCreate()

        # Set up Kaggle credentials if provided
        if kaggle_credentials_path:
            os.environ["KAGGLE_CONFIG_DIR"] = str(Path(kaggle_credentials_path).parent)

    def download_and_upload_dataset(
        self, dataset_name: str, file_name: Optional[str] = None
    ) -> str:
        """
        Download a dataset from Kaggle and upload to Databricks.

        Args:
            dataset_name: Kaggle dataset identifier (e.g., "owner/dataset-name")
            file_name: Specific file to download (optional, downloads all if not specified)

        Returns:
            Path to the uploaded file(s) in Databricks
        """
        try:
            from kaggle.api.kaggle_api_extended import KaggleApi
        except ImportError:
            logger.error(
                "kaggle package not installed. Install it with: pip install kaggle"
            )
            raise

        # Create temporary directory for download
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                api = KaggleApi()
                api.authenticate()
                logger.info("Successfully authenticated with Kaggle")

                # Download to temporary directory
                temp_path = Path(temp_dir)
                dataset_short_name = dataset_name.split("/")[-1]

                if file_name:
                    logger.info(f"Downloading {file_name} from {dataset_name}...")
                    api.dataset_download_file(dataset_name, file_name, path=temp_path)
                else:
                    logger.info(f"Downloading dataset {dataset_name}...")
                    api.dataset_download_files(dataset_name, path=temp_path)

                # Extract zip files
                logger.info("Extracting downloaded files...")
                for zip_file in temp_path.glob("*.zip"):
                    logger.info(f"Extracting {zip_file.name}...")
                    with zipfile.ZipFile(zip_file, "r") as zip_ref:
                        zip_ref.extractall(temp_path)
                    zip_file.unlink()  # Remove the zip file after extraction
                logger.info("Extraction completed")

                # Upload to Databricks
                databricks_dataset_path = f"{self.databricks_path}/{dataset_short_name}"
                logger.info(f"Uploading to Databricks at {databricks_dataset_path}...")

                # Use dbutils to copy files to Databricks
                try:
                    dbutils = globals().get("dbutils")
                    if dbutils:
                        # Running in Databricks notebook
                        for local_file in temp_path.rglob("*"):
                            if local_file.is_file():
                                relative_path = local_file.relative_to(temp_path)
                                dbutils_path = f"{databricks_dataset_path}/{relative_path}".replace(
                                    "\\", "/"
                                )
                                dbutils.fs.put(
                                    dbutils_path, local_file.read_text(), True
                                )
                        logger.info(
                            f"Files uploaded successfully to {databricks_dataset_path}"
                        )
                    else:
                        raise RuntimeError("Not running in Databricks notebook context")
                except Exception as notebook_err:
                    logger.warning(
                        f"Notebook upload failed: {notebook_err}. Attempting DBFS copy..."
                    )
                    # Fallback: copy to DBFS mount
                    dbfs_path = databricks_dataset_path.replace(
                        "/Volumes/", "/dbfs/Volumes/"
                    )
                    os.makedirs(dbfs_path, exist_ok=True)
                    for local_file in temp_path.rglob("*"):
                        if local_file.is_file():
                            relative_path = local_file.relative_to(temp_path)
                            dest_file = os.path.join(dbfs_path, str(relative_path))
                            os.makedirs(os.path.dirname(dest_file), exist_ok=True)
                            shutil.copy2(local_file, dest_file)
                    logger.info(f"Files copied to {dbfs_path}")

                return databricks_dataset_path

            except Exception as e:
                logger.error(f"Failed to download/upload dataset from Kaggle: {e}")
                raise

    def load_csv_with_spark(
        self,
        csv_path: str,
        num_rows: int = 5,
        truncate: bool = False,
    ) -> None:
        """
        Load and display a CSV file from Databricks using PySpark.

        Args:
            csv_path: Path to the CSV file (can be Volumes or DBFS path)
            num_rows: Number of rows to display
            truncate: Whether to truncate output columns
        """
        try:
            logger.info(f"Reading CSV from {csv_path}...")
            df = (
                self.spark.read.option("header", "true")
                .option("inferSchema", "true")
                .csv(csv_path)
            )

            logger.info("Dataframe schema:")
            df.printSchema()

            logger.info(f"Displaying first {num_rows} rows:")
            df.select(df.columns[:5]).show(num_rows, truncate=truncate)

            total_rows = df.count()
            logger.info(f"Total rows: {total_rows}")

            return df

        except Exception as e:
            logger.error(f"Failed to load or process CSV: {e}")
            raise


def main(
    kaggle_dataset: Optional[str] = None,
    csv_file: Optional[str] = None,
    csv_path: Optional[str] = None,
    databricks_path: str = "/Volumes/workspace/default/kaggle",
    download: bool = False,
):
    """
    Main function to download from Kaggle and load to Databricks.

    Args:
        kaggle_dataset: Kaggle dataset identifier (e.g., "owner/dataset-name")
        csv_file: Specific file to download from the dataset
        csv_path: Direct path to CSV file in Databricks (if not downloading)
        databricks_path: Base path in Databricks Volumes/DBFS for storing data
        download: Whether to download from Kaggle
    """
    loader = DatabricksKaggleLoader(databricks_path=databricks_path)

    # Download from Kaggle and upload to Databricks if requested
    if download and kaggle_dataset:
        logger.info(
            f"Starting Kaggle download and Databricks upload for: {kaggle_dataset}"
        )
        dataset_path = loader.download_and_upload_dataset(
            kaggle_dataset, file_name=csv_file
        )

        # Construct path to uploaded file
        dataset_name = kaggle_dataset.split("/")[-1]
        if csv_file:
            csv_path = f"{dataset_path}/{csv_file}"
        else:
            # Use the dataset directory and let Spark read all CSVs
            csv_path = dataset_path

    # Load and process the CSV
    if csv_path:
        df = loader.load_csv_with_spark(csv_path)
        return df
    else:
        logger.error(
            "No CSV path provided. Use csv_path or set download=True with kaggle_dataset"
        )
        return None


if __name__ == "__main__":
    # Example usage in Databricks notebook
    # Uncomment and modify as needed:

    # Option 1: Download from Kaggle and upload to Databricks
    # main(
    #     kaggle_dataset="aekundayo/health-insurance-data",
    #     csv_file="BenefitsCostSharing.csv",
    #     databricks_path="/Volumes/workspace/default/kaggle",
    #     download=True
    # )

    # Option 2: Use existing file in Databricks Volumes
    # main(
    #     csv_path="/Volumes/workspace/default/kaggle/health-insurance-data/BenefitsCostSharing.csv"
    # )

    logger.info("Run this script from Databricks notebook context")
