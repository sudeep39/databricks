# Databricks Kaggle Data Loader - Setup Guide

## Setup Instructions

### 1. Install Dependencies in Databricks Cluster
In a Databricks notebook cell, run:
```python
%pip install kaggle pyspark
```

### 2. Configure Kaggle Credentials in Databricks

#### Option A: Using Databricks Secrets (Recommended for Production)
```python
# In a Databricks notebook, create a secret scope
dbutils.secrets.create_scope("kaggle-scope")
dbutils.secrets.put("kaggle-scope", "api-key", "YOUR_KAGGLE_API_KEY")
dbutils.secrets.put("kaggle-scope", "username", "YOUR_KAGGLE_USERNAME")

# Then configure kaggle
import os
os.environ["KAGGLE_USERNAME"] = dbutils.secrets.get("kaggle-scope", "username")
os.environ["KAGGLE_KEY"] = dbutils.secrets.get("kaggle-scope", "api-key")
```

#### Option B: Upload kaggle.json to Databricks
1. Go to https://www.kaggle.com/account and create a new API token
2. Save the `kaggle.json` file
3. Upload it to Databricks DBFS or create it directly:
```python
dbutils.fs.put("dbfs:/tmp/kaggle.json", """
{
  "username": "YOUR_USERNAME",
  "key": "YOUR_API_KEY"
}
""", True)

import os
os.environ["KAGGLE_CONFIG_DIR"] = "/tmp"
```

### 3. Usage in Databricks Notebook

#### Import the module in a notebook cell:
```python
# Add the repo to path
import sys
sys.path.append("/Workspace/databricks")

from basics.file_read import main

# Download from Kaggle and load to Databricks Volumes
df = main(
    kaggle_dataset="aekundayo/health-insurance-data",
    csv_file="BenefitsCostSharing.csv",
    databricks_path="/Volumes/workspace/default/kaggle",
    download=True
)
```

#### Or use the loader class directly:
```python
from basics.file_read import DatabricksKaggleLoader

loader = DatabricksKaggleLoader(
    databricks_path="/Volumes/workspace/default/kaggle"
)

# Download and upload dataset
dataset_path = loader.download_and_upload_dataset(
    "aekundayo/health-insurance-data",
    file_name="BenefitsCostSharing.csv"
)

# Load and analyze the data
df = loader.load_csv_with_spark(
    f"{dataset_path}/BenefitsCostSharing.csv"
)
```

#### Load existing data from Volumes:
```python
from basics.file_read import main

df = main(
    csv_path="/Volumes/workspace/default/kaggle/health-insurance-data/BenefitsCostSharing.csv"
)
```

## Key Features for Databricks

- ✅ Loads data directly to Databricks Volumes or DBFS
- ✅ Works with Databricks notebook context
- ✅ Integrates with Databricks Secrets for secure credential management
- ✅ Automatic dataframe creation and analysis
- ✅ Handles large datasets with PySpark
- ✅ Comprehensive logging for debugging

## Databricks Paths

### Volumes (Recommended - Governed Storage)
```
/Volumes/workspace/default/kaggle/
```

### DBFS (Unmanaged Storage)
```
/dbfs/mnt/data/kaggle/
```

### Auto Loader (Streaming)
If using Auto Loader, register the path as a table:
```python
df = spark.read.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .load("/Volumes/workspace/default/kaggle/health-insurance-data/*.csv")
```

## Troubleshooting

**Issue: Kaggle authentication fails**
- Ensure kaggle.json is properly configured
- Check that username and API key are correct
- Use `dbutils.secrets.get()` if using secret scope

**Issue: Files not appearing in Volumes**
- Check that the Volumes path exists
- Verify cluster has write permissions
- Check cluster logs: `dbutils.fs.ls("/Volumes/workspace/default/kaggle")`

**Issue: PySpark session conflicts**
- Databricks notebooks manage SparkSession automatically
- Don't call `.stop()` on spark session in notebooks
- The refactored code handles this automatically
