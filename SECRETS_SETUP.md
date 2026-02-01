# Databricks Secret Setup Script

This script automates the creation of Databricks secret scopes and adds your Kaggle credentials securely.

## Prerequisites

1. **Databricks OAuth configured** in VS Code or CLI:
   ```bash
   databricks configure --auth-type oauth
   ```

2. **Install the Databricks SDK**:
   ```bash
   pip install -r requirements.txt
   # or
   pip install databricks-sdk
   ```

## Quick Start

### Step 1: Automatic (Recommended)
If you have a `kaggle.json` file in your **Downloads** folder:

```bash
python setup_databricks_secrets.py
```

The script will:
1. Look for `~/Downloads/kaggle.json`
2. Ask for confirmation
3. Create the `kaggle-scope` secret scope
4. Add your credentials securely

### Step 2: Manual Input
```bash
python setup_databricks_secrets.py
```

When prompted:
- Enter your Kaggle username
- Enter your Kaggle API key (from https://www.kaggle.com/settings/account)

### Step 3: Manual CLI Setup

```bash
# Create scope
databricks secrets create-scope --scope kaggle-scope

# Add your username
databricks secrets put --scope kaggle-scope --key username --string-value "your_username"

# Add your API key
databricks secrets put --scope kaggle-scope --key api-key --string-value "your_api_key"
```

## Kaggle Credentials Format

Your `kaggle.json` file should contain:
```json
{
  "username": "your_kaggle_username",
  "key": "your_api_key"
}
```

## Usage in Notebooks

Once secrets are created, use them in any Databricks notebook:

```python
import os

# Retrieve secrets
username = dbutils.secrets.get("kaggle-scope", "username")
api_key = dbutils.secrets.get("kaggle-scope", "api-key")

# Set environment variables
os.environ["KAGGLE_USERNAME"] = username
os.environ["KAGGLE_KEY"] = api_key

# Now you can use Kaggle API
from basics.file_read import DatabricksKaggleLoader

loader = DatabricksKaggleLoader()
dataset_path = loader.download_and_upload_dataset(
    "aekundayo/health-insurance-data",
    "BenefitsCostSharing.csv"
)
```

## Troubleshooting

**"Failed to connect to Databricks"**
- Ensure OAuth is configured: `databricks configure --auth-type oauth`
- Check your internet connection
- Verify workspace URL is correct

**"Permission denied"**
- You need admin access to create secret scopes
- Contact your Databricks workspace admin

**"Scope already exists"**
- The script will use the existing scope
- If you need to update secrets, use the CLI:
  ```bash
  databricks secrets put --scope kaggle-scope --key api-key --string-value "NEW_KEY"
  ```

**"kaggle.json not found"**
- Download your API token from https://www.kaggle.com/settings/account
- Save it to `~/.kaggle/kaggle.json`
- Or run the script and enter credentials manually

## File Locations

- **Script**: `setup_databricks_secrets.py`
- **Kaggle credentials**: `~/.kaggle/kaggle.json`
- **Databricks config**: `~/.databricks/config`

## Security Notes

⚠️ **Important**:
- Secrets are encrypted at rest in Databricks
- Never hardcode credentials in notebooks
- Always use `dbutils.secrets.get()` to retrieve secrets
- You can only read secrets, not view their values
- The script doesn't store credentials locally in plain text

## Next Steps

1. Run: `python setup_databricks_secrets.py`
2. Open [notebooks/kaggle_loader.ipynb](notebooks/kaggle_loader.ipynb) in Databricks
3. Run the notebook cells
4. Your data will be downloaded from Kaggle and uploaded to Databricks Volumes!
