# Running Databricks Code from VS Code

This guide explains how to connect VS Code to your Databricks workspace and run code directly.

## Method 1: Databricks Extension for VS Code (Recommended)

### Step 1: Install Databricks Extension
1. Open VS Code
2. Go to Extensions (Cmd+Shift+X)
3. Search for "Databricks"
4. Install the official **Databricks** extension by Databricks

### Step 2: Configure Databricks Connection with OAuth
1. Press Cmd+Shift+P (or Ctrl+Shift+P on Windows/Linux)
2. Search for "Databricks: Configure"
3. Select "Databricks: Configure Databricks Profile"
4. When prompted for authentication method, choose **OAuth**
5. You'll be redirected to your browser to authenticate
6. Authorize VS Code to access your Databricks workspace
7. Return to VS Code - connection is now configured

### Step 3: Verify Connection
```bash
# Test connection with Databricks CLI
databricks fs ls /Volumes/workspace/default
```

---

## OAuth Configuration Details

### How OAuth Works
- VS Code opens a browser window for you to log in to Databricks
- You grant VS Code permission to access your workspace
- Credentials are securely stored locally (no tokens in config files)
- Automatically refreshes without manual intervention

### VS Code Settings for OAuth
Your settings will be stored in `.databricks/config`:
```ini
[DEFAULT]
auth_type = oauth
# Other credentials managed securely by OS keychain
```

### Troubleshooting OAuth

**Issue: "OAuth flow cancelled"**
- Make sure to click "Allow" in the browser authorization screen
- Return to VS Code after authorization

**Issue: "Host not found"**
- Verify your workspace URL is correct
- Check your internet connection
- Clear cache: `rm -rf ~/.databricks/`

**Issue: "Token expired"**
- OAuth tokens auto-refresh, but if issues persist:
  - Run `Databricks: Sign Out`
  - Run `Databricks: Configure` again

### OAuth vs PAT

| Feature | OAuth | PAT |
|---------|-------|-----|
| Security | Browser-based auth | Token stored locally |
| Refresh | Automatic | Manual |
| Expiration | Built-in | User-defined |
| Setup | Browser popup | Manual token copy |
| Recommended | ✅ Yes | For automation only |

---

### Step 4: Run Code in Databricks

#### Option A: Create a Databricks Notebook
1. Right-click on a folder in VS Code
2. Select "Create Databricks Notebook"
3. Create a new `.ipynb` file
4. Add your code:

```python
# Cell 1: Setup
%pip install kaggle pyspark

# Cell 2: Import and configure
import sys
sys.path.append("/Workspace/databricks")

from basics.file_read import main

# Cell 3: Run
df = main(
    kaggle_dataset="aekundayo/health-insurance-data",
    csv_file="BenefitsCostSharing.csv",
    databricks_path="/Volumes/workspace/default/kaggle",
    download=True
)
```

#### Option B: Upload Python Module to Workspace
```bash
# Upload your code to Databricks workspace
databricks workspace import_dir ./basics /Users/your-email/databricks/basics
```

Then in a Databricks notebook:
```python
%pip install kaggle
import sys
sys.path.append("/Workspace/Users/your-email/databricks")

from basics.file_read import main

df = main(
    kaggle_dataset="aekundayo/health-insurance-data",
    csv_file="BenefitsCostSharing.csv",
    download=True
)
```

---

## Method 2: Databricks CLI + Job Submission (with OAuth)

### Step 1: Install Databricks CLI
```bash
pip install databricks-cli
```

### Step 2: Configure with OAuth
```bash
# Start OAuth configuration
databricks configure --auth-type oauth

# The CLI will:
# 1. Open your browser
# 2. Ask you to authorize the CLI
# 3. Redirect back with confirmation
# 4. Save credentials securely
```

**Note**: With OAuth, your credentials are managed by your OS (macOS Keychain, Windows Credential Manager, Linux pass).

### Step 2a: Alternative - Configure with Host Only (OAuth Device Flow)
If browser auth doesn't work:
```bash
# Configure just the host
databricks configure --host https://dbc-7bdc29d4-ba25.cloud.databricks.com

# Then authenticate with device flow
databricks auth token
```

### Step 3: Create a Databricks Job Configuration

Create a file `databricks-job.yml`:
```yaml
name: kaggle-loader-job
tasks:
  - task_key: load_kaggle_data
    spark_python_task:
      python_file: dbfs:/databricks/scripts/file_read.py
      parameters: [
        "--dataset=aekundayo/health-insurance-data",
        "--file=BenefitsCostSharing.csv",
        "--databricks-path=/Volumes/workspace/default/kaggle"
      ]
    new_cluster:
      spark_version: "14.3.x-scala2.12"
      node_type_id: "i3.xlarge"
      num_workers: 1
```

### Step 4: Upload Script to DBFS
```bash
# Create a wrapper script for job execution
databricks fs cp ./basics/file_read.py dbfs:/databricks/scripts/file_read.py --overwrite
```

### Step 5: Submit Job
```bash
# Run the job
databricks jobs run-now --job-id <job-id>

# Or create and run
databricks jobs create --json-file databricks-job.yml
```

---

## Method 3: VS Code Remote Containers (SSH to Databricks Compute)

### Step 1: Install Remote - SSH Extension
1. Install "Remote - SSH" extension in VS Code
2. Configure SSH access to a Databricks cluster

### Step 2: Connect via SSH
```bash
# SSH into cluster through Databricks
ssh -i ~/.ssh/your-key.pem ubuntu@<cluster-public-ip>
```

---

## Method 4: Sync with Databricks Workspace (Git Integration)

### Step 1: Add Databricks to VS Code Settings
Create `.vscode/settings.json`:
```json
{
  "databricks.pythonPath": "/usr/bin/python3",
  "databricks.defaultWorkspacePath": "/Workspace/Users/your-email@company.com/databricks"
}
```

### Step 2: Auto-Sync on Save
Enable auto-sync in Databricks extension settings to sync code when you save.

---

## Step-by-Step: Running Your Code Now

### Quick Start (Method 1 - Recommended):

1. **Install Databricks Extension** in VS Code

2. **Configure Connection**:
   ```
   Cmd+Shift+P → Databricks: Configure
   ```

3. **Create a test notebook** in VS Code (`.ipynb` file):
   ```python
   %pip install kaggle pyspark
   
   import os
   os.environ["KAGGLE_USERNAME"] = dbutils.secrets.get("kaggle-scope", "username")
   os.environ["KAGGLE_KEY"] = dbutils.secrets.get("kaggle-scope", "api-key")
   
   import sys
   sys.path.append("/Workspace/databricks")
   
   from basics.file_read import main
   
   df = main(
       kaggle_dataset="aekundayo/health-insurance-data",
       csv_file="BenefitsCostSharing.csv",
       download=True
   )
   ```

4. **Run the notebook** using the Databricks extension

5. **View results** in VS Code's output panel

---

## Troubleshooting

### OAuth-Specific Issues

**Issue: "OAuth flow cancelled"**
- Browser window may have closed or timed out
- Run `Databricks: Configure` again
- Make sure cookies are enabled in your browser

**Issue: "Invalid OAuth scope"**
- This is typically handled automatically by Databricks
- If persists, clear cache: `rm -rf ~/.databricks/`
- Reconfigure: `Databricks: Configure`

**Issue: "Connection refused" after OAuth**
- OAuth succeeded but connection failed
- Verify your workspace URL: `https://dbc-7bdc29d4-ba25.cloud.databricks.com`
- Check internet connection
- Restart VS Code

### General Issues

**Issue: "Module not found"**
- Make sure Python module is uploaded to workspace
- Use full workspace path: `/Workspace/Users/your-email/databricks/basics`
- Or install as a package: `%pip install -e /Workspace/Users/your-email/databricks`

**Issue: "Kaggle authentication failed"**
- Verify Kaggle secrets are created in Databricks
- Check credentials with: `dbutils.secrets.get("kaggle-scope", "username")`
- Use VS Code's Databricks terminal to test commands

**Issue: "PySpark not found"**
- Run `%pip install pyspark` in the first cell of your notebook
- This is automatic on Databricks clusters but ensure it's in the first cell

---

## Useful Databricks CLI Commands

```bash
# List workspace contents
databricks workspace ls /Volumes

# Upload a file
databricks fs cp ./file.py dbfs:/path/to/file.py

# Run a job
databricks jobs run-now --job-id 123

# List clusters
databricks clusters list

# Get cluster details
databricks clusters get --cluster-id <cluster-id>
```

## VS Code Extensions to Install

1. **Databricks** - Official extension for workspace integration
2. **Python** - For Python syntax highlighting
3. **Jupyter** - For notebook support
4. **Remote - SSH** - For SSH access to clusters
5. **REST Client** - Optional, for testing Databricks API

---

## Next Steps

1. Install the Databricks extension
2. Configure your workspace connection
3. Create a test notebook
4. Upload your Python module
5. Run and monitor jobs from VS Code
