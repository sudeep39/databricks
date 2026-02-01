#!/usr/bin/env python3
"""
Databricks Secret Setup Script

This script creates a secret scope in Databricks and adds Kaggle credentials.
Run this script once to set up secrets for your Databricks workspace.

Usage:
    python setup_databricks_secrets.py
"""

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

try:
    from databricks.sdk import WorkspaceClient
except ImportError:
    print("Error: databricks-sdk not installed.")
    print("Attempting to install databricks-sdk...")
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "databricks-sdk"]
        )
        print("✓ databricks-sdk installed successfully")
        from databricks.sdk import WorkspaceClient
    except subprocess.CalledProcessError:
        print("✗ Failed to install databricks-sdk")
        print("Install it manually with: pip install databricks-sdk")
        sys.exit(1)


class DatabricksSecretManager:
    """Manages Databricks secret scopes and secrets."""

    @staticmethod
    def install_missing_packages(packages: List[str]) -> bool:
        """
        Install missing Python packages using pip.

        Args:
            packages: List of package names to install

        Returns:
            True if all packages installed successfully, False otherwise
        """
        print(f"Installing required packages: {', '.join(packages)}")
        try:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install"] + packages,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            print(f"✓ Packages installed successfully")
            return True
        except subprocess.CalledProcessError:
            print(f"✗ Failed to install packages")
            print(f"Install manually with: pip install {' '.join(packages)}")
            return False

    def __init__(self):
        """Initialize Databricks workspace client with OAuth."""
        try:
            self.client = WorkspaceClient()
            print("✓ Connected to Databricks workspace")
        except Exception as e:
            print(f"✗ Failed to connect to Databricks: {e}")
            print("Make sure your Databricks credentials are configured:")
            print("  databricks configure --auth-type oauth")
            sys.exit(1)

    def create_scope(self, scope_name: str) -> bool:
        """
        Create a secret scope.

        Args:
            scope_name: Name of the scope to create

        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if scope already exists
            scopes = self.client.secrets.list_scopes()
            existing_scopes = [s.name for s in scopes]

            if scope_name in existing_scopes:
                print(f"ℹ Scope '{scope_name}' already exists")
                return True

            # Create new scope
            self.client.secrets.create_scope(scope=scope_name)
            print(f"✓ Secret scope '{scope_name}' created successfully")
            return True

        except Exception as e:
            print(f"✗ Failed to create scope '{scope_name}': {e}")
            return False

    def add_secret(self, scope: str, key: str, value: str) -> bool:
        """
        Add a secret to a scope.

        Args:
            scope: Name of the scope
            key: Secret key name
            value: Secret value

        Returns:
            True if successful, False otherwise
        """
        try:
            self.client.secrets.put_secret(scope=scope, key=key, string_value=value)
            print(f"✓ Secret '{key}' added to scope '{scope}'")
            return True

        except Exception as e:
            print(f"✗ Failed to add secret '{key}': {e}")
            return False

    def list_secrets(self, scope: str) -> list:
        """
        List all secrets in a scope.

        Args:
            scope: Name of the scope

        Returns:
            List of secret keys in the scope
        """
        try:
            secrets = self.client.secrets.list_secrets(scope=scope)
            return [s.key for s in secrets]
        except Exception as e:
            print(f"✗ Failed to list secrets in scope '{scope}': {e}")
            return []

    def setup_kaggle_secrets(self, username: str, api_key: str) -> bool:
        """
        Set up Kaggle secrets.

        Args:
            username: Kaggle username
            api_key: Kaggle API key

        Returns:
            True if successful, False otherwise
        """
        scope_name = "kaggle-scope"

        print(f"\n{'='*60}")
        print(f"Setting up Kaggle secrets in Databricks")
        print(f"{'='*60}\n")

        # Step 1: Create scope
        if not self.create_scope(scope_name):
            return False

        # Step 2: Add secrets
        print("\nAdding Kaggle credentials to scope...")
        if not self.add_secret(scope_name, "username", username):
            return False

        if not self.add_secret(scope_name, "api-key", api_key):
            return False

        # Step 3: Verify
        print("\nVerifying secrets...")
        secrets = self.list_secrets(scope_name)
        if "username" in secrets and "api-key" in secrets:
            print(f"✓ Secrets verified in scope '{scope_name}'")
            print(f"  Available secrets: {', '.join(secrets)}")
            return True
        else:
            print("✗ Verification failed")
            return False


def load_kaggle_credentials_from_file(
    credentials_file: Optional[str] = None,
) -> tuple:
    """
    Load Kaggle credentials from kaggle.json file.

    Args:
        credentials_file: Path to kaggle.json (defaults to ~/Downloads/kaggle.json)

    Returns:
        Tuple of (username, api_key)
    """
    if not credentials_file:
        credentials_file = Path.home() / "Downloads" / "kaggle.json"

    credentials_file = Path(credentials_file)

    if not credentials_file.exists():
        return None, None

    try:
        with open(credentials_file) as f:
            creds = json.load(f)
            return creds.get("username"), creds.get("key")
    except Exception as e:
        print(f"✗ Failed to read credentials file: {e}")
        return None, None


def prompt_for_credentials() -> tuple:
    """
    Prompt user for Kaggle credentials.

    Returns:
        Tuple of (username, api_key)
    """
    print("\nEnter your Kaggle credentials:")
    print("(Get these from https://www.kaggle.com/settings/account)")
    print()

    username = input("Kaggle Username: ").strip()
    if not username:
        print("✗ Username cannot be empty")
        return None, None

    api_key = input("Kaggle API Key: ").strip()
    if not api_key:
        print("✗ API Key cannot be empty")
        return None, None

    return username, api_key


def main():
    """Main function to set up Databricks secrets."""
    print(f"\n{'='*60}")
    print("Databricks Secret Setup")
    print(f"{'='*60}\n")

    # Try to load credentials from file
    print("Checking for kaggle.json file...")
    username, api_key = load_kaggle_credentials_from_file()

    if username and api_key:
        print(f"✓ Found Kaggle credentials in ~/.kaggle/kaggle.json")
        print(f"  Username: {username}")

        # Confirm with user
        confirm = input("\nUse these credentials? (yes/no): ").strip().lower()
        if confirm not in ["yes", "y"]:
            username, api_key = prompt_for_credentials()
    else:
        print("ℹ No kaggle.json file found")
        username, api_key = prompt_for_credentials()

    if not username or not api_key:
        print("\n✗ Setup cancelled")
        sys.exit(1)

    # Initialize manager and set up secrets
    manager = DatabricksSecretManager()
    success = manager.setup_kaggle_secrets(username, api_key)

    print(f"\n{'='*60}")
    if success:
        print("✓ Setup completed successfully!")
        print(f"{'='*60}\n")
        print("You can now use Kaggle secrets in your notebooks:")
        print()
        print("  import os")
        print(
            "  os.environ['KAGGLE_USERNAME'] = dbutils.secrets.get('kaggle-scope', 'username')"
        )
        print(
            "  os.environ['KAGGLE_KEY'] = dbutils.secrets.get('kaggle-scope', 'api-key')"
        )
        print()
        return 0
    else:
        print("✗ Setup failed!")
        print(f"{'='*60}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
