# from pyquery import PyQuery as pq
import requests
from bln import Client
# from test_helpers import (
#     test_filter_old_data,
#     test_override_new_timestamps,
#     test_simulate_deletions,
# )
# from bots.slack_alerts import SlackInternalAlert
# import csv
from datetime import datetime
# from dateutil.relativedelta import relativedelta
import os
from dotenv import load_dotenv
# import shutil
import re
import sys
import logging
from pathlib import Path
from typing import List, Dict, Optional

logging.basicConfig(
    format="\n%(asctime)s %(levelname)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%I:%M:%S",
)
logger = logging.getLogger(__name__)

def set_environment():
    """
    Load and validate environment variables from a `.env.<env>` file based on a required command-line argument.

    This function:
    - Requires the first command-line argument to be either 'test' or 'prod'
    - Loads environment variables from the corresponding `.env.test` or `.env.prod` file
    - Verifies that all required environment variables are present and non-empty
    - Logs success or exits with errors if configuration is missing or invalid

    Usage:
        python sync_doge_scrape.py <test|prod>

    Returns:
        str: The name of the environment that was successfully loaded ('test' or 'prod').

    Raises:
        SystemExit: If the CLI argument is missing or invalid.
        RuntimeError: If any required environment variable is missing or empty.
    """
    if len(sys.argv) < 2:
        logger.error("python sync_doge_scrape.py <test|prod>")
        sys.exit(1)

    env = sys.argv[1].strip()
    if env not in ["test", "prod"]:
        logger.error(f"Invalid env '{env}'. Use 'test' or 'prod'.")
        sys.exit(1)

    # Get the directory this script is in
    script_dir = Path(__file__).parent
    # Build path to the .env file relative to this script
    dotenv_path = script_dir / f".env.{env}"

    logger.info(f"Loading environment from: {dotenv_path}")
    load_dotenv(dotenv_path=dotenv_path, override=True)
    # make sure all required env vars are present
    required_env_vars = [
        "BLN_API_TOKEN",
        "BLN_PROJECT_ID",
        "SLACK_ERROR_TOKEN",
        "SLACK_ERROR_CHANNEL_ID",
    ]
    for key in required_env_vars:
        value = os.environ.get(key)
        if not value or not value.strip():
            logger.error(f"Missing or empty: {key} (from {dotenv_path})")
            raise RuntimeError(
                f"Required env variable '{key}' is missing or empty in {dotenv_path}"
            )
    logger.info(f"{dotenv_path} successsfully loaded")
    return env

def list_github_dir(owner: str, repo: str, path: str, ref: str = 'main', token: Optional[str] = None) -> List[Dict]:
    """
    List files in a GitHub repo directory.

    Returns:
        List of dicts with 'name', 'path', 'download_url'

    Note:
    - Only need API token if we hit a rate limit or the repo is private
    """
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    headers = {'Accept': 'application/vnd.github+json'}
    if token:
        headers['Authorization'] = f'token {token}'

    resp = requests.get(url, headers=headers, params={'ref': ref})
    resp.raise_for_status()
    items = resp.json()

    files = []
    for item in items:
        if item['type'] == 'file':
            files.append({
                'name': item['name'],
                'path': item['path'],
                'download_url': item['download_url'],
            })
    return files


def get_last_commit_dates(owner: str, repo: str, file_paths: List[str], ref: str = 'main', token: Optional[str] = None) -> Dict[str, str]:
    """
    Given a list of file paths in a GitHub repo, return the last commit timestamp for each.

    Returns:
        Dict mapping file path to a compact ISO-style timestamp (e.g., 2025-02-18T232513).
    """
    headers = {'Accept': 'application/vnd.github+json'}
    if token:
        headers['Authorization'] = f'token {token}'

    commit_dates = {}
    for path in file_paths:
        url = f"https://api.github.com/repos/{owner}/{repo}/commits"
        resp = requests.get(url, headers=headers, params={'path': path, 'sha': ref, 'per_page': 1})
        resp.raise_for_status()
        data = resp.json()

        if data:
            raw_ts = data[0]['commit']['committer']['date']  # e.g., "2025-02-18T23:25:13Z"
            dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))  # Handle UTC time with Z
            formatted_ts = dt.strftime("%Y-%m-%dT%H%M%S")  # e.g., "2025-02-18T232513"
            commit_dates[path] = formatted_ts
        else:
            commit_dates[path] = None

    return commit_dates


def list_bln_project_files(client: Client, project_id: str) -> List[Dict[str, str]]:
    """
    List files currently uploaded to a BLN project.

    Args:
        client (Client): An initialized BLN API client.
        project_id (str): The BLN project ID.

    Returns:
        list of filenames
    """
    project = client.get_project_by_id(project_id)
    current_files = []
    for f in project.get("files", []):
        current_files.append(f["name"])

    return current_files
