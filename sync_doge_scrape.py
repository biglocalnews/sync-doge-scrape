import logging
from typing import List, Dict, Optional
import os
from bln import Client
from collections import defaultdict
import requests
import tempfile
import logging
from time import sleep
from helpers import list_github_dir, get_last_commit_dates, list_bln_project_files
from bots.slack_alerts import SlackInternalAlert

logging.basicConfig(
    format="\n%(asctime)s %(levelname)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%I:%M:%S",
)
logger = logging.getLogger(__name__)

def get_files_with_last_modified(
    owner: str,
    repo: str,
    path: str,
    ref: str = 'main',
    token: Optional[str] = None
) -> List[Dict[str, str]]:
    """
    Get files in GitHub dir along with their last commit date and download url
    """
    files = list_github_dir(owner, repo, path, ref=ref, token=token)
    file_paths = [f['path'] for f in files]
    last_modified = get_last_commit_dates(owner, repo, file_paths, ref=ref, token=token)

    enriched = defaultdict(dict)
    for f in files:
        timestamp = last_modified.get(f['path'])
        name= f['name']
        path= f['path']
        download_url= f['download_url']
        base_name, ext = name.split(".")
        comparison_name = f"{base_name}_{timestamp}.{ext}"
        enriched[comparison_name] = {
            'path': path,
            'download_url': download_url,
            'timestamp': timestamp,
            'name' : name
        }
    return enriched


def get_new_github_files_for_bln(
    bln_file_names: List[str],
    github_files: Dict[str, Dict],
    SLACK_BOT_INTERNAL_ALERTER
) -> Dict[str, Dict]:
    """
    Return GitHub files that should be uploaded to BLN because they're not already present.

    Args:
        bln_file_names: List of existing filenames in the BLN project.
        github_files: Dict of GitHub files keyed by full versioned filename (e.g. 'doge-contract_2025-06-29T204434.csv').

    Returns:
        Dict of GitHub files that are new (not already in BLN).
    """
    bln_file_set = set(bln_file_names)
    new_files = {}

    for filename, metadata in github_files.items():
        if filename not in bln_file_set:
            new_files[filename] = metadata
    
    message = f"{len(new_files)} new files found"
    logger.info(message)

    return new_files

def copy_github_files_to_bln(
    github_files: Dict[str, Dict],
    client: Client,
    project_id: str,
    slackbot_alerter,
    delay_seconds: float = 1.0,
) -> None:
    """
    Copy GitHub-hosted files into a BLN project by downloading them to disk
    (with correct filenames) and uploading via API.

    Args:
        github_files: Dict of files to copy, where each key is a versioned BLN-style filename
                      (e.g., "doge-contract_2025-06-29T204434.csv") and the value is a dict with:
                          {
                              'path': str,
                              'download_url': str,
                              'timestamp': str,
                              'name': str
                          }

        client: An initialized BLN API client.
        project_id: The BLN project ID to upload to.
        delay_seconds: Optional sleep between uploads to avoid rate limiting (default 1.0s).
    """

    if not github_files:
        message = "No new files found in source repo."
        logger.info(message)
        slackbot_alerter.post(message, "notice")
        return

    uploads = {"success": [], "failure": []}

    with tempfile.TemporaryDirectory() as tmpdir:
        for filename, metadata in github_files.items():
            logger.info(f"⬇️ Downloading {filename} ...")
            response = requests.get(metadata["download_url"])
            if not response.ok:
                logger.error(f"❌ Failed to download {filename} — Status code: {response.status_code}")
                uploads["failure"].append(filename)
                continue

            local_path = os.path.join(tmpdir, filename)
            try:
                with open(local_path, "wb") as f:
                    f.write(response.content)

                logger.info(f"⬆️ Uploading {filename} to BLN ...")
                client.upload_file(project_id, local_path)
                uploads["success"].append(filename)

            except Exception as e:
                logger.error(f"❌ Failed to upload {filename}: {e}")
                uploads["failure"].append(filename)

            sleep(delay_seconds)  # avoid overloading the API

    if uploads['success']:
        message = f"{len(uploads['success'])} new files uploaded to BLN project ({uploads['success']})"
        logger.info(message)
        slackbot_alerter.post(message, "notice")
    if uploads['failure']:
        message = f"New files failed upload to BLN: {uploads['failure']}"
        logger.info(message)
        slackbot_alerter.post(message, "error")


def run_pipeline(environment):
    starting_message = f"Starting run for {environment.upper()} env"
    logger.info(starting_message)
    SLACK_BOT_INTERNAL_ALERTER = SlackInternalAlert("sync-DOGE-scrape")
    SLACK_BOT_INTERNAL_ALERTER.post(starting_message, "notice")
    bln_api_key = os.environ.get("BLN_API_TOKEN")
    bln_project_id = os.environ.get("BLN_PROJECT_ID")
    bln_client = Client(bln_api_key)
    source_repo = "doge-scrape"
    source_repo_owner = 'm-nolan'
    source_data_path = "data"

    github_files = get_files_with_last_modified(
        source_repo_owner,
        source_repo,
        path=source_data_path,
    )

    message = f"Files found in '{source_repo}/{source_data_path}' github: {github_files}"
    logger.info(message)

    bln_project_files = list_bln_project_files(bln_client, bln_project_id)
    logger.info(f"Files found in BLN project: {bln_project_files}")

    new_github_files = get_new_github_files_for_bln(bln_project_files, github_files, SLACK_BOT_INTERNAL_ALERTER)

    copy_github_files_to_bln(
        github_files=new_github_files,
        client=bln_client,
        project_id=bln_project_id,
        slackbot_alerter = SLACK_BOT_INTERNAL_ALERTER
    )

    message = "Process complete"
    logger.info(message)
    SLACK_BOT_INTERNAL_ALERTER.post(message, "success")
