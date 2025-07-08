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
    owner: str, repo: str, path: str, ref: str = "main", token: Optional[str] = None
) -> List[Dict[str, str]]:
    """
    Retrieve files from a GitHub repository directory, enriched with their last commit timestamp.

    Args:
        owner: GitHub username or organization name (e.g., 'm-nolan').
        repo: Repository name (e.g., 'doge-scrape').
        path: Directory path within the repo to list files from (e.g., 'data').
        ref: Branch name or commit SHA to use as the reference point (default: 'main').
        token: Optional GitHub token for authenticated API access.

    Returns:
        A dict keyed by comparison filename (e.g., 'doge-contract_2025-06-29T204434.csv'),
        with values containing:
            - 'path': str — the file's path in the repo
            - 'download_url': str — raw URL to download the file
            - 'timestamp': str — the last commit timestamp for the file
            - 'name': str — original filename
    """
    files = list_github_dir(owner, repo, path, ref=ref, token=token)
    file_paths = [f["path"] for f in files]
    last_modified = get_last_commit_dates(owner, repo, file_paths, ref=ref, token=token)

    enriched = defaultdict(dict)
    for f in files:
        timestamp = last_modified.get(f["path"])
        name = f["name"]
        path = f["path"]
        download_url = f["download_url"]
        base_name, ext = name.split(".")
        comparison_name = f"{base_name}_{timestamp}.{ext}"
        enriched[comparison_name] = {
            "path": path,
            "download_url": download_url,
            "timestamp": timestamp,
            "name": name,
        }
    return enriched


def get_new_github_files_for_bln(
    bln_file_names: List[str], github_files: Dict[str, Dict]
) -> Dict[str, Dict]:
    """
    Identify which GitHub-hosted files are not yet present in the BLN project and should be uploaded.

    Compares the list of existing file names in BLN ploject against the versioned filenames
    from GitHub and returns only those that are new. Also logs a summary message
    and sends a Slack alert with the count of new files found.

    Args:
        bln_file_names: List of filenames currently in the BLN project
                        (e.g., ['doge-contract_2025-06-29T204434.csv']).
        github_files: Dict of GitHub files keyed by versioned BLN-style filename
                      (e.g., 'doge-contract_2025-06-29T204434.csv'), with values containing:
                          {
                              'path': str,
                              'download_url': str,
                              'timestamp': str,
                              'name': str
                          }

    Returns:
        Dict of new GitHub files (not yet present in BLN), keyed by versioned filename.
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
    Download and upload GitHub-hosted files to the BLN project.

    Each file is downloaded to a temporary location (retaining its versioned filename) and uploaded to BLN.
    Slack alerts are posted summarizing which files were successfully uploaded and which (if any) failed.

    Args:
        github_files: Dict of GitHub files to upload, keyed by versioned BLN-style filename
                      (e.g., "doge-contract_2025-06-29T204434.csv"). Each value must include:
                          {
                              'path': str,           # GitHub path (e.g., "data/doge-contract.csv")
                              'download_url': str,   # Direct raw URL to download the file
                              'timestamp': str,      # Commit timestamp (e.g., "2025-06-29T204434")
                              'name': str            # Original GitHub filename (e.g., "doge-contract.csv")
                          }

        client: An initialized instance of the BLN API Client.
        project_id: The unique BLN project ID where the files will be uploaded.
        slackbot_alerter: A callable or bot object
        delay_seconds: Optional delay between uploads (default: 1.0 second) to avoid rate limiting or API overload.

    Behavior:
        - If no files are passed, logs and sends a Slack "notice" stating that no new files were found.
        - After processing, logs and alerts on the number of successful and failed uploads.
    """
    uploads = {"success": [], "failure": []}

    with tempfile.TemporaryDirectory() as tmpdir:
        for filename, metadata in github_files.items():
            logger.info(f"⬇️ Downloading {filename} ...")
            response = requests.get(metadata["download_url"])
            if not response.ok:
                logger.error(
                    f"❌ Failed to download {filename} — Status code: {response.status_code}"
                )
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

    if uploads["success"]:
        message = f"{len(uploads['success'])} new files uploaded to BLN project ({uploads['success']})"
        logger.info(message)
    if uploads["failure"]:
        message = f"New files failed upload to BLN: {uploads['failure']}"
        logger.info(message)
    return uploads


def run_pipeline(environment):
    """
    Orchestrates the full pipeline to sync updated files from a GitHub directory into a BLN project.

    Steps:
        1. Logs and alerts the start of the run.
        2. Fetches the list of files in the source GitHub repo (with last-modified timestamps).
        3. Fetches the list of current files in the target BLN project.
        4. Compares the two to determine which GitHub files are new.
        5. Downloads and uploads new files to the BLN project.
        6. Logs and alerts the result of the operation.

    Args:
        environment: The environment label (e.g. 'prod', 'test') used for logging and alert context.

    Assumptions:
        - `BLN_API_TOKEN` and `BLN_PROJECT_ID` are available in the environment.
        - GitHub source repo is 'm-nolan/doge-scrape', targeting the 'data/' directory.
        - Uses SlackInternalAlert for operational notifications.

    Side Effects:
        - Posts notices and success/failure alerts to Slack.
        - Logs detailed sync status using the configured logger.
        - Uploads any new GitHub-hosted files to BLN.
    """
    starting_message = f"Starting run for {environment.upper()} env"
    logger.info(starting_message)
    bln_api_key = os.environ.get("BLN_API_TOKEN")
    bln_project_id = os.environ.get("BLN_PROJECT_ID")
    bln_client = Client(bln_api_key)
    source_repo = "doge-scrape"
    source_repo_owner = "m-nolan"
    source_data_path = "data"

    SLACK_BOT_INTERNAL_ALERTER = SlackInternalAlert("doge-scrape")

    github_files = get_files_with_last_modified(
        source_repo_owner,
        source_repo,
        path=source_data_path,
    )

    message = (
        f"Files found in '{source_repo}/{source_data_path}' github: {github_files}"
    )
    logger.info(message)

    bln_project_files = list_bln_project_files(bln_client, bln_project_id)
    logger.info(f"Files found in BLN project: {bln_project_files}")

    new_github_files = get_new_github_files_for_bln(
        bln_project_files, github_files
    )
    file_upload_message = ""
    failed_uploads = None
    outcome = None

    if new_github_files:
        uploads = copy_github_files_to_bln(
            github_files=new_github_files,
            client=bln_client,
            project_id=bln_project_id,
            slackbot_alerter=SLACK_BOT_INTERNAL_ALERTER,
        )
        if uploads["success"]:
            success_uploads = uploads['success']
            file_upload_message = f"{len(success_uploads)} new/updated file(s) uploaded to BLN project ({', '.join(success_uploads)})"
            outcome = "success"
        if uploads["failure"]:
            failure_uploads = uploads['failure']
            file_upload_message = f"{file_upload_message}. {len(failure_uploads)} new/updated file(s) failed to upload to BLN project ({', '.join(failure_uploads)})"
            outcome = "error"

    else:
        file_upload_message = f"No new files found."
        outcome = "success"
    
    base_final_message = "Process complete."
    final_message = f"{base_final_message} {file_upload_message}"

    logger.info(final_message)
    SLACK_BOT_INTERNAL_ALERTER.post(final_message, outcome)
