import logging
import os
import pandas as pd
import tempfile
from collections import defaultdict
from datetime import datetime
from time import sleep
from typing import Dict, List, Optional

import requests
from bln import Client

from bots.slack_alerts import SlackInternalAlert
from doge_scrape import scrape_doge, clean_stub_df, df_row_diff_2, extend_contract_data, extend_grant_data
from helpers import get_last_commit_dates, list_bln_project_files, list_new_bln_project_files, list_github_dir

logging.basicConfig(
    format="\n%(asctime)s %(levelname)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
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


def copy_scrape_files_to_bln(
    scrape_files: List[str],
    client: Client,
    project_id: str,
    file_dir: str = "./tmp_data",
    delay_seconds: float = 1.0
) -> Dict:
    """
    Upload specified files to the BLN project.

    Args:
        scrape_files: list of file names idenitfying files to transfer to a specified BLN project directory.
        client: initialized instance of the BLN API client.
        project_id: unique BLN project ID where the files will be uploaded.
        file_dir: directory where `scrape_files` are located. Default: './tmp_data'.
        delay_seconds: wait time between upload attempts. Default: 1.0 seconds.

    Returns:
        uploads: dict of successfully and unsuccessfully uploaded files stored as lists of file names under the keys "success" and "failure."
    """
    
    uploads = {"success": [], "failure": []}
    
    for filename in scrape_files:
        filepath = os.path.join(file_dir, filename)
        try:
            logger.info(f"⬆️ Uploading {filename} to BLN ...")
            client.upload_file(project_id, filepath)
            uploads["success"].append(filename)
            
        except Exception as e:
            logger.error(f"❌ Failed to upload {filename}: {e}")

        sleep(delay_seconds)

    if uploads["success"]:
        message = f"{len(uploads['success'])} new files uploaded to BLN project ({uploads['success']})"
        logger.info(message)
    if uploads["failure"]:
        message = f"New files failed upload to BLN: {uploads['failure']}"
        logger.info(message)
    return uploads


def load_pre_data_bln(
        bln_client: Client, 
        bln_project_id: str, 
        bln_project_files: List[str], 
        tmp_dir: str="./tmp_data"
    ):
    """
    Downloads CSV files listed in the specified BLN project directory and loads them as pandas DataFrames.

    Args:
        bln_client: initialized instance of the BLN API client.
        bln_project_id: unique BLN project ID where the files will be uploaded.
        bln_project_files: list of files in BLN project directory to download.
        tmp_dir: local directory where BLN project files are saved.

    Returns:
        pre_contract_df, pre_grant_df, pre_property_df: pandas DataFrames containing DOGE contract, grant, and property/lease cuts, respectively.
    """

    # download copies of the existing files
    for project_file in bln_project_files:
        bln_client.download_file(bln_project_id, project_file, output_dir=tmp_dir)
    # load files as dataframes
    contract_file = [f for f in bln_project_files if 'contract' in f][0]    # there should only be a single file in the resulting list but this unpacks the filtered output
    pre_contract_df = pd.read_csv(os.path.join(tmp_dir,contract_file))
    grant_file = [f for f in bln_project_files if 'grant' in f][0]
    pre_grant_df = pd.read_csv(os.path.join(tmp_dir,grant_file))
    property_file = [f for f in bln_project_files if 'property' in f][0]
    pre_property_df = pd.read_csv(os.path.join(tmp_dir,property_file))
    return pre_contract_df, pre_grant_df, pre_property_df


def update_doge_data(bln_client, bln_project_id, bln_project_files):
    """
    Crawls the DOGE "savings" site and scrape its current tables containing cut federal grants, contracts and leases.
    New entries are added to pandas DataFrames to be uploaded to the BLN DOGE tracker project.

    Args:
        bln_client: initialized instance of the BLN API client.
        bln_project_id: unique BLN project ID where the files will be uploaded.
        bln_project_files: list of files in BLN project directory to download and update.

    Returns:
        contract_df, grant_df, property_df: pandas dataframes of DOGE program cuts, separated by type
        new_data_stats: dict of new entry counts for contracts, grants and leases.
    """
    datetime_scrape = datetime.strftime(datetime.now(),'%Y-%m-%d-%H%M')
    logger.info('loading current data...')
    pre_contract_df, pre_grant_df, pre_property_df = load_pre_data_bln(bln_client, bln_project_id, bln_project_files)
    logger.info('scraping new data...')
    stub_contract_df, stub_grant_df, stub_property_df = scrape_doge()
    stub_contract_df, stub_grant_df, stub_property_df = [clean_stub_df(df) for df in [stub_contract_df, stub_grant_df, stub_property_df]]
    logger.info('finding new and changed entries...')
    (new_contract_df, contract_drop_idx), (new_grant_df, grant_drop_idx), (new_property_df, property_drop_idx) = [
        df_row_diff_2(pre_df,stub_df) for pre_df, stub_df in zip(
            [pre_contract_df,pre_grant_df,pre_property_df],[stub_contract_df, stub_grant_df, stub_property_df]
        )
    ] # dropped idx values are for debugging and tracking erroneously ejected "duplicate" entries.
    new_data_stats = {
        "contract": len(new_contract_df),
        "grant": len(new_grant_df),
        "property": len(new_property_df)
    }
    logger.info('extending contract table with FPDS data...')
    new_contract_df = extend_contract_data(new_contract_df,datetime_scrape)
    new_contract_df['dt_scrape'] = datetime_scrape
    contract_df = pd.concat([pre_contract_df,new_contract_df],ignore_index=True)
    logger.info('extending grant table with USASpending data...')
    new_grant_df = extend_grant_data(new_grant_df,datetime_scrape)
    new_grant_df['dt_scrape'] = datetime_scrape
    grant_df = pd.concat([pre_grant_df,new_grant_df],ignore_index=True)
    new_property_df['dt_scrape'] = datetime_scrape
    property_df = pd.concat([pre_property_df,new_property_df],ignore_index=True)
    return contract_df, grant_df, property_df, new_data_stats


def save_doge_data_bln(contract_df, grant_df, property_df, new_data_stats, data_dir="./tmp_data"):
    """
    Save dataframes of DOGE data to CSV files if each given dataframe has new entires from the most recent scrape.

    Args:
        contract_df, grant_df, property_df: pandas DataFrames of scraped, updated DOGE-cut federal programs.
        new_data_stats: dict of new entry counts for contracts, grants and leases.
        data_dir: directory where new data files are saved from input *_df files.

    Returns:
        new_files: list of file names for each file saved to data_dir. Default: "./tmp_data"
    """
    dt_str = datetime.now().isoformat(timespec='seconds').replace(':','')
    new_files = []

    if os.path.exists(data_dir):
        pass
    else:
        os.makedirs(data_dir)

    if new_data_stats["contract"] > 0:
        contract_filename = f"doge-contract_{dt_str}.csv"
        contract_df.to_csv(os.path.join(data_dir, contract_filename),index=False)
        new_files.append(contract_filename)

    if new_data_stats["grant"] > 0:
        grant_filename = f"doge-grant_{dt_str}.csv"
        grant_df.to_csv(os.path.join(data_dir, grant_filename),index=False)
        new_files.append(grant_filename)

    if new_data_stats["property"] > 0:
        property_filename = f"doge-property_{dt_str}.csv"
        property_df.to_csv(os.path.join(data_dir, property_filename),index=False)
        new_files.append(property_filename)

    return new_files


def delete_tmp_data(files_to_delete,tmp_dir="./tmp_data"):
    """
    Delete specified files from a specified directory.

    Args:
        files_to_delete: list of file name strings for all files to delete.
        tmp_dir: data directory where files in `files_to_delete` are located.
    """
    for file in files_to_delete:
        if os.path.exists(os.path.join(tmp_dir,file)):
            logger.info(f"Removing temporary file: {file}")
            os.remove(os.path.join(tmp_dir,file))
        else:
            logger.info(f"Attempt to delete temorary file: {file} failed, file not found")


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
    
    newscrape=True

    # NEW INTEGRATED SCRAPING CODE, crawls and uploads instead of dl github files to sync
    bln_project_files = list_new_bln_project_files(bln_client, bln_project_id)
    logger.info(f"Most recent files found in BLN project: {bln_project_files}")
    
    logger.info("Crawling doge.gov/savings for new entries:")
    contract_df, grant_df, property_df, new_data_stats = update_doge_data(bln_client, bln_project_id, bln_project_files)

    # save new scrape
    logger.info("Saving local copies of scraped dataframes:")
    new_scrape_files = save_doge_data_bln(contract_df, grant_df, property_df, new_data_stats)

    if new_scrape_files:
        logger.info(f"New files saved, uploading to BLN project: {new_scrape_files}")
        uploads = copy_scrape_files_to_bln(
            scrape_files=new_scrape_files,
            client=bln_client,
            project_id=bln_project_id,
        )
        logger.info(f"Files successfully uploaded to BLN project")

    # delete tmp pre-scrape files
    delete_tmp_data([*bln_project_files, *new_scrape_files])
    # delete_tmp_data(new_scrape_files) # TODO: add this? Possibly remake using `with tempfile.TemporaryDirectory() as tmpdir:`

    if uploads["success"]:
        success_uploads = uploads["success"]
        file_upload_message = f"{len(success_uploads)} new/updated file(s) uploaded to BLN project ({', '.join(success_uploads)})"
        outcome = "success"
    if uploads["failure"]:
        failure_uploads = uploads["failure"]
        file_upload_message = f"{file_upload_message}. {len(failure_uploads)} new/updated file(s) failed to upload to BLN project ({', '.join(failure_uploads)})"
        outcome = "error"

    else:
        file_upload_message = f"No new files found."
        outcome = "success"

    logger.info("Deleting temporary data directory: './tmp_data/'")
    os.removedirs("./tmp_data")

    base_final_message = "Process complete."
    final_message = f"{base_final_message} {file_upload_message}"

    logger.info(final_message)
    SLACK_BOT_INTERNAL_ALERTER.post(final_message, outcome)
