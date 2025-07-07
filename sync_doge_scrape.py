import logging
from typing import List, Dict, Optional
import os
from bln import Client
from collections import defaultdict
from helpers import list_github_dir, get_last_commit_dates, list_bln_project_files

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
        comparison_name = f"{name}_{timestamp}"
        enriched[comparison_name] = {
            'path': path,
            'download_url': download_url,
            'timestamp': timestamp,
            'name' : name
        }
    return enriched


def run_pipeline(environment):
    bln_api_key = os.environ.get("BLN_API_TOKEN")
    bln_project_id = os.environ.get("BLN_PROJECT_ID")
    bln_client = Client(bln_api_key)
    logger.info(f"Running: {environment}")
    source_repo = "doge-scrape"
    source_repo_owner = 'm-nolan'
    github_files = get_files_with_last_modified(
        owner=source_repo_owner,
        repo=source_repo,
        path='data'
    )

    logger.info(f"Files found in {source_repo} github: {github_files}" )

    bln_project_files = list_bln_project_files(bln_client, bln_project_id)
    logger.info(f"Files found in BLN project ({bln_project_id}): {bln_project_files}" )

