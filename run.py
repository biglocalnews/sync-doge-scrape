import datetime
import logging

from helpers import set_environment
from sync_doge_scrape import run_pipeline

"""
Sync Doge Scrape

Looks for new data in https://github.com/m-nolan/doge-scraper/data and
uploads anything missing to Big Local News project.
"""

logging.basicConfig(
    format="\n%(asctime)s %(levelname)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.debug(f"{datetime.datetime.now()}: Running sync-doge-scrape...")
    environment = set_environment()
    run_pipeline(environment)
