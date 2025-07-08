from sync_doge_scrape import run_pipeline
from helpers import set_environment
import logging

"""
Sync Doge Scrape

Looks for new data in https://github.com/m-nolan/doge-scraper/data and
uploads anything missing to Big Local News project.
"""

logging.basicConfig(
    format="\n%(asctime)s %(levelname)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%I:%M:%S",
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    print("Running sync doge scrape...")
    environment = set_environment()
    run_pipeline(environment)
