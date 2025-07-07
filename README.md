# Sync Doge Scrape

Looks for new data in https://github.com/m-nolan/doge-scrape/tree/main/data and uploads anything missing to Big Local News project.


## 🚀 Getting Started

### 1. Clone the repository

```bash
git clone git@github.com:biglocalnews/sync-doge-scrape.git
cd sync-doge-scrape
```

### 2. Set up your environment files

- Copy the example files and fill in your credentials:

    ```bash
    cp .env.test.example .env.test
    cp .env.prod.example .env.prod
    ```

- Fill in values for all variables in .env files 


### 3. Create a virtual environment and install dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 🏃 Running the Script

Run locally with `test` as the argument. Running with `prod` will publish updates to real alert channels for external users - this should be reserved for use in production.

```bash
python run.py test
```

### Behavior:

- Loads environment-specific variables from `.env.test` or `.env.prod`
- Downloads the latest OSHA fatalities page
- Saves a snapshot of the raw HTML to `snapshots/snapshot_{YYYY-MM-DDTmmss}.html`
- Parses fatality records from the page
- Downloads the current master dataset from BLN
- Compares new data to existing records
- Identifies new records and saves to `additions.csv`
- Posts new entries to Slack (using environment-specific webhook bots)
- Updates the master dataset and logs the result
- Uploads a monthly backup to BLN and deletes old ones


## 🧪 Test Mode Behavior

When run with `test`, the script:

- Simulates new additions (e.g., deletes some old records and overrides timestamps)
- Posts Slack alerts for new records to: `#newstips-test`
- Sends error and status lerts to: `#alerts-data-etl-test`

## ✅ Requirements

- Python 3.8+
- Valid BLN API token wich account access to both (test and prod) BLN osha-fatalities projects
- Slack credentials for alerts
