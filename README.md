# Sync Doge Scrape

Looks for new data in https://github.com/m-nolan/doge-scrape/tree/main/data and uploads anything missing to Big Local News project.


## 🚀 Getting Started

## Requirements

- Python 3.8+
- Valid BLN API token wich account access to both BLN `DOGE claim archive projects` ([test](https://biglocalnews.org/#/project/UHJvamVjdDowMjZlMzQzMi04MTZhLTRiYzAtOTY0NS0yMDJkZmU3ZTJiNDM=), [prod](https://biglocalnews.org/#/project/UHJvamVjdDo2NzkxYTJmNi0wNTNmLTQzMTEtYjE5Yy03MTc3MzFmMGUwZDY=).
- Slack credentials for alerts

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
- Fetches the list of files in the source GitHub repo (with last-modified timestamps).
- Fetches the list of current files in the target BLN project.
- Compares the two to determine which GitHub files are new.
- Downloads and uploads new files to the BLN project.


## 🧪 Test Mode Behavior

(Scrappy method for now)

If you want to test the script, go into the test BLN project and delete the most recent files before running the command above.

