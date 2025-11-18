import pandas as pd
import requests
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor

INPUT_CSV = '../../data/visual/visual-prs-merged-in-range-saner.csv'
OUTPUT_CSV = '../../data/visual/visual-prs-merged-saner-with-metrices.csv'
URL_COLUMN = 'pr_url'  


GITHUB_TOKEN = "xxx"
GITHUB_GRAPHQL_URL = "https://api.github.com/graphql"

REPO_PULL_PATTERN = re.compile(r"https://github\.com/([^/]+)/([^/]+)/pull/(\d+)")



def get_pr_details_query(owner, repo, pull_number):
    return {
        "query": f"""
        query {{
          repository(owner: "{owner}", name: "{repo}") {{
            pullRequest(number: {pull_number}) {{
              additions
              deletions
              changedFiles
              comments {{
                totalCount
              }}
              commits {{
                totalCount
              }}
            }}
          }}
        }}
        """
    }


def fetch_pr_metrics(row):

    pr_url = row.get(URL_COLUMN)

    match = REPO_PULL_PATTERN.match(str(pr_url))
    if not match:
        row['fetch_status'] = 'Error: Invalid PR URL'
        return row

    owner, repo, pull_number = match.groups()
    pull_number = int(pull_number)
    headers = {
        "Authorization": f"bearer {GITHUB_TOKEN}",
        "Content-Type": "application/json"
    }
    query = get_pr_details_query(owner, repo, pull_number)

    try:
        response = requests.post(GITHUB_GRAPHQL_URL, headers=headers, json=query, timeout=10)
        response.raise_for_status()  
        data = response.json()


        if 'errors' in data:
            row['fetch_status'] = f"GraphQL Error: {data['errors'][0]['message']}"
            return row

        pr_data = data.get('data', {}).get('repository', {}).get('pullRequest')

        if pr_data:
            row['addline'] = pr_data.get('additions')
            row['deleteline'] = pr_data.get('deletions')
            row['changefile'] = pr_data.get('changedFiles')
            row['total_comments'] = pr_data.get('comments', {}).get('totalCount')
            row['total_commits'] = pr_data.get('commits', {}).get('totalCount')
            row['fetch_status'] = 'Success'
        else:
            row['fetch_status'] = 'Error: PR data not found (may be closed/merged PR or bad query)'

    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            row['fetch_status'] = 'HTTP Error 404: Repository or PR not found'
        elif response.status_code == 401:
            row['fetch_status'] = 'HTTP Error 401: Invalid Token'
        elif response.status_code == 403:
            reset_time = response.headers.get('x-ratelimit-reset')
            wait_time = int(reset_time) - int(time.time()) + 5 if reset_time else 60
            row['fetch_status'] = f'HTTP Error 403: Rate Limit. Wait {wait_time}s.'
            time.sleep(wait_time) 
        else:
            row['fetch_status'] = f'HTTP Error: {e}'
    except requests.exceptions.RequestException as e:
        row['fetch_status'] = f'Request Error: {e}'

    return row


if __name__ == "__main__":

    try:
        df = pd.read_csv(INPUT_CSV)
    except FileNotFoundError:
        print(f"Error: can not find {INPUT_CSV}")
        exit(1)

    if URL_COLUMN not in df.columns:
        print(f"Error: can not find '{URL_COLUMN}' column in the input CSV.")
        exit(1)

    print(f" total pr : {len(df)} ")

    MAX_THREADS = 8

    rows_to_process = df.to_dict('records')

    updated_rows = []

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        results = list(executor.map(fetch_pr_metrics, rows_to_process))
        updated_rows.extend(results)

    df_output = pd.DataFrame(updated_rows)

    if 'addline' not in df_output.columns:
        df_output['addline'] = None
    if 'deleteline' not in df_output.columns:
        df_output['deleteline'] = None
    if 'changefile' not in df_output.columns:
        df_output['changefile'] = None
    if 'total_comments' not in df_output.columns:
        df_output['total_comments'] = None
    if 'total_commits' not in df_output.columns:
        df_output['total_commits'] = None

    success_count = (df_output['fetch_status'] == 'Success').sum()
    error_count = len(df_output) - success_count

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    df_output.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')

    print("----------------")
    print(f"OUTPUT file : '{OUTPUT_CSV}'")
    print("----------------")