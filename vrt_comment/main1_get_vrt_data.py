from time import sleep
import requests
import csv
from datetime import datetime, timedelta
import re 


GITHUB_TOKEN = 'your_token'  

GRAPHQL_URL = 'https://api.github.com/graphql'
REQUEST_TIMEOUT_SECONDS = 300
API_CALL_DELAY_SECONDS = 10
PR_DETAILS_API_CALL_DELAY_SECONDS = 1
OUTPUT_CSV_FILENAME = '../../data/list-vrt-comments-1.csv' 
SEARCH_KEYWORD_IN_COMMENTS = "www.chromatic.com/test?"  
MAX_ITEMS_PER_FETCH_CYCLE = 1000
DATE_SETTINGS_FILE = 'settings.txt'


MAIN_SEARCH_QUERY_TEMPLATE = '''
query ($cursor: String, $searchQuery: String!) {
  search(query: $searchQuery, type: ISSUE, first: 20, after: $cursor) {
    edges {
      node {
        ... on PullRequest {
          title
          url
          createdAt
          closedAt
          state
          comments(first: 50) {
            totalCount
            nodes { body, url, author { login, __typename }, createdAt }
          }
          reviewThreads(first: 30) {
            nodes {
              comments(first: 50) {
                totalCount
                nodes { body, url, author { login, __typename }, createdAt }
              }
            }
          }
          commits(first: 100) {
            totalCount
            nodes {
              commit { committedDate }
            }
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
'''

PR_FILES_DETAIL_QUERY = """
query GetPullRequestFileDetails($owner: String!, $repo: String!, $prNumber: Int!, $filesCursor: String) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $prNumber) {
      changedFiles
      additions
      deletions
      files(first: 100, after: $filesCursor) {
        totalCount
        nodes { path, changeType }
        pageInfo { endCursor, hasNextPage }
      }
    }
  }
}
"""


def run_graphql_query(query, variables):
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
    if GITHUB_TOKEN == 'YOUR_GITHUB_PERSONAL_ACCESS_TOKEN_HERE' or not GITHUB_TOKEN:  # GITHUB_TOKENのチェックを強化
        raise ValueError(
            "GitHub Personal Access Token (GITHUB_TOKEN) is not set or is a placeholder. Please update it.")
    try:
        response = requests.post(GRAPHQL_URL, json={'query': query, 'variables': variables}, headers=headers,
                                 timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise Exception(f"GraphQL query failed: {e}") from e
    except ValueError as e:
        raise Exception(f"Failed to decode JSON response: {e}. Response text: {response.text}")


def get_pr_file_stats_and_changes(owner, repo, pr_number):
    all_file_changes_list = []
    files_cursor = None
    has_next_files_page = True
    pr_file_stats = {'changefile': None, 'addline': None, 'deleteline': None, 'fileChanges': None}
    fetched_top_level_stats = False
    max_retries = 3
    current_retry = 0

    print(f"Fetching file details for PR: {owner}/{repo}#{pr_number}...")
    while current_retry < max_retries:
        try:
            
            files_cursor_for_this_attempt = files_cursor
            has_next_files_page_for_this_attempt = has_next_files_page

            if current_retry > 0 and files_cursor is None:
                all_file_changes_list = []
                fetched_top_level_stats = False

            while has_next_files_page_for_this_attempt:
                variables = {"owner": owner, "repo": repo, "prNumber": int(pr_number),
                             "filesCursor": files_cursor_for_this_attempt}
                result = run_graphql_query(PR_FILES_DETAIL_QUERY, variables)

                if 'errors' in result:
                    is_rate_limited = any(
                        'type' in error and error['type'] == 'RATE_LIMITED' for error in result.get('errors', []))
                    if is_rate_limited:
                        current_retry += 1
                        if current_retry < max_retries:
                            wait_time = 60 * current_retry
                            print(
                                f"RATE LIMITED (GraphQL error) for PR files {owner}/{repo}#{pr_number}. Retrying in {wait_time}s... ({max_retries - current_retry} retries left)")
                            sleep(wait_time)
                            files_cursor = files_cursor_for_this_attempt
                            has_next_files_page = has_next_files_page_for_this_attempt
                            continue
                        else:
                            print(
                                f"GraphQL Error (Rate Limit) fetching PR files for {owner}/{repo}#{pr_number} after {max_retries} attempts: {result['errors']}")
                            return {'error': f"GraphQL Rate Limit Error: {result['errors']}"}
                    else:
                        print(f"GraphQL Error fetching PR files for {owner}/{repo}#{pr_number}: {result['errors']}")
                        return {'error': f"GraphQL Error: {result['errors']}"}

                if not result.get('data') or not result['data'].get('repository') or not result['data'][
                    'repository'].get('pullRequest'):
                    print(f"PR file details not found or issue with data structure for {owner}/{repo}#{pr_number}.")
                    return {'error': "PR file details not found or data structure issue."}

                pr_data = result['data']['repository']['pullRequest']
                if not fetched_top_level_stats:
                    pr_file_stats['changefile'] = pr_data.get('changedFiles')
                    pr_file_stats['addline'] = pr_data.get('additions')
                    pr_file_stats['deleteline'] = pr_data.get('deletions')
                    fetched_top_level_stats = True

                files_info = pr_data.get('files', {})
                for file_node in files_info.get('nodes', []):
                    if file_node and 'path' in file_node and 'changeType' in file_node:
                        all_file_changes_list.append(f"{file_node['changeType']}:{file_node['path']}")

                page_info = files_info.get('pageInfo', {})
                has_next_files_page_for_this_attempt = page_info.get('hasNextPage', False)
                files_cursor_for_this_attempt = page_info.get(
                    'endCursor') if has_next_files_page_for_this_attempt else None

                has_next_files_page = has_next_files_page_for_this_attempt
                files_cursor = files_cursor_for_this_attempt

                if has_next_files_page_for_this_attempt:
                    sleep(PR_DETAILS_API_CALL_DELAY_SECONDS / 2)

            pr_file_stats['fileChanges'] = "\n".join(all_file_changes_list) if all_file_changes_list else None
            return pr_file_stats

        except Exception as e:
            current_retry += 1
            if current_retry < max_retries:
                wait_time = 30 * current_retry
                print(
                    f"Error fetching PR files for {owner}/{repo}#{pr_number}: {e}. Retrying in {wait_time}s... ({max_retries - current_retry} retries left)")
                sleep(wait_time)
                files_cursor = None
                has_next_files_page = True
                all_file_changes_list = []
                fetched_top_level_stats = False
            else:
                print(f"Failed to fetch PR files for {owner}/{repo}#{pr_number} after {max_retries} retries: {e}")
                return {'error': str(e)}
    return {'error': f"Exhausted retries for PR file details {owner}/{repo}#{pr_number}"}


def parse_pr_url(pr_url_str):
    if not pr_url_str: return None, None, None
    match = re.match(r"https://github\.com/([^/]+)/([^/]+)/pull/(\d+)", pr_url_str)
    if match:
        return match.group(1), match.group(2), int(match.group(3))
    return None, None, None


def fetch_items_main_search(from_date_str, to_date_str):
    all_pr_items_from_search = []
    cursor = None
    has_next_page = True

    while has_next_page:

        search_query = f"{SEARCH_KEYWORD_IN_COMMENTS} in:comments,body is:pr created:{from_date_str}..{to_date_str} closed:{from_date_str}..{to_date_str}"

        variables = {"cursor": cursor, "searchQuery": search_query}
        try:
            result = run_graphql_query(MAIN_SEARCH_QUERY_TEMPLATE, variables)
        except Exception as e:
            print(f"Error during main search query for period {from_date_str}-{to_date_str}: {e}")
            break

        if 'errors' in result:
            print(f"Main search query for {from_date_str}-{to_date_str} failed: {result['errors']}")
            break
        if 'data' not in result or 'search' not in result['data']:
            print(f"Unexpected API response for main search {from_date_str}-{to_date_str}: {result}")
            break

        search_results = result['data']['search']['edges']
        current_page_pr_count = 0
        for edge in search_results:
            if edge and edge.get('node'):
                all_pr_items_from_search.append(edge['node'])
                current_page_pr_count += 1

        print(
            f"Fetched {current_page_pr_count} PRs on this page for period {from_date_str}-{to_date_str} (created and closed). Total accumulated: {len(all_pr_items_from_search)}")

        if len(all_pr_items_from_search) >= MAX_ITEMS_PER_FETCH_CYCLE:
            print(
                f"Reached or exceeded MAX_ITEMS_PER_FETCH_CYCLE ({MAX_ITEMS_PER_FETCH_CYCLE}) for this date range. Stopping fetch for this period.")
            has_next_page = False

        if has_next_page:
            page_info = result['data']['search']['pageInfo']
            has_next_page = page_info.get('hasNextPage', False)
            cursor = page_info.get('endCursor')

        if has_next_page:
            print(f"More PRs to fetch for {from_date_str}-{to_date_str}... Sleeping for {API_CALL_DELAY_SECONDS}s")
            sleep(API_CALL_DELAY_SECONDS)
    return all_pr_items_from_search


def count_commits_since_comment_time(comment_created_at_str, commit_nodes):
    if not comment_created_at_str: return 0
    try:
        comment_time = datetime.fromisoformat(comment_created_at_str.replace("Z", "+00:00"))
    except ValueError:
        return 0
    count = 0
    for commit_node in commit_nodes:
        committed_date_str = commit_node.get('commit', {}).get('committedDate')
        if committed_date_str:
            try:
                commit_time = datetime.fromisoformat(committed_date_str.replace("Z", "+00:00"))
                if commit_time > comment_time:
                    count += 1
            except ValueError:
                continue
    return count


def save_data_to_csv(pr_list_from_search):
    fieldnames = [
        'pr_title', 'text', 'url', 'comment_index', 'commit_count_since_comment',
        'total_comments', 'total_commits', 'comment_count_since_comment',
        'created_at', 'closed_at', 'state',
        'changefile', 'addline', 'deleteline', 'fileChanges'
    ]
    file_stats_cache = {}
    with open(OUTPUT_CSV_FILENAME, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        processed_pr_urls_for_logging = set()

        for pr_item_node in pr_list_from_search:
            pr_title = pr_item_node.get('title', 'N/A')
            pr_url_str = pr_item_node.get('url')
            pr_created_at = pr_item_node.get('createdAt')
            pr_closed_at = pr_item_node.get('closedAt')
            pr_state = pr_item_node.get('state')

            commits_data = pr_item_node.get('commits', {})
            total_commits_val = commits_data.get('totalCount', 0)
            pr_commit_nodes = commits_data.get('nodes', [])

            total_pr_direct_comments = pr_item_node.get('comments', {}).get('totalCount', 0)
            total_review_thread_comments = 0
            for review_thread in pr_item_node.get('reviewThreads', {}).get('nodes', []):
                total_review_thread_comments += review_thread.get('comments', {}).get('totalCount', 0)
            total_comments_val = total_pr_direct_comments + total_review_thread_comments

            file_stats_data = {'changefile': None, 'addline': None, 'deleteline': None, 'fileChanges': None}
            if pr_url_str:
                if pr_url_str in file_stats_cache:
                    file_stats_data = file_stats_cache[pr_url_str]
                    if pr_url_str not in processed_pr_urls_for_logging:
                        print(f"Using cached file stats for PR: {pr_url_str}")
                        processed_pr_urls_for_logging.add(pr_url_str)
                else:
                    owner, repo, pr_number = parse_pr_url(pr_url_str)
                    if owner and repo and pr_number:
                        fetched_stats = get_pr_file_stats_and_changes(owner, repo, pr_number)
                        if 'error' not in fetched_stats:
                            file_stats_data = fetched_stats
                        else:
                            print(f"Error fetching file stats for PR {pr_url_str}: {fetched_stats['error']}")
                        file_stats_cache[pr_url_str] = file_stats_data
                        processed_pr_urls_for_logging.add(pr_url_str)
                        sleep(PR_DETAILS_API_CALL_DELAY_SECONDS)
                    else:
                        print(f"Could not parse URL for file stats: {pr_url_str}")
                        file_stats_cache[pr_url_str] = file_stats_data

            all_pr_comments_list = []
            for comment_node in pr_item_node.get('comments', {}).get('nodes', []):
                all_pr_comments_list.append(comment_node)
            for review_thread in pr_item_node.get('reviewThreads', {}).get('nodes', []):
                for review_comment_node in review_thread.get('comments', {}).get('nodes', []):
                    all_pr_comments_list.append(review_comment_node)
            all_pr_comments_list.sort(key=lambda c: c.get('createdAt', ''))

            non_bot_comment_serial_in_pr = 0
            for comment_detail in all_pr_comments_list:
                comment_body = comment_detail.get('body', '')
                comment_url = comment_detail.get('url')
                comment_created_at = comment_detail.get('createdAt')
                author_info = comment_detail.get('author')
                is_comment_by_bot = bool(author_info and author_info.get('__typename') == 'Bot')
                current_comment_index_val = -1
                if not is_comment_by_bot:
                    non_bot_comment_serial_in_pr += 1
                    current_comment_index_val = non_bot_comment_serial_in_pr

                if SEARCH_KEYWORD_IN_COMMENTS in comment_body and not is_comment_by_bot:
                    commit_count_val = count_commits_since_comment_time(comment_created_at, pr_commit_nodes)
                    row_data = {
                        'pr_title': pr_title,
                        'text': comment_body,
                        'url': comment_url,
                        'comment_index': current_comment_index_val,
                        'commit_count_since_comment': commit_count_val,
                        'total_comments': total_comments_val,
                        'total_commits': total_commits_val,
                        'comment_count_since_comment': commit_count_val,
                        'created_at': pr_created_at,
                        'closed_at': pr_closed_at,
                        'state': pr_state,
                        'changefile': file_stats_data.get('changefile'),
                        'addline': file_stats_data.get('addline'),
                        'deleteline': file_stats_data.get('deleteline'),
                        'fileChanges': file_stats_data.get('fileChanges')
                    }
                    writer.writerow(row_data)


def load_date_ranges_from_file(filepath):
    date_ranges = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'): continue
                parts = line.split(',')
                if len(parts) != 2:
                    print(f"W: Invalid format in {filepath} L{line_num}: '{line}'. Skip.")
                    continue
                start_str, end_str = parts[0].strip(), parts[1].strip()
                try:
                    s_dt = datetime.fromisoformat(start_str)
                    e_dt = datetime.fromisoformat(end_str)
                    if s_dt > e_dt:
                        print(f"W: Start > end in {filepath} L{line_num}: '{line}'. Skip.")
                        continue
                    date_ranges.append((start_str, end_str))
                except ValueError:
                    print(f"W: Invalid date fmt in {filepath} L{line_num}: '{line}'. Skip.")
                    continue
    except FileNotFoundError:
        print(f"E: File not found: {filepath}")
        exit(1) 
    except Exception as e:
        print(f"E: Reading {filepath}: {e}")
        exit(1)  
    return date_ranges


if __name__ == '__main__':

    all_pr_nodes_across_periods = []
    date_periods = load_date_ranges_from_file(DATE_SETTINGS_FILE)

    if not date_periods:
        print(f"No date periods loaded from '{DATE_SETTINGS_FILE}'. Exiting.")
    else:
        print(f"Loaded {len(date_periods)} date period(s) from '{DATE_SETTINGS_FILE}'.")
        for i, (from_d, to_d) in enumerate(date_periods):
            print(f"\n--- Main Search - Period {i + 1}/{len(date_periods)}: {from_d} to {to_d} ---")
            try:
                pr_nodes_from_period = fetch_items_main_search(from_d, to_d)
                all_pr_nodes_across_periods.extend(pr_nodes_from_period)
                print(
                    f"Fetched {len(pr_nodes_from_period)} PR items in this period. Total items so far: {len(all_pr_nodes_across_periods)}")
            except Exception as e:
                print(f"Critical error processing period {from_d} to {to_d} for main search: {e}")
                continue
            if len(date_periods) > 1 and i < len(date_periods) - 1:
                print(f"Sleeping for {API_CALL_DELAY_SECONDS * 2}s before next major period...")
                sleep(API_CALL_DELAY_SECONDS * 2)

        if all_pr_nodes_across_periods:
            unique_pr_items_map = {item['url']: item for item in all_pr_nodes_across_periods if item.get('url')}
            unique_pr_list = list(unique_pr_items_map.values())
            print(f"\nStarting CSV generation with {len(unique_pr_list)} unique PR items (deduplicated by URL).")
            save_data_to_csv(unique_pr_list)
            print(f"\nCSV file '{OUTPUT_CSV_FILENAME}' generated/updated.")
        else:
            print("\nNo PR data collected to generate CSV.")