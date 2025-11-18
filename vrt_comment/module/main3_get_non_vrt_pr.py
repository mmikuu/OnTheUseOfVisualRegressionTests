import os
import time
from datetime import datetime
import requests
import csv
from requests.exceptions import ChunkedEncodingError

# --- 定数 ---
# ★★★★★ ご自身のGitHubトークンに書き換えてください ★★★★★
GITHUB_TOKEN = 'xxx'
GRAPHQL_URL = 'https://api.github.com/graphql'
DATE_SETTINGS_FILE = '../settings.txt'

# --- GraphQLクエリ ---
QUERY_TEMPLATE = '''
query ($cursor: String, $searchQuery: String!) {
  search(query: $searchQuery, type: ISSUE, first: 30, after: $cursor) {
    edges {
      node {
        ... on PullRequest {
          title
          url
          body
          createdAt
          closedAt
          repository {
            name
          }
          comments {
            totalCount
          }
          reviewThreads {
            totalCount
          }
          commits {
            totalCount
          }
          state
          author {
            login
            __typename
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

# --- 関数定義 ---

def run_query(query, variables, max_retries=3):
    """GraphQLクエリを実行し、リトライ処理も行う"""
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
    attempt = 0
    while attempt < max_retries:
        try:
            response = requests.post(
                GRAPHQL_URL,
                json={'query': query, 'variables': variables},
                headers=headers,
                timeout=200,
                stream=True
            )
            if response.status_code == 502:
                raise requests.exceptions.HTTPError("502 Server Error")
            response.raise_for_status()
            return response.json()
        except (ChunkedEncodingError, requests.exceptions.HTTPError) as e:
            print(f"Error occurred: {e}. Retrying {attempt + 1}/{max_retries}...")
            attempt += 1
            time.sleep(min(60, 2 ** attempt))
        except requests.exceptions.RequestException as e:
            print(f"Request failed with error: {e}")
            raise
    raise Exception("Max retries reached. Query failed.")

def fetch_pull_requests_from_repo(repo_name, start_date_str, end_date_str, pull_numbers_to_exclude):
    """指定されたリポジトリと期間からPRをフェッチし、条件でフィルタリングする"""
    all_items = []
    cursor = None
    has_next_page = True
    total_fetched_this_call = 0
    search_query = f"repo:{repo_name} is:pr is:closed created:{start_date_str}..{end_date_str}"
    print(f"Constructed search query: {search_query}")

    while has_next_page:
        variables = {"cursor": cursor, "searchQuery": search_query}
        result = run_query(QUERY_TEMPLATE, variables)
        if 'errors' in result:
            raise Exception(f"Query failed with errors: {result['errors']}")
        if 'data' not in result or 'search' not in result['data'] or result['data']['search'] is None:
            print(f"Warning: No 'search' data in result for query: {search_query} with cursor: {cursor}")
            break

        search_results = result['data']['search']['edges']
        for edge in search_results:
            item = edge['node']
            if not item: continue

            pr_number_str = item.get('url', '').split('/')[-1]
            author_info = item.get('author', {})
            print(f"Processing PR URL: {item.get('url', '')}, PR Number: {pr_number_str}")

            if author_info and author_info.get('__typename') == 'Bot':
                print(f"Skipping PR #{pr_number_str} by Bot {author_info.get('login')}.")
                continue

            # --- ★ここからが変更箇所★ ---

            def contains_image(text):
                """テキストに画像マークダウンまたはimgタグが含まれているかチェック"""
                if not text:
                    return False
                # マークダウン形式の画像: ![alt](src) または HTML形式の画像: <img
                return ("![" in text and "](" in text) or "<img" in text

            # 1. PRのbody (説明文) をチェック
            pr_body = item.get('body') or ""
            has_image_in_body = contains_image(pr_body)

            # 2. PRのコメント (Issue Comments) をチェック
            has_image_in_comments = False
            comment_edges = item.get('comments', {}).get('edges', [])
            for comment_edge in comment_edges:
                comment_body = comment_edge.get('node', {}).get('body')
                if contains_image(comment_body):
                    has_image_in_comments = True
                    break  # 1つでも見つかればチェック終了

            # 3. PRのレビューコメント (Review Comments) をチェック
            has_image_in_review_comments = False
            if not has_image_in_body and not has_image_in_comments: # まだ画像が見つかっていなければ
                review_thread_edges = item.get('reviewThreads', {}).get('edges', [])
                for thread_edge in review_thread_edges:
                    thread_comments = thread_edge.get('node', {}).get('comments', {}).get('edges', [])
                    for review_comment_edge in thread_comments:
                        review_comment_body = review_comment_edge.get('node', {}).get('body')
                        if contains_image(review_comment_body):
                            has_image_in_review_comments = True
                            break  # スレッド内のループを終了
                    if has_image_in_review_comments:
                        break  # 全スレッドのループを終了

            # 最終的な画像有無の判定 (body, comments, reviewComments のいずれか)
            has_image = has_image_in_body or has_image_in_comments or has_image_in_review_comments
            
            # --- ★変更箇所ここまで★ ---

            is_excluded = pr_number_str in pull_numbers_to_exclude

            if has_image and not is_excluded:
                print(f"PR #{pr_number_str} has an image (in body or comments) AND is not excluded. ADDING to results.")
                all_items.append(item)
            else:
                reason = []
                if not has_image: reason.append("does not contain an image in body or comments")
                if is_excluded: reason.append("is in the exclusion list")
                print(f"Skipping PR #{pr_number_str} because it {', '.join(reason)}.")
            
            total_fetched_this_call += 1

        page_info = result['data']['search']['pageInfo']
        has_next_page = page_info['hasNextPage']
        cursor = page_info['endCursor']
        print(f"Fetched {len(search_results)} items on this page for {repo_name}. Total for this call so far: {total_fetched_this_call}")
        if has_next_page: print("Fetching next page...")

    return all_items


def save_to_csv(items, repo_name):
    """収集したデータをCSVファイルに保存する"""
    directory = '../../data/visual_prs_not_in_vrt_in_comments'
    if not os.path.exists(directory):
        os.makedirs(directory)
    file_path = os.path.join(directory, f'pr_details_{repo_name.replace("/", "_")}.csv')

    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['repo_name', 'pr_title', 'pr_url', 'created_at', 'closed_at', 'total_comments', 'total_commits', 'state']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for item in items:
            total_comments = item.get('comments', {}).get('totalCount', 0) + item.get('reviewThreads', {}).get('totalCount', 0)
            total_commits = item.get('commits', {}).get('totalCount', 0)
            writer.writerow({
                'repo_name': item.get('repository', {}).get('name', 'N/A'),
                'pr_title': item.get('title', 'N/A'),
                'pr_url': item.get('url', 'N/A'),
                'created_at': item.get('createdAt', 'N/A'),
                'closed_at': item.get('closedAt', 'N/A'),
                'total_comments': total_comments,
                'total_commits': total_commits,
                'state': item.get('state', 'N/A')
            })
    print(f"Data for {repo_name} saved to {file_path}")

def get_repositories_from_csv(csv_files):
    """複数のCSVファイルから除外リストを読み込み、1つの辞書に統合する"""
    repo_info = {}
    for csv_file in csv_files:
        try:
            with open(csv_file, 'r', encoding='utf-8') as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    repo_name = row.get('repository_name') or row.get('repositoryname')
                    if not repo_name:
                        print(f"Warning: Missing repository name in a row in {csv_file}. Skipping row.")
                        continue
                    
                    pull_numbers_str = row.get('pull_numbers', '')
                    pull_numbers = set(filter(None, pull_numbers_str.split(',')))
                    
                    if repo_name in repo_info:
                        repo_info[repo_name].update(pull_numbers)
                    else:
                        repo_info[repo_name] = pull_numbers
        except FileNotFoundError:
            print(f"Error: Repositories CSV file '{csv_file}' not found. Skipping this file.")
            continue
        except Exception as e:
            print(f"Error reading repositories CSV file '{csv_file}': {e}. Skipping this file.")
            continue
    return repo_info

def load_date_ranges_from_file(filepath):
    """settings.txtから期間設定を読み込む"""
    date_ranges = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line: continue
                parts = line.split(',')
                if len(parts) != 2:
                    print(f"Warning: Invalid format in {filepath} at line {line_num}: '{line}'. Skipping.")
                    continue
                start_str, end_str = parts[0].strip(), parts[1].strip()
                try:
                    s_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                    e_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                    if s_dt > e_dt:
                        print(f"Warning: Start date is after end date in {filepath} at line {line_num}: '{line}'. Skipping.")
                        continue
                    date_ranges.append((start_str, end_str))
                except ValueError:
                    print(f"Warning: Invalid date format in {filepath} at line {line_num}: '{line}'. Skipping.")
                    continue
    except FileNotFoundError:
        print(f"Error: Date settings file '{filepath}' not found. Exiting.")
        exit(1)
    except Exception as e:
        print(f"Error reading date settings file '{filepath}': {e}. Exiting.")
        exit(1)
    return date_ranges

# --- メイン実行ブロック ---

if __name__ == '__main__':
    # MergedとClosedの除外リストファイルを指定
    files_to_process = [
        '../../data/unique-vrt-comments-without-open-saner.csv',
    ]
    
    repo_info_data = get_repositories_from_csv(files_to_process)
    date_periods = load_date_ranges_from_file(DATE_SETTINGS_FILE)

    if not date_periods:
        print(f"No date periods loaded from '{DATE_SETTINGS_FILE}'. Exiting.")
    elif not repo_info_data:
        print("No repository information loaded from source CSVs. Exiting.")
    else:
        print(f"Loaded {len(date_periods)} date period(s) from '{DATE_SETTINGS_FILE}'.")
        print(f"Loaded exclusion data for {len(repo_info_data)} repositories.")

        for repo_name_key, pull_numbers_to_exclude_set in repo_info_data.items():
            all_items_for_this_repo = []
            print(f"\nProcessing repository: {repo_name_key}")

            for i, (period_start, period_end) in enumerate(date_periods):
                print(f"Fetching PRs for period {i + 1}/{len(date_periods)}: {period_start} to {period_end}")
                try:
                    items_from_this_period = fetch_pull_requests_from_repo(
                        repo_name_key, 
                        period_start, 
                        period_end, 
                        pull_numbers_to_exclude_set
                    )
                    if items_from_this_period:
                        all_items_for_this_repo.extend(items_from_this_period)
                    print(f"Fetched {len(items_from_this_period)} items for {repo_name_key} in this period. Total for this repo so far: {len(all_items_for_this_repo)}")
                except Exception as e:
                    print(f"Error fetching data for repository {repo_name_key}, period {period_start}-{period_end}: {e}")

            if all_items_for_this_repo:
                save_to_csv(all_items_for_this_repo, repo_name_key)
            else:
                print(f"No items found or fetched for repository {repo_name_key} across all configured periods.")

        print("\nProcessing complete.")