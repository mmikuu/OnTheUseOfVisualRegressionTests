import csv
import collections
import random
import re
import os
import requests
import time
from datetime import datetime


INPUT_CSV = '../../data/unique-vrt-comments-closed.csv'
PULL_LIST_CSV = '../../data/list-vrt-comments.csv'
OUTPUT_CSV = '../../data/non_vrt/non-vrt-merged-closed-1.csv'
NON_CHROMATIC_DIR = "../../data/non-chromatic"

# GitHub GraphQL API Endpoint
GITHUB_API_URL = "https://api.github.com/graphql"
# GitHub Personal Access Token 
GITHUB_TOKEN = "your_token"  

TARGET_EXTENSIONS = [
    '.tsx', '.ts', '.json', '.js', '.md',
    '.scss', '.lock', '.yml', '.css', '.mdx'
]

# Regex pattern to extract GitHub repository and PR number
REPO_PULL_PATTERN = re.compile(r"https://github\.com/([^/]+)/([^/]+)/pull/(\d+)")
REPO_PATTERN = re.compile(r"https://github\.com/([^/]+)/([^/]+)")  # For extracting repository name only


def get_pr_modified_files(owner, repo, pull_number, token):
    """Fetches the list of modified files for a PR using GitHub GraphQL API."""
    if not token:
        print("Error: GITHUB_TOKEN is not set. Skipping API call.")
        return None

    all_files = []
    has_next_page = True
    cursor = None
    files_fetched_count = 0
    api_call_attempts = 0
    max_attempts = 3  

    while has_next_page and api_call_attempts < max_attempts:
        api_call_attempts += 1
        query = """
        query GetPullRequestFiles($owner: String!, $repo: String!, $pullNumber: Int!, $filesCursor: String) {
          repository(owner: $owner, name: $repo) {
            pullRequest(number: $pullNumber) {
              files(first: 100, after: $filesCursor) {
                pageInfo {
                  endCursor
                  hasNextPage
                }
                nodes {
                  path
                }
              }
            }
          }
          rateLimit {
            cost
            remaining
            resetAt
          }
        }
        """
        variables = {
            "owner": owner,
            "repo": repo,
            "pullNumber": pull_number,
            "filesCursor": cursor
        }
        headers = {
            "Authorization": f"bearer {token}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(GITHUB_API_URL, json={'query': query, 'variables': variables}, headers=headers)
            response.raise_for_status()
            data = response.json()

            if 'data' in data and data.get('rateLimit'):
                rate_limit = data['rateLimit']
                remaining = rate_limit.get('remaining')
                reset_at_str = rate_limit.get('resetAt')
                if remaining is not None and remaining < 30:
                    if reset_at_str:
                        reset_time = datetime.strptime(reset_at_str, "%Y-%m-%dT%H:%M:%SZ")
                        now_utc = datetime.utcnow()
                        wait_seconds = (reset_time - now_utc).total_seconds() + 20
                        if wait_seconds > 0:
                            print(
                                f"Approaching rate limit ({remaining} remaining), waiting for {wait_seconds:.1f} seconds...")
                            time.sleep(wait_seconds)
                    else:
                        print(f"Approaching rate limit ({remaining} remaining), reset time unknown. Waiting for 90 seconds...")
                        time.sleep(90)

            if 'errors' in data:
                print(f"GraphQL error ({owner}/{repo}# {pull_number}, attempt {api_call_attempts}): {data['errors']}")
                if api_call_attempts < max_attempts: time.sleep(5 * api_call_attempts)
                continue

            pr_data = data.get('data', {}).get('repository', {}).get('pullRequest')
            if not pr_data or 'files' not in pr_data:
                print(
                    f"Warning: Could not retrieve file list ({owner}/{repo}# {pull_number}, attempt {api_call_attempts}). PR may not exist or be accessible. Response: {data.get('data')}")
                return None

            files_data = pr_data['files']
            current_page_files = [node['path'] for node in files_data.get('nodes', []) if node and 'path' in node]
            all_files.extend(current_page_files)
            files_fetched_count += len(current_page_files)

            page_info = files_data.get('pageInfo', {})
            has_next_page = page_info.get('hasNextPage', False)
            cursor = page_info.get('endCursor')

            api_call_attempts = 0 

            if files_fetched_count > 1000 and has_next_page:
                print(f"Warning: PR {owner}/{repo}#{pull_number} has over 1000 modified files. Halting file fetching.")
                break

            if has_next_page:
                time.sleep(0.25)

        except requests.exceptions.HTTPError as http_err:
            print(
                f"HTTP error (get_pr_modified_files, {owner}/{repo}# {pull_number}, attempt {api_call_attempts}): {http_err} - {response.text}")
            if response.status_code == 401:
                print("Fatal Error: GitHub Token is invalid or lacks permissions. Terminating script.")
                raise SystemExit("Invalid GitHub Token.")
            if api_call_attempts < max_attempts:
                time.sleep(10 * api_call_attempts)
            else:
                return None
        except requests.exceptions.RequestException as req_err:
            print(
                f"Request error (get_pr_modified_files, {owner}/{repo}# {pull_number}, attempt {api_call_attempts}): {req_err}")
            if api_call_attempts < max_attempts:
                time.sleep(10 * api_call_attempts)
            else:
                return None
        except Exception as e:
            print(
                f"Unexpected error (get_pr_modified_files, {owner}/{repo}# {pull_number}, attempt {api_call_attempts}): {e}")
            if api_call_attempts < max_attempts:
                time.sleep(5)
            else:
                return None

    if api_call_attempts >= max_attempts and has_next_page:
        print(
            f"Warning: Max attempts reached for fetching file list for PR {owner}/{repo}#{pull_number}. Returning partial list ({len(all_files)} files).")

    return all_files


def contains_target_extension(modified_files_list):
    if not modified_files_list:
        return False
    for filename in modified_files_list:
        if not isinstance(filename, str):
            continue
        fn_lower = filename.lower()
        for ext in TARGET_EXTENSIONS:
            if fn_lower.endswith(ext):
                return True
    return False


print("Script start")
print(f"Step 1: Loading oldest PR creation dates per repository from {PULL_LIST_CSV}...")
repo_oldest_created_at = {}
try:
    with open(PULL_LIST_CSV, "r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        processed_rows = 0;
        skipped_url_format = 0;
        skipped_date_format = 0;
        skipped_no_url = 0;
        skipped_no_date = 0;
        skipped_not_merged = 0
        required_columns = ["url", "created_at", "state"]
        if not all(col in reader.fieldnames for col in required_columns):
            print(
                f"Error: Required column(s) {required_columns} missing in {PULL_LIST_CSV}. Columns found: {reader.fieldnames}");
            exit(1)
        for row in reader:
            processed_rows += 1;
            pr_url = row.get("url");
            created_at_str = row.get("created_at");
            pr_state = row.get('state', '').upper()
            if not pr_url: skipped_no_url += 1; continue
            if not created_at_str: skipped_no_date += 1; continue
            if pr_state != 'MERGED': skipped_not_merged += 1; continue
            
            match_pull = REPO_PULL_PATTERN.match(pr_url);
            match_repo = REPO_PATTERN.match(pr_url);
            repo_name_from_url = None
            if match_pull:
                repo_name_from_url = f"{match_pull.group(1)}/{match_pull.group(2)}"
            elif match_repo:
                repo_name_from_url = f"{match_repo.group(1)}/{match_repo.group(2)}"
            
            if repo_name_from_url:
                try:
                    created_at_date = datetime.strptime(created_at_str, "%Y-%m-%dT%H:%M:%SZ")
                    if repo_name_from_url not in repo_oldest_created_at or created_at_date < repo_oldest_created_at[
                        repo_name_from_url]:
                        repo_oldest_created_at[repo_name_from_url] = created_at_date
                except ValueError:
                    skipped_date_format += 1
            else:
                skipped_url_format += 1
    print(
        f"Step 1 Complete: Loaded oldest dates for {len(repo_oldest_created_at)} repositories. (Processed: {processed_rows}, Skipped [No URL:{skipped_no_url}, No Date:{skipped_no_date}, Not Merged:{skipped_not_merged}, URL Format:{skipped_url_format}, Date Format:{skipped_date_format}])")
except FileNotFoundError:
    print(f"Error: File not found - {PULL_LIST_CSV}"); exit(1)
except Exception as e:
    print(f"Error reading {PULL_LIST_CSV}: {e}"); exit(1)

print(f"\nStep 2: Loading target PR counts per repository from {INPUT_CSV}...")
repo_target_counts = collections.defaultdict(int) # Renamed from chromaticDict
try:
    with open(INPUT_CSV, "r", encoding="utf-8", newline="") as inputFile:
        reader = csv.DictReader(inputFile)
        processed_rows = 0;
        skipped_missing_col = 0;
        skipped_value_error = 0
        required_columns = ['repositoryname', 'unique_count']
        if not all(col in reader.fieldnames for col in required_columns):
            print(
                f"Error: Required column(s) {required_columns} missing in {INPUT_CSV}. Columns found: {reader.fieldnames}");
            exit(1)
        for row in reader:
            processed_rows += 1;
            repo_name_from_input = row.get('repositoryname');
            pr_count_str = row.get('unique_count')
            if repo_name_from_input and pr_count_str:
                try:
                    repo_target_counts[repo_name_from_input] = int(pr_count_str)
                except ValueError:
                    skipped_value_error += 1
            else:
                skipped_missing_col += 1
    print(
        f"Step 2 Complete: Loaded target PR counts for {len(repo_target_counts)} repositories. (Processed: {processed_rows}, Skipped [Missing Data:{skipped_missing_col}, Value Error:{skipped_value_error}])")
except FileNotFoundError:
    print(f"Error: File not found - {INPUT_CSV}"); exit(1)
except Exception as e:
    print(f"Error reading {INPUT_CSV}: {e}"); exit(1)


print(f"\nStep 3: Processing non-chromatic PR data from {NON_CHROMATIC_DIR} and extracting candidates...")
nonchromatic_candidates_per_repo = collections.defaultdict(list) 
processed_repo_count_step3 = 0
total_repos_to_process_step3 = len(repo_target_counts) 
repos_with_file_not_found_step3 = []

for repo_name_current_processing in repo_target_counts.keys():
    processed_repo_count_step3 += 1
    repo_name_safe = repo_name_current_processing.replace('/', "_")
    non_chromatic_file_path = os.path.join(NON_CHROMATIC_DIR, f"pr_details_{repo_name_safe}.csv")

    if not os.path.exists(non_chromatic_file_path):
        if repo_name_current_processing not in repos_with_file_not_found_step3:
            repos_with_file_not_found_step3.append(repo_name_current_processing)
        continue

    try:
        with open(non_chromatic_file_path, "r", encoding="utf-8", newline="") as nonChromaticFileObj:
            reader = csv.DictReader(nonChromaticFileObj)
       
            file_processed_pr_count = 0
            file_added_candidate_count = 0
            file_skipped_missing_data = 0
            file_skipped_date_error = 0
            file_skipped_too_old = 0
            file_skipped_not_merged = 0
            file_skipped_url_parse = 0
            
            oldest_date_filter = repo_oldest_created_at.get(repo_name_current_processing)

            for row in reader:
                file_processed_pr_count += 1
                created_at_str = row.get('created_at')
                pr_url = row.get('pr_url')
                pr_state = row.get('state', '').upper()

                if not created_at_str or not pr_url: file_skipped_missing_data += 1; continue
                if pr_state != 'MERGED': file_skipped_not_merged += 1; continue # Only consider MERGED PRs as candidates from non-chromatic files

                owner, repo_short_name, pr_number_str_val = None, None, None
                match_pr = REPO_PULL_PATTERN.match(pr_url)
                if match_pr:
                    owner = match_pr.group(1)
                    repo_short_name = match_pr.group(2)
                    pr_number_str_val = match_pr.group(3)
                else:
                    file_skipped_url_parse += 1;
                    continue

                if not owner or not repo_short_name or not pr_number_str_val:
                    file_skipped_url_parse += 1;
                    continue
                try:
                    pr_number_val = int(pr_number_str_val)
                except ValueError:
                    file_skipped_url_parse += 1;
                    continue

                try:
                    created_at_date = datetime.strptime(created_at_str, "%Y-%m-%dT%H:%M:%SZ")
                    if oldest_date_filter and created_at_date < oldest_date_filter:
                        file_skipped_too_old += 1;
                        continue

                    nonchromatic_candidates_per_repo[repo_name_current_processing].append({
                        'repo_name': repo_name_current_processing,
                        'owner': owner, 'repo_short_name': repo_short_name, 'pull_number': pr_number_val,
                        'pr_title': row.get('pr_title', ''), 'pr_url': pr_url,
                        'created_at': created_at_str, 'closed_at': row.get('closed_at', ''),
                        'total_comments': row.get('total_comments', ''),
                        'total_commits': row.get('total_commits', ''), 'state': pr_state,
                    })
                    file_added_candidate_count += 1
                except ValueError:
                    file_skipped_date_error += 1
                except Exception as row_e:
                    print(f"  Warning: Unexpected error while processing row ({row_e}) - {row}")
    except FileNotFoundError: 
        if repo_name_current_processing not in repos_with_file_not_found_step3: 
            repos_with_file_not_found_step3.append(repo_name_current_processing)
    except Exception as file_e:
        print(f"  Error: An error occurred while processing {non_chromatic_file_path} - {file_e}")

print(f"Step 3 Complete: Extracted candidates for {len(nonchromatic_candidates_per_repo)} repositories.")
if repos_with_file_not_found_step3:
    print(f"(Non-chromatic PR detail files not found for: {len(repos_with_file_not_found_step3)} repositories)")


print(f"\nStep 4: Starting sampling, extension check (via API), and replacement process for each repository...")
final_selected_prs = {} 
total_selected_pr_count = 0 
total_api_calls = 0 
total_replacements = 0 
repos_with_insufficient_selection = 0 
repos_with_prs_missing_extensions = 0 
api_check_cache = {}

processed_repo_count_step4 = 0


for repo_name, target_pr_count_for_repo in repo_target_counts.items():
    processed_repo_count_step4 += 1
    print(
        f"\n--- Processing repository ({processed_repo_count_step4}/{total_repos_to_process_step3}): {repo_name} (Required: {target_pr_count_for_repo}) ---")

    if repo_name not in nonchromatic_candidates_per_repo:
        print("  -> No candidates extracted in Step 3. Skipping.")
        if target_pr_count_for_repo > 0: repos_with_insufficient_selection += 1
        continue

    candidates_for_this_repo = list(nonchromatic_candidates_per_repo[repo_name])
    if not candidates_for_this_repo:
        print("  -> 0 available candidates. Skipping.")
        if target_pr_count_for_repo > 0: repos_with_insufficient_selection += 1
        continue

    actual_candidate_count = len(candidates_for_this_repo)
    print(f"  Available candidates (after date/merge filter): {actual_candidate_count}")

    num_to_select_for_repo = min(target_pr_count_for_repo, actual_candidate_count)
    if num_to_select_for_repo == 0:
        print("  -> Number of PRs to select is 0. Skipping.")
        continue

    random.shuffle(candidates_for_this_repo)
    current_selection_for_repo = candidates_for_this_repo[:num_to_select_for_repo]
    replacement_candidate_pool_for_repo = candidates_for_this_repo[num_to_select_for_repo:]

    print(f"  Initial selection count: {len(current_selection_for_repo)}")
    print(f"  Replacement candidate pool size: {len(replacement_candidate_pool_for_repo)}")

    api_calls_for_this_repo = 0
    replacements_for_this_repo = 0

    for i in range(len(current_selection_for_repo)):
        pr_to_check = current_selection_for_repo[i]
        pr_url_to_check = pr_to_check['pr_url']

        has_target_extension_status = api_check_cache.get(pr_url_to_check)

        if has_target_extension_status is None:
            time.sleep(1) 
            api_calls_for_this_repo += 1
            modified_files_list = get_pr_modified_files(
                pr_to_check['owner'], pr_to_check['repo_short_name'],
                pr_to_check['pull_number'], GITHUB_TOKEN
            )
            if modified_files_list is None:
                has_target_extension_status = False
                print(f"    -> API error/No PR info for {pr_url_to_check}. Treating as no target extension.")
            else:
                has_target_extension_status = contains_target_extension(modified_files_list)
            api_check_cache[pr_url_to_check] = has_target_extension_status

        if has_target_extension_status:
            pass
        else: 
            found_replacement_for_pr = False
            for k_idx in range(len(replacement_candidate_pool_for_repo) - 1, -1, -1): # Iterate backwards to pop safely
                replacement_candidate = replacement_candidate_pool_for_repo[k_idx]
                replacement_pr_url = replacement_candidate['pr_url']

                replacement_has_extension_status = api_check_cache.get(replacement_pr_url)
                if replacement_has_extension_status is None:
                    time.sleep(1) 
                    api_calls_for_this_repo += 1
                    modified_files_for_replacement = get_pr_modified_files(
                        replacement_candidate['owner'], replacement_candidate['repo_short_name'],
                        replacement_candidate['pull_number'], GITHUB_TOKEN
                    )
                    if modified_files_for_replacement is None:
                        replacement_has_extension_status = False
                        print(f"        -> API error/No PR info for replacement candidate {replacement_pr_url}. Treating as no target extension.")
                    else:
                        replacement_has_extension_status = contains_target_extension(modified_files_for_replacement)
                    api_check_cache[replacement_pr_url] = replacement_has_extension_status

                if replacement_has_extension_status:
                    print(f"      -> Replacing PR #{pr_to_check['pull_number']} with PR #{replacement_candidate['pull_number']} (has target extension).")
                    current_selection_for_repo[i] = replacement_candidate_pool_for_repo.pop(k_idx)
                    replacements_for_this_repo += 1
                    found_replacement_for_pr = True
                    break 
          

    final_selected_count_for_repo = len(current_selection_for_repo)
    print(
        f"  -> Processing complete for {repo_name}: Final selected count {final_selected_count_for_repo}, API calls {api_calls_for_this_repo}, Replacements {replacements_for_this_repo}")
    
    final_selected_prs[repo_name] = current_selection_for_repo
    total_selected_pr_count += final_selected_count_for_repo
    total_api_calls += api_calls_for_this_repo
    total_replacements += replacements_for_this_repo

    extensions_missing_in_final_selection = 0
    for final_pr_data in current_selection_for_repo:
        final_pr_url = final_pr_data['pr_url']
        
        if not api_check_cache.get(final_pr_url, False): 
            extensions_missing_in_final_selection += 1
        elif final_pr_url not in api_check_cache and GITHUB_TOKEN and GITHUB_TOKEN != "YOUR_GITHUB_PAT_HERE" and "11APL6R7I0asGBprwXgznp_rg9Mi1kVL1akVFPfZyTthgeHsBQDGdxF8T4BXzle2mYA77GOHUUH8W1IIqF" != GITHUB_TOKEN:
         
            print(f"  Warning: PR {final_pr_url} is in final selection but missing from API check cache (token was valid). Counted as missing extension.")
            extensions_missing_in_final_selection += 1


    if final_selected_count_for_repo < num_to_select_for_repo: 
        repos_with_insufficient_selection += 1
        print(f"  Warning: Final PR count for {repo_name} ({final_selected_count_for_repo}) is less than originally aimed selection count ({num_to_select_for_repo})")
    if extensions_missing_in_final_selection > 0:
        repos_with_prs_missing_extensions += 1
        print(f"  Warning: {extensions_missing_in_final_selection} PRs without target extensions remained in the final selection for {repo_name}.")

print(f"\nStep 4 Complete: Selected a total of {total_selected_pr_count} PRs for output.")
print(f"  (Total API calls (Step 4): {total_api_calls}, Total replacements: {total_replacements})")
if repos_with_insufficient_selection > 0:
    print(f"  ({repos_with_insufficient_selection} repositories had fewer selected PRs than candidates available or target count)")
if repos_with_prs_missing_extensions > 0:
    print(f"  ({repos_with_prs_missing_extensions} repositories could not be completely filled with PRs having target extensions)")


print(f"\nStep 5: Writing results to {OUTPUT_CSV}...")
try:
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    output_fieldnames = ['repo_name', 'pr_title', 'pr_url', 'created_at', 'closed_at',
                  'total_comments', 'total_commits', 'state'] 
    with open(OUTPUT_CSV, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=output_fieldnames, extrasaction='ignore')
        writer.writeheader()
        written_count = 0
        for repo_name_sorted in sorted(final_selected_prs.keys()):
            pr_list_for_repo = final_selected_prs[repo_name_sorted]
            for pr_data_to_write in pr_list_for_repo:
                writer.writerow(pr_data_to_write)
                written_count += 1
    if written_count != total_selected_pr_count:
        print(f"Warning: Selected count ({total_selected_pr_count}) and written count ({written_count}) do not match.")
    print(f" Step 5 Complete: Wrote {written_count} PR data entries to {OUTPUT_CSV}!")
except IOError as e:
    print(f"Error: Failed to write to output file '{OUTPUT_CSV}' - {e}")
    exit(1)
except Exception as e:
    print(f"Error: Unexpected error during CSV writing - {e}")
    exit(1)

print("\nScript (API extension filtering version) finished successfully ")