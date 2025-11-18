import csv
import collections
import random
import re
import os
from datetime import datetime

INPUT_CSV = '../../data/unique-vrt-comments-without.csv'
PULL_LIST_CSV = '../../data/list-vrt-comments.csv'
OUTPUT_CSV = '../../data/non_vrt/visual-pr-without-open.csv'

OUTPUT_CSV_IN_RANGE = '../../data/non_vrt/visual-pr-without-open-in-range-saner.csv'


CANDIDATE_DIRS = [
    "../../data/visual_prs_not_in_vrt_in_comments2",
]


# Regex pattern to extract GitHub repository and PR number
REPO_PULL_PATTERN = re.compile(r"https://github\.com/([^/]+)/([^/]+)/pull/(\d+)")
REPO_PATTERN = re.compile(r"https://github\.com/([^/]+)/([^/]+)")

DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
try:
    START_DATE_FILTER = datetime.strptime("2018-07-02T00:00:00Z", DATE_FORMAT)
    END_DATE_FILTER = datetime.strptime("2025-09-30T23:59:59Z", DATE_FORMAT)
except ValueError as e:
    print(f"Error defining date filters: {e}")
    exit(1)
# ---

print("Script start")
print(f"Step 1: Loading oldest PR creation dates per repository from {PULL_LIST_CSV}...")

repo_oldest_created_at = {}
try:
    with open(PULL_LIST_CSV, "r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        required_columns = ["url", "created_at", "state"]
        if not all(col in reader.fieldnames for col in required_columns):
            print(f"Error: Required column(s) {required_columns} missing in {PULL_LIST_CSV}.")
            exit(1)
        for row in reader:
            pr_url = row.get("url")
            created_at_str = row.get("created_at")
            if not pr_url or not created_at_str:
                continue
            match_pull = REPO_PULL_PATTERN.match(pr_url)
            match_repo = REPO_PATTERN.match(pr_url)
            repo_name_from_url = None
            if match_pull:
                repo_name_from_url = f"{match_pull.group(1)}/{match_pull.group(2)}"
            elif match_repo:
                repo_name_from_url = f"{match_repo.group(1)}/{match_repo.group(2)}"
            if repo_name_from_url:
                try:
                    created_at_date = datetime.strptime(created_at_str, DATE_FORMAT)
                    if repo_name_from_url not in repo_oldest_created_at or created_at_date < repo_oldest_created_at[repo_name_from_url]:
                        repo_oldest_created_at[repo_name_from_url] = created_at_date
                except ValueError:
                    continue
    print(f"Step 1 Complete: Loaded oldest dates for {len(repo_oldest_created_at)} repositories.")
except FileNotFoundError:
    print(f"Error: File not found - {PULL_LIST_CSV}")
    exit(1)

print(f"\nStep 2: Loading target PR counts per repository from {INPUT_CSV}...")

repo_target_counts = collections.defaultdict(int)
try:
    with open(INPUT_CSV, "r", encoding="utf-8", newline="") as inputFile:
        reader = csv.DictReader(inputFile)
        required_columns = ['repository_name', 'unique_pr_count']
        if not all(col in reader.fieldnames for col in required_columns):
            print(f"Error: Required column(s) {required_columns} missing in {INPUT_CSV}.")
            exit(1)
        for row in reader:
            repo_name = row.get('repository_name')
            pr_count_str = row.get('unique_pr_count')
            if repo_name and pr_count_str:
                try:
                    repo_target_counts[repo_name] = int(pr_count_str)
                except ValueError:
                    continue
    print(f"Step 2 Complete: Loaded target PR counts for {len(repo_target_counts)} repositories.")
except FileNotFoundError:
    print(f"Error: File not found - {INPUT_CSV}")
    exit(1)

print(f"\nStep 3: Loading and filtering PR candidates from {len(CANDIDATE_DIRS)} directories...")

nonchromatic_candidates_per_repo = collections.defaultdict(list)

in_range_candidates_per_repo = collections.defaultdict(list) 

seen_pr_urls_per_repo = collections.defaultdict(set)

for repo_name in repo_target_counts.keys():
    repo_name_safe = repo_name.replace('/', "_")
    

    for candidate_dir in CANDIDATE_DIRS:
        file_path = os.path.join(candidate_dir, f"pr_details_{repo_name_safe}.csv")
        
        if not os.path.exists(file_path):
            continue
        
        try:
            with open(file_path, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                oldest_date_filter = repo_oldest_created_at.get(repo_name)
                
                for row in reader:
                    pr_url = row.get('pr_url')
                    created_at_str = row.get('created_at')
                    
                    if not pr_url or not created_at_str:
                        continue
                    

                    if pr_url in seen_pr_urls_per_repo[repo_name]:
                        continue
                    
                    match = REPO_PULL_PATTERN.match(pr_url)
                    if not match:
                        continue
                        
                    owner, repo_short_name, pr_number_str = match.groups()
                    
                    try:
                        created_at_date = datetime.strptime(created_at_str, DATE_FORMAT)
                        

                        pr_state = row.get('state', '').upper() 
                        if pr_state == 'OPEN':
                            continue


                        pr_data_dict = {
                            'repo_name': repo_name,
                            'owner': owner,
                            'repo_short_name': repo_short_name,
                            'pull_number': int(pr_number_str),
                            'pr_title': row.get('pr_title', ''),
                            'pr_url': pr_url,
                            'created_at': created_at_str,
                            'closed_at': row.get('closed_at', ''),
                            'total_comments': row.get('total_comments', ''),
                            'total_commits': row.get('total_commits', ''),
                            'state': pr_state,
                        }

                        added_to_any_list = False 
                        
    
                        if START_DATE_FILTER <= created_at_date <= END_DATE_FILTER:
                            in_range_candidates_per_repo[repo_name].append(pr_data_dict)
                            added_to_any_list = True

            
                        if oldest_date_filter and created_at_date < oldest_date_filter:
                      
                            if added_to_any_list:
                                seen_pr_urls_per_repo[repo_name].add(pr_url)
                            continue
                        
         
                        nonchromatic_candidates_per_repo[repo_name].append(pr_data_dict)
                        added_to_any_list = True
                        
     
                        if added_to_any_list:
                            seen_pr_urls_per_repo[repo_name].add(pr_url)
                        
                    except ValueError:
                        continue
        except Exception as e:
            print(f"Error reading {file_path}: {e}")


print(f"Step 3 Complete: Extracted candidates for {len(nonchromatic_candidates_per_repo)} repositories (after oldest_date filter).")
print(f"Step 3 Complete: Extracted candidates for {len(in_range_candidates_per_repo)} repositories (in date range only).")


print(f"\nStep 4: Randomly selecting PRs (After oldest_date filter)...")

final_selected_prs = {}
total_selected_pr_count = 0
shortage_repos_list = [] 

for repo_name, target_count in repo_target_counts.items():
    candidates = nonchromatic_candidates_per_repo.get(repo_name, [])
    
    if not candidates:
        shortage_repos_list.append(f"  {repo_name} (Needed: {target_count}, Found: 0)")
        continue
        
    random.shuffle(candidates)
    selected = candidates[:min(target_count, len(candidates))] 
    final_selected_prs[repo_name] = selected
    total_selected_pr_count += len(selected)

    if len(selected) < target_count:
        warning_msg = f"  {repo_name} (Needed: {target_count}, Found: {len(selected)})"
        print(f"  Warning: {warning_msg.strip()}") 
        shortage_repos_list.append(warning_msg) 

print(f"\nStep 4 Complete: Selected a total of {total_selected_pr_count} PRs (After oldest_date filter).")

if shortage_repos_list:
    print(f"\n--- Insufficient PRs Summary (After oldest_date filter) ---")
    print(f"{len(shortage_repos_list)} repositories did not meet the target selection count:")
    for line in sorted(shortage_repos_list):
        print(line)
else:
    print("All repositories met their target selection counts (After oldest_date filter).")



print(f"\nStep 4.5: Sampling PRs per repository (In date range only, BEFORE oldest_date filter)...")
final_selected_prs_in_range = {}
total_selected_in_range = 0
shortage_repos_list_in_range = []

for repo, target_count in repo_target_counts.items():

    candidates = in_range_candidates_per_repo.get(repo, []) 
    
    if not candidates:
        shortage_repos_list_in_range.append(f"  {repo} (Needed: {target_count}, Found: 0)")
        continue

    random.shuffle(candidates)
    selected = candidates[:target_count]
    final_selected_prs_in_range[repo] = selected
    total_selected_in_range += len(selected)

    if len(selected) < target_count:
        warning_msg = f"  {repo} (Needed: {target_count}, Found: {len(selected)})"
        print(f"  Warning (in range): {warning_msg.strip()}") 
        shortage_repos_list_in_range.append(warning_msg) 

print(f"\nStep 4.5 complete: Selected total of {total_selected_in_range} PRs (In date range only)")

if shortage_repos_list_in_range:
    print(f"\n--- Insufficient PRs Summary (In date range only) ---")
    print(f"{len(shortage_repos_list_in_range)} repositories did not meet the target selection count:")
    for line in sorted(shortage_repos_list_in_range): 
        print(line)
else:
    print("All repositories met their target selection counts (In date range only).")
# ---


print(f"\nStep 5: Writing results to {OUTPUT_CSV} (After oldest_date filter)...")

try:
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    with open(OUTPUT_CSV, mode='w', newline='', encoding='utf-8') as outfile:
        fieldnames = ['repo_name', 'pr_title', 'pr_url', 'created_at', 'closed_at', 'total_comments', 'total_commits', 'state']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        written_count = 0
        for repo_name_sorted in sorted(final_selected_prs.keys()):
            for pr_data in final_selected_prs[repo_name_sorted]:
                writer.writerow(pr_data)
                written_count += 1
    print(f"Step 5 Complete: Wrote {written_count} entries to {OUTPUT_CSV}.")
except Exception as e:
    print(f"Error writing to {OUTPUT_CSV}: {e}")
    exit(1)


print(f"\nStep 5.5: Writing results to {OUTPUT_CSV_IN_RANGE} (In date range only)...")
try:
    os.makedirs(os.path.dirname(OUTPUT_CSV_IN_RANGE), exist_ok=True)
    with open(OUTPUT_CSV_IN_RANGE, "w", newline="", encoding="utf-8") as out:
        fieldnames = ['repo_name', 'pr_title', 'pr_url', 'created_at', 'closed_at',
                      'total_comments', 'total_commits', 'state']
        writer = csv.DictWriter(out, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        count = 0

        for repo in sorted(final_selected_prs_in_range.keys()):
            for pr in final_selected_prs_in_range[repo]:
                writer.writerow(pr)
                count += 1
    print(f"Step 5.5 complete: Wrote {count} entries to {OUTPUT_CSV_IN_RANGE}")
except Exception as e:
    print(f"Error writing to output: {e}")
    exit(1)

print("\nScript finished successfully.")