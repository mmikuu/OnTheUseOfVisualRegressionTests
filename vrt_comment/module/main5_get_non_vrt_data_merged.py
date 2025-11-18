import csv
import collections
import random
import re
import os
from datetime import datetime

INPUT_CSV = '../../data/unique-vrt-comments-merged.csv'
PULL_LIST_CSV = '../../data/list-vrt-comments.csv'
OUTPUT_CSV = '../../data/non_vrt/visual-prs-merged.csv'


OUTPUT_CSV_IN_RANGE = '../../data/non_vrt/visual-prs-merged-in-range-saner.csv'


NON_CHROMATIC_DIR = "../../data/visual_prs_not_in_vrt_in_comments"

REPO_PULL_PATTERN = re.compile(r"https://github\.com/([^/]+)/([^/]+)/pull/(\d+)")
REPO_PATTERN = re.compile(r"https://github\.com/([^/]+)/([^/]+)")

DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
try:
    START_DATE_FILTER = datetime.strptime("2018-07-02T00:00:00Z", DATE_FORMAT)
    END_DATE_FILTER = datetime.strptime("2025-09-30T23:59:59Z", DATE_FORMAT)
except ValueError as e:
    print(f"Error defining date filters: {e}")
    exit(1)

print("Script start")

print(f"Step 1: Loading oldest PR creation dates from {PULL_LIST_CSV}...")

repo_oldest_created_at = {}
try:
    with open(PULL_LIST_CSV, "r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        required_columns = ["url", "created_at", "state"]
        if not all(col in reader.fieldnames for col in required_columns):
            print(f"Error: Required columns missing in {PULL_LIST_CSV}")
            exit(1)
        for row in reader:
            pr_url = row.get("url")
            created_at_str = row.get("created_at")
            pr_state = row.get('state', '').upper()
            if not pr_url or not created_at_str or pr_state != 'MERGED':
                continue

            match_pull = REPO_PULL_PATTERN.match(pr_url)
            match_repo = REPO_PATTERN.match(pr_url)
            repo_name = None
            if match_pull:
                repo_name = f"{match_pull.group(1)}/{match_pull.group(2)}"
            elif match_repo:
                repo_name = f"{match_repo.group(1)}/{match_repo.group(2)}"

            if repo_name:
                try:
                    created_at_date = datetime.strptime(created_at_str, DATE_FORMAT)
                    if repo_name not in repo_oldest_created_at or created_at_date < repo_oldest_created_at[repo_name]:
                        repo_oldest_created_at[repo_name] = created_at_date
                except ValueError:
                    continue
    print(f"Loaded oldest dates for {len(repo_oldest_created_at)} repositories.")
except Exception as e:
    print(f"Error reading {PULL_LIST_CSV}: {e}")
    exit(1)


print(f"\nStep 2: Loading target PR counts from {INPUT_CSV}...")

repo_target_counts = collections.defaultdict(int)
try:
    with open(INPUT_CSV, "r", encoding="utf-8", newline="") as inputFile:
        reader = csv.DictReader(inputFile)
        required_columns = ['repository_name', 'unique_pr_count']
        if not all(col in reader.fieldnames for col in required_columns):
            print(f"Error: Required columns missing in {INPUT_CSV}")
            exit(1)
        for row in reader:
            repo = row.get('repository_name')
            count = row.get('unique_pr_count')
            if repo and count:
                try:
                    repo_target_counts[repo] = int(count)
                except ValueError:
                    continue
    print(f"Loaded target PR counts for {len(repo_target_counts)} repositories.")
except Exception as e:
    print(f"Error reading {INPUT_CSV}: {e}")
    exit(1)


print(f"\nStep 3: Extracting candidates from {NON_CHROMATIC_DIR}...")

nonchromatic_candidates_per_repo = collections.defaultdict(list)


merged_in_range_candidates_per_repo = collections.defaultdict(list) 



total_read_count = 0
total_merged_read_count = 0
total_merged_in_date_range_count = 0 
total_merged_in_range_and_date_filtered_count = 0 
total_filtered_candidates_count = 0

for repo_name in repo_target_counts.keys():
    safe_name = repo_name.replace("/", "_")
    file_path = os.path.join(NON_CHROMATIC_DIR, f"pr_details_{safe_name}.csv")
    if not os.path.exists(file_path):
        continue

    try:
        with open(file_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            oldest_date = repo_oldest_created_at.get(repo_name)

            for row in reader:
                
                total_read_count += 1

                created_at = row.get('created_at')
                pr_url = row.get('pr_url')
                pr_state = row.get('state', '').upper()

                if pr_state == 'MERGED':
  
                    total_merged_read_count += 1
                

                if not created_at or not pr_url or pr_state != 'MERGED':
                    continue


                match = REPO_PULL_PATTERN.match(pr_url)
                if not match:
                    continue
                owner = match.group(1)
                repo_short = match.group(2)
                try:
                    pr_number = int(match.group(3))
                except ValueError:
                    continue
                
                try:
                    created_at_date = datetime.strptime(created_at, DATE_FORMAT)

   
                    pr_data_dict = {
                        'repo_name': repo_name,
                        'owner': owner,
                        'repo_short_name': repo_short,
                        'pull_number': pr_number,
                        'pr_title': row.get('pr_title', ''),
                        'pr_url': pr_url,
                        'created_at': created_at,
                        'closed_at': row.get('closed_at', ''),
                        'total_comments': row.get('total_comments', ''),
                        'total_commits': row.get('total_commits', ''),
                        'state': pr_state,
                    }
                    
                    is_in_range = False 
                    if START_DATE_FILTER <= created_at_date <= END_DATE_FILTER:

                        total_merged_in_date_range_count += 1
                        is_in_range = True
      
                        merged_in_range_candidates_per_repo[repo_name].append(pr_data_dict)
     
                    if oldest_date and created_at_date < oldest_date:
                        continue
                        
                    if is_in_range: 
                
                        total_merged_in_range_and_date_filtered_count += 1

                except ValueError:
                    continue

 
                total_filtered_candidates_count += 1

                nonchromatic_candidates_per_repo[repo_name].append(pr_data_dict)

    except Exception as e:
        print(f"Error reading {file_path}: {e}")


print(f"\n--- Step 3 Summary ---")
print(f"Total PRs read (before filter): {total_read_count}")
print(f"Total 'MERGED' PRs read (before any filter): {total_merged_read_count}")
print(f"Total 'MERGED' in range (2018-07-02 to 2025-09-30): {total_merged_in_date_range_count}")
print(f"Total 'MERGED' in range AND passed oldest_date filter: {total_merged_in_range_and_date_filtered_count}") 
print(f"Total PRs passed all filters (candidates): {total_filtered_candidates_count}")

print(f"Step 3 complete: Extracted PR candidates for {len(nonchromatic_candidates_per_repo)} repositories.")


print(f"\nStep 4: Sampling PRs per repository (After oldest_date filter)...")
final_selected_prs = {}
total_selected = 0
shortage_repos_list = []

for repo, target_count in repo_target_counts.items():
    candidates = nonchromatic_candidates_per_repo.get(repo, [])
    
    if not candidates:
        shortage_repos_list.append(f"  {repo} (Needed: {target_count}, Found: 0)")
        continue

    random.shuffle(candidates)
    selected = candidates[:target_count]
    final_selected_prs[repo] = selected
    total_selected += len(selected)

    if len(selected) < target_count:
        warning_msg = f"  {repo} (Needed: {target_count}, Found: {len(selected)})"
        print(f"  Warning: {warning_msg.strip()}") 
        shortage_repos_list.append(warning_msg) 

print(f"\nStep 4 complete: Selected total of {total_selected} PRs (After oldest_date filter)")

if shortage_repos_list:
    print(f"\n--- Insufficient PRs Summary (After oldest_date filter) ---")
    print(f"{len(shortage_repos_list)} repositories did not meet the target selection count:")
    for line in sorted(shortage_repos_list): 
        print(line)
else:
    print("All repositories met their target selection counts (After oldest_date filter).")



print(f"\nStep 4.5: Sampling PRs per repository (MERGED in range, BEFORE oldest_date filter)...")
final_selected_prs_in_range = {}
total_selected_in_range = 0
shortage_repos_list_in_range = []

for repo, target_count in repo_target_counts.items():

    candidates = merged_in_range_candidates_per_repo.get(repo, []) 
    
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

print(f"\nStep 4.5 complete: Selected total of {total_selected_in_range} PRs (MERGED in range)")

if shortage_repos_list_in_range:
    print(f"\n--- Insufficient PRs Summary (MERGED in range) ---")
    print(f"{len(shortage_repos_list_in_range)} repositories did not meet the target selection count:")
    for line in sorted(shortage_repos_list_in_range): 
        print(line)
else:
    print("All repositories met their target selection counts (MERGED in range).")

print(f"\nStep 5: Writing results to {OUTPUT_CSV} (After oldest_date filter)...")
try:
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as out:
        fieldnames = ['repo_name', 'pr_title', 'pr_url', 'created_at', 'closed_at',
                      'total_comments', 'total_commits', 'state']
        writer = csv.DictWriter(out, fieldnames=fieldnames, extrasaction='ignore')  
        writer.writeheader()
        count = 0
        for repo in sorted(final_selected_prs.keys()):
            for pr in final_selected_prs[repo]:
                writer.writerow(pr)
                count += 1
    print(f"Step 5 complete: Wrote {count} entries to {OUTPUT_CSV}")
except Exception as e:
    print(f"Error writing to output: {e}")
    exit(1)


print(f"\nStep 5.5: Writing results to {OUTPUT_CSV_IN_RANGE} (MERGED in range)...")
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


print("\n Script finished successfully (no GitHub API / no extension filtering)")