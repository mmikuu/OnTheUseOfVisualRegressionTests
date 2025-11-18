import csv
import collections
import re
import os  

csv_file_path = "../../data/list-vrt-comments.csv"

output_file_path_merged = "../../data/unique-vrt-comments-merged.csv"
output_file_path_closed = "../../data/unique-vrt-comments-closed.csv"
output_file_path_open = "../../data/unique-vrt-comments-open.csv"
output_file_path_without_open = "../../data/unique-vrt-comments-without-open.csv" 
comment_output_file = "../../results/analytics/calculate-pr.csv"

output_file_path_merged_pr_urls = "../../data/classification/vrt_merged_comments.csv"

merged_repo_comment_prs = collections.defaultdict(list)
closed_repo_comment_prs = collections.defaultdict(list)
open_repo_comment_prs = collections.defaultdict(list)
without_open_repo_comment_prs = collections.defaultdict(list) 

merged_repo_all_pr_numbers_list = collections.defaultdict(list)
closed_repo_all_pr_numbers_list = collections.defaultdict(list)
open_repo_all_pr_numbers_list = collections.defaultdict(list)
without_open_repo_all_pr_numbers_list = collections.defaultdict(list)

pull_pattern = re.compile(r"^https://github.com/([^/]+)/([^/]+)/pull/(\d+)")
all_processed_repo_names = set()

merged_pr_urls_set = set()

classification_dir = os.path.dirname(output_file_path_merged_pr_urls)
if classification_dir:
    os.makedirs(classification_dir, exist_ok=True)
    print(f"Ensured directory exists: {classification_dir}")



try:
    with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        if 'state' not in reader.fieldnames:
            print(f"Error: 'state' column not found in input CSV file '{csv_file_path}'. Aborting process.")
            exit()

        for row_num, row in enumerate(reader):
            url = row.get("url", "").strip()
            pr_state = row.get("state", "").strip().upper()

            if pr_state == 'MERGED':
                merged_pr_urls_set.add(url)

            match = pull_pattern.search(url)
            if match:
                repo_name = f"{match.group(1)}/{match.group(2)}"
                pr_number = match.group(3)
                all_processed_repo_names.add(repo_name)

                if pr_state == 'MERGED':
                    merged_repo_comment_prs[repo_name].append(pr_number)
                    merged_repo_all_pr_numbers_list[repo_name].append(pr_number)
                    without_open_repo_comment_prs[repo_name].append(pr_number)
                    without_open_repo_all_pr_numbers_list[repo_name].append(pr_number)
                    
                
                    
                elif pr_state == 'CLOSED':
                    closed_repo_comment_prs[repo_name].append(pr_number)
                    closed_repo_all_pr_numbers_list[repo_name].append(pr_number)

                    without_open_repo_comment_prs[repo_name].append(pr_number)
                    without_open_repo_all_pr_numbers_list[repo_name].append(pr_number)
                elif pr_state == 'OPEN':
                    open_repo_comment_prs[repo_name].append(pr_number)
                    open_repo_all_pr_numbers_list[repo_name].append(pr_number)

except FileNotFoundError:
    print(f"Error: Input file '{csv_file_path}' not found.")
    exit()
except Exception as e:
    print(f"Error: An issue occurred while reading the input file: {e}")
    exit()

try:
    with open(output_file_path_merged_pr_urls, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['pr_url']) 
        for pr_url in sorted(list(merged_pr_urls_set)): 
            writer.writerow([pr_url])
    print(f"\nSuccessfully wrote {len(merged_pr_urls_set)} unique merged PR URLs (including non-standard URLs) to '{output_file_path_merged_pr_urls}'.")
except Exception as e:
    print(f"\nError: Could not write merged PR URLs CSV '{output_file_path_merged_pr_urls}': {e}")


def calculate_repo_unique_pr_counts(repo_all_prs_list_for_state):
    repo_unique_counts = {}
    for repo, pr_numbers_list in repo_all_prs_list_for_state.items():
        repo_unique_counts[repo] = len(set(pr_numbers_list))
    return repo_unique_counts

merged_repo_unique_counts_per_repo = calculate_repo_unique_pr_counts(merged_repo_all_pr_numbers_list)
closed_repo_unique_counts_per_repo = calculate_repo_unique_pr_counts(closed_repo_all_pr_numbers_list)
open_repo_unique_counts_per_repo = calculate_repo_unique_pr_counts(open_repo_all_pr_numbers_list)

def write_output_csv_per_repo(output_path, repo_comment_data, repo_all_prs_list_for_state,
                              repo_unique_counts_per_repo_data_not_used, state_description):
    written_repo_count = 0
    with open(output_path, mode='w', newline='', encoding='utf-8') as outfile:
        fieldnames = ['repository_name', 'comment_count', 'unique_pr_count', 'pull_numbers']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for repo, comments_pr_list in repo_comment_data.items():
            comment_count_for_repo = len(comments_pr_list)
            unique_prs_for_repo_set = set(repo_all_prs_list_for_state.get(repo, []))
            unique_pr_count_for_repo = len(unique_prs_for_repo_set)

            writer.writerow({
                'repository_name': repo,
                'comment_count': comment_count_for_repo,
                'unique_pr_count': unique_pr_count_for_repo,
                'pull_numbers': ', '.join(sorted(list(unique_prs_for_repo_set)))
            })
            written_repo_count += 1
    print(f"{state_description} PR data: Outputted data for {written_repo_count} repositories to CSV file '{output_path}'.")
    return written_repo_count


write_output_csv_per_repo(output_file_path_merged, merged_repo_comment_prs, merged_repo_all_pr_numbers_list,
                          merged_repo_unique_counts_per_repo, "Merged")
write_output_csv_per_repo(output_file_path_closed, closed_repo_comment_prs, closed_repo_all_pr_numbers_list,
                          closed_repo_unique_counts_per_repo, "Closed (Not Merged)")
write_output_csv_per_repo(output_file_path_open, open_repo_comment_prs, open_repo_all_pr_numbers_list,
                          open_repo_unique_counts_per_repo, "Open")
write_output_csv_per_repo(output_file_path_without_open, without_open_repo_comment_prs, without_open_repo_all_pr_numbers_list,
                          None, "Merged and Closed (without Open)")


total_unique_merged_prs = sum(merged_repo_unique_counts_per_repo.values())
total_unique_closed_prs = sum(closed_repo_unique_counts_per_repo.values())
total_unique_open_prs = sum(open_repo_unique_counts_per_repo.values())

all_repo_pr_identifiers_overall = set()
for repo_name, pr_list in merged_repo_all_pr_numbers_list.items(): 
    for pr_num in set(pr_list):
        all_repo_pr_identifiers_overall.add((repo_name, pr_num))
for repo_name, pr_list in closed_repo_all_pr_numbers_list.items(): 
    for pr_num in set(pr_list):
        all_repo_pr_identifiers_overall.add((repo_name, pr_num))
for repo_name, pr_list in open_repo_all_pr_numbers_list.items():
    for pr_num in set(pr_list):
        all_repo_pr_identifiers_overall.add((repo_name, pr_num))
total_unique_pr_count_overall = len(all_repo_pr_identifiers_overall)

total_project_count = len(all_processed_repo_names)
total_projects_with_merged_prs = len(merged_repo_unique_counts_per_repo)
total_projects_with_closed_prs = len(closed_repo_unique_counts_per_repo)
total_projects_with_open_prs = len(open_repo_unique_counts_per_repo)
projects_with_merged_or_closed_prs = set(merged_repo_unique_counts_per_repo.keys()) | set(closed_repo_unique_counts_per_repo.keys())
total_projects_with_merged_or_closed_prs = len(projects_with_merged_or_closed_prs)

total_merged_comments = sum(len(pr_list) for pr_list in merged_repo_comment_prs.values())
total_closed_comments = sum(len(pr_list) for pr_list in closed_repo_comment_prs.values())
total_open_comments = sum(len(pr_list) for pr_list in open_repo_comment_prs.values())
total_unique_merge_closed_prs = total_unique_merged_prs + total_unique_closed_prs

with open(comment_output_file, mode='w', newline='', encoding='utf-8') as c_outfile:
    writer = csv.writer(c_outfile)
    writer.writerow(["Statistic", "Count"])
    writer.writerow(["Total Unique PR Numbers (All States, (Repo,PR#) unique)", total_unique_pr_count_overall])
    writer.writerow(["Total Unique Projects (Repositories)", total_project_count])
    writer.writerow(["Total Unique PR Numbers (Merged and Closed, Sum of per-repo uniques)",total_unique_merge_closed_prs ]) 
    writer.writerow(["Total Projects with Merged OR Closed PRs", total_projects_with_merged_or_closed_prs])
    writer.writerow(["--- Merged PRs ---", "---"])
    writer.writerow(["Total Projects with Merged PRs", total_projects_with_merged_prs])
    writer.writerow(["Total Unique Merged PRs (Sum of per-repo uniques)", total_unique_merged_prs])
    writer.writerow(["Total Comments on Merged PRs (Input Rows, *Standard URL Only*)", total_merged_comments]) # メモを追加
    writer.writerow(["--- Closed PRs (Not Merged) ---", "---"])
    writer.writerow(["Total Projects with Closed PRs", total_projects_with_closed_prs])
    writer.writerow(["Total Unique Closed PRs (Sum of per-repo uniques)", total_unique_closed_prs])
    writer.writerow(["Total Comments on Closed PRs (Input Rows)", total_closed_comments])
    writer.writerow(["--- Open PRs ---", "---"])
    writer.writerow(["Total Projects with Open PRs", total_projects_with_open_prs])
    writer.writerow(["Total Unique Open PRs (Sum of per-repo uniques)", total_unique_open_prs])
    writer.writerow(["Total Comments on Open PRs (Input Rows)", total_open_comments])

print(f"\nAggregated statistics saved to '{comment_output_file}'.")