import csv
import re
import os


def extract_repo_and_pull_number_from_url(url_string):
    if not url_string:
        return None, None
    match = re.search(r"github\.com/([^/]+/[^/]+)/pull/(\d+)", url_string)
    if match:
        repo_name = match.group(1)
        pull_number = match.group(2)
        return repo_name, pull_number
    return None, None


def extract_repo_specific_unique_pr_rows_to_csv(input_filename, output_filename, url_column_name, output_columns_list,
                                                allowed_states):
    seen_repo_prs = {}
    unique_rows_to_write = []

    unique_output_columns = list(dict.fromkeys(output_columns_list))

    try:
        with open(input_filename, mode='r', encoding='utf-8-sig', newline='') as infile:
            reader = csv.DictReader(infile)

            input_headers = reader.fieldnames
            if not input_headers:
                print(
                    f"Error: Could not read headers from input file '{input_filename}'. The file might be empty or incorrectly formatted.")
                return

            if url_column_name not in input_headers:
                print(
                    f"Error: URL column '{url_column_name}' not found in the headers of input file '{input_filename}'.")
                print(f"Available columns: {input_headers}")
                return

            if 'state' not in input_headers:
                print(
                    f"Error: 'state' column not found in the headers of input file '{input_filename}'. This column is required for filtering.")
                print(f"Available columns: {input_headers}")
                return


            missing_cols_in_input = [col for col in unique_output_columns if col not in input_headers]
            if missing_cols_in_input:
                print(
                    f"Error: The following specified output columns were not found in the headers of input file '{input_filename}': {sorted(missing_cols_in_input)}")
                print(f"Available columns: {input_headers}")
                return

            print(
                f"Starting processing of '{input_filename}'. Extracting rows for unique pull requests per repository from column '{url_column_name}' for states {allowed_states}...")
            for row_number, row in enumerate(reader, 1):
                url_value = row.get(url_column_name)
                repo_name, pull_number = None, None

                if url_value:
                    repo_name, pull_number = extract_repo_and_pull_number_from_url(url_value)

                current_pr_state = row.get('state', '').strip().upper()
                if current_pr_state not in allowed_states:
                    continue

                if repo_name and pull_number:
                    if repo_name not in seen_repo_prs:
                        seen_repo_prs[repo_name] = set()

                    if pull_number not in seen_repo_prs[repo_name]:
                        seen_repo_prs[repo_name].add(pull_number)

                        output_row_data = {}
                        for col_name in unique_output_columns:
                            output_row_data[col_name] = row.get(col_name)
                        unique_rows_to_write.append(output_row_data)


    except FileNotFoundError:
        print(f"Error: File '{input_filename}' not found.")
        return
    except Exception as e:
        print(f"Error during CSV reading or processing: {e}")
        return

    if not unique_rows_to_write:
        print(f"No rows corresponding to unique pull requests per repository (for states {allowed_states}) were found.")
        return

    try:
        output_dir = os.path.dirname(output_filename)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        with open(output_filename, mode='w', encoding='utf-8', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=unique_output_columns)  # Use the unique list for fieldnames
            writer.writeheader()
            writer.writerows(unique_rows_to_write)
        print(
            f"Successfully wrote specified columns for unique pull requests per repository (for states {allowed_states}) to '{output_filename}'.")
        print(f"A total of {len(unique_rows_to_write)} rows were written.")

    except Exception as e:
        print(f"Error occurred while writing to file '{output_filename}': {e}")



input_csv_file = '../../data/list-vrt-comments.csv'
output_csv_file_merged = '../../data/valid-vrt-merged.csv'
output_csv_file_without_open = '../../data/valid-vrt-without-open.csv'
url_column_header = 'url'

output_columns_to_include = [
    "pr_title", "text", "url", "comment_index",
    "commit_count_since_comment",
    "total_comments", "total_commits",
    "comment_count_since_comment",
    "created_at", "closed_at", "state", "changefile",
    "addline", "deleteline", "fileChanges"
]


if __name__ == "__main__":
    print("--- Processing for MERGED PRs ---")
    extract_repo_specific_unique_pr_rows_to_csv(
        input_csv_file,
        output_csv_file_merged,
        url_column_header,
        output_columns_to_include,
        allowed_states=['MERGED']
    )
    print("\n--- Processing for MERGED or CLOSED PRs (Without OPEN) ---")
    extract_repo_specific_unique_pr_rows_to_csv(
        input_csv_file,
        output_csv_file_without_open,
        url_column_header,
        output_columns_to_include,
        allowed_states=['MERGED', 'CLOSED']
    )
    print("\nScript finished.")