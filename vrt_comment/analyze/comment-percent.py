import csv
import statistics 
import sys


def calculate_all_medians(csv_filepath):
    comment_position_percentages = []
    commit_position_percentages = []

    try:
        with open(csv_filepath, mode='r', encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)


            required_cols = [
                'comment_index', 'total_comments',
                'commit_count_since_comment', 'total_commits',
                'state'  
            ]

            missing_cols = [col for col in required_cols if col not in reader.fieldnames]
            if missing_cols:
                print(f"Error: missing col : {missing_cols}", file=sys.stderr)
                print(f" {reader.fieldnames}", file=sys.stderr)
                return None

    
            for row in reader:

                if row.get('state', '').upper() != 'MERGED':
                    continue


                try:

                    comment_index_str = row.get('comment_index')
                    total_comments_str = row.get('total_comments')

                    if comment_index_str and total_comments_str:
                        idx = float(comment_index_str)
                        total_comm = float(total_comments_str)

                        if total_comm > 0:
                            percentage = (idx / total_comm) * 100
                            comment_position_percentages.append(percentage)

                except (ValueError, TypeError):
      
                    pass

                try:

                    total_commits_str = row.get('total_commits')
                    since_comment_str = row.get('commit_count_since_comment')

                    if total_commits_str and since_comment_str:
                        total_c = float(total_commits_str)
                        since_c = float(since_comment_str)

                        if total_c > 0:
                            position_from_start = total_c - since_c
                            percentage = (position_from_start / total_c) * 100
     
                            percentage = max(0, min(100, percentage))
                            commit_position_percentages.append(percentage)

                except (ValueError, TypeError):
      
                    pass

    except FileNotFoundError:
        print(f"Error: can not find  '{csv_filepath}' ", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return None


    results = {}


    if comment_position_percentages:
        results['comment_median'] = statistics.median(comment_position_percentages)
        results['comment_count'] = len(comment_position_percentages)
    else:
        results['comment_median'] = None
        results['comment_count'] = 0


    if commit_position_percentages:
        results['commit_median'] = statistics.median(commit_position_percentages)
        results['commit_count'] = len(commit_position_percentages)
    else:
        results['commit_median'] = None
        results['commit_count'] = 0

    return results



if __name__ == "__main__":


    INPUT_CSV = '../../data/list-vrt-comments.csv'

    print(f"file : {INPUT_CSV}")


    analysis_results = calculate_all_medians(INPUT_CSV)


    if analysis_results:
        print("\n--- MERGED ---") 

        print("\n[1] comment position (comment_index / total_comments)")
        if analysis_results['comment_median'] is not None:
            print(f"  valid data : {analysis_results['comment_count']}")
            print(f"  median : {analysis_results['comment_median']:.2f} %")
        else:
            print("  no valid data found.")


        print("\n[2] commit position ((total_commits - commit_count_since_comment) / total_commits)")
        if analysis_results['commit_median'] is not None:
            print(f"  valid data: {analysis_results['commit_count']}")
            print(f"  median : {analysis_results['commit_median']:.2f} %")
        else:
            print("  no valid data found.")