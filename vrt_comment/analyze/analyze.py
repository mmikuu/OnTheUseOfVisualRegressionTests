import pandas as pd
import numpy as np
from lifelines.statistics import logrank_test
from scipy.stats import mannwhitneyu
import os


CSV_VRT_PR_PATH = '../../data/valid-vrt-without-open.csv'

# Visual PR (without open)
CSV_VISUAL_PR_WITHOUT_OPEN_PATH = '../../data/non_vrt/visual-pr-without-open-with-metrices.csv'
# Visual PR (merged) 
CSV_VISUAL_PR_MERGED_PATH = '../../data/non_vrt/visual-prs-merged-with-metrices.csv'

OUTPUT_CSV_PATH = '../../results/analytics/result-effectsize.csv'

CREATED_AT_COLUMN = 'created_at'
CLOSED_AT_COLUMN = 'closed_at'
STATE_COLUMN = 'state'
PROJECT_NAME_A = 'VRT PR'
PROJECT_NAME_B = 'Visual PR'


# -----------------------------

def print_separator(title=""):
    if title:
        print(f"\n{'=' * 20} {title} {'=' * 20}")
    else:
        print(f"\n{'=' * 60}")


def calculate_rank_biserial_r(u_stat, n1, n2):
    """
    Mann-Whitney U r (Rank-Biserial Correlation)
    Formula: r = 1 - (2U / (n1 * n2))
    Range: -1 to 1
    """
    try:
        if n1 == 0 or n2 == 0:
            return np.nan, "N/A"


        r = 1 - (2 * u_stat) / (n1 * n2)


        abs_r = abs(r)
        if abs_r < 0.1:
            magnitude = "-"  # Negligible
        elif abs_r < 0.3:
            magnitude = "(S)"  # Small
        elif abs_r < 0.5:
            magnitude = "(M)"  # Medium
        else:
            magnitude = "(L)"  # Large

        return r, magnitude

    except Exception as e:
        print(f"Error calculating Effect Size r: {e}")
        return np.nan, "Error"




def process_time_data(csv_path, created_at_col, closed_at_col):

    print(f"\n--- Loading CSV for time data: '{csv_path}' ---")
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Error loading '{csv_path}': {e}")
        return None, None, None


    if STATE_COLUMN not in df.columns:
        print(
            f"Warning: '{STATE_COLUMN}' not found in {csv_path}. Returning full dataframe for state analysis (if applicable), but time data will be empty.")
        return None, None, df

    df[STATE_COLUMN] = df[STATE_COLUMN].astype(str).str.upper().fillna('UNKNOWN')
    df_merged = df[df[STATE_COLUMN] == 'MERGED'].copy()

    if df_merged.empty:
        print(f"No 'MERGED' data found in {csv_path} for time analysis.")
        return None, None, df

    df_merged[created_at_col] = pd.to_datetime(df_merged[created_at_col], errors='coerce')
    df_merged[closed_at_col] = pd.to_datetime(df_merged[closed_at_col], errors='coerce')
    df_time = df_merged.dropna(subset=[created_at_col, closed_at_col]).copy()


    df_time['time_to_close_days'] = (df_time[closed_at_col] - df_time[created_at_col]).dt.total_seconds() / (3600 * 24)
    df_time = df_time[df_time['time_to_close_days'] >= 0]

    if df_time.empty:
        print(f"No valid time data calculated in {csv_path}.")
        return None, None, df

    durations = df_time['time_to_close_days'].dropna()
    event_observed = np.ones(len(durations))


    return durations, event_observed, df




def process_numerical_column_data(csv_path, target_column_name):
    print(f"\n--- Loading CSV for '{target_column_name}' (MERGED & Valid Dates only): '{csv_path}' ---")
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Error loading '{csv_path}': {e}")
        return None


    if CREATED_AT_COLUMN not in df.columns or CLOSED_AT_COLUMN not in df.columns:
        print(f"Error: Date columns '{CREATED_AT_COLUMN}' or '{CLOSED_AT_COLUMN}' not found in {csv_path}.")
        return None

    df[CREATED_AT_COLUMN] = pd.to_datetime(df[CREATED_AT_COLUMN], errors='coerce')
    df[CLOSED_AT_COLUMN] = pd.to_datetime(df[CLOSED_AT_COLUMN], errors='coerce')
    df.dropna(subset=[CREATED_AT_COLUMN, CLOSED_AT_COLUMN], inplace=True)

    if df.empty:
        print(f"No data remaining after date validation in {csv_path}.")
        return None


    if target_column_name not in df.columns:
        print(f"Column '{target_column_name}' not found.")
        return None

    
    if STATE_COLUMN in df.columns:
        df[STATE_COLUMN] = df[STATE_COLUMN].astype(str).str.upper().fillna('UNKNOWN')
        df = df[df[STATE_COLUMN] == 'MERGED'].copy()
        if df.empty:
            print(f"No 'MERGED' data found for '{target_column_name}' in {csv_path}.")
            return None
        print(f"Filtered for 'MERGED' state. {len(df)} rows remaining for {target_column_name}.")
    else:

        print(
            f"Warning: '{STATE_COLUMN}' not in {csv_path}. Assuming all rows are 'MERGED' (as it's a merged-only file).")

    df[target_column_name] = pd.to_numeric(df[target_column_name], errors='coerce')
    df.dropna(subset=[target_column_name], inplace=True) 
    df = df[df[target_column_name] >= 0]

    if df.empty:
        print(f"No valid numerical data found for '{target_column_name}' after filtering and cleaning.")
        return None

    return df[target_column_name]


def analyze_pr_state(df, project_name):

    if df is None or df.empty:
        print(f"DataFrame for {project_name} (State Analysis) is empty or None.")
        return []
    if STATE_COLUMN not in df.columns:
        print(f"'{STATE_COLUMN}' not found in DataFrame for {project_name}.")
        return []

    df['state'] = df[STATE_COLUMN].astype(str).str.upper().fillna('UNKNOWN')
    total = len(df)
    merged = (df['state'] == 'MERGED').sum()
    closed = (df['state'] == 'CLOSED').sum()
    merged_pct = (merged / total) * 100 if total > 0 else 0

    results = {
        'Total Count': total,
        'Merged Count': merged,
        'Closed Count': closed,
        'Merged Percentage (%)': merged_pct
    }
    print(f"State Analysis for {project_name}: Total={total}, Merged={merged}, Closed={closed}")

    data = []
    for k, v in results.items():
        m_name = 'Merged Percentage' if k == 'Merged Percentage (%)' else 'PR State'
        data.append({'Metric': m_name, 'Statistic': k, 'Project': project_name, 'Value': v})
    return data


def generate_descriptive_stats(data_a, data_b, metric_name, name_a, name_b):

    stats_a = data_a.agg(['mean', 'median', 'std', 'min', 'max', 'count', 'sum'])
    stats_b = data_b.agg(['mean', 'median', 'std', 'min', 'max', 'count', 'sum'])

    df = pd.DataFrame({name_a: stats_a, name_b: stats_b}).reset_index().rename(columns={'index': 'Statistic'})
    df.insert(0, 'Metric', metric_name)
    return df



all_stats = []
all_states = []

fmt_config = {
    'Mann-Whitney U Statistic': {'dec': 2},
    'Mann-Whitney U P-Value': {'dec': 4, 'sci': 1e-4},
    'Log-Rank Test Statistic': {'dec': 2},
    'Log-Rank Test P-Value': {'dec': 4, 'sci': 1e-4},
}


print_separator("ANALYSIS: TIME TO MERGE")

METRIC_TIME = 'Time to Merge (days)'

t_a, e_a, df_a = process_time_data(CSV_VRT_PR_PATH, CREATED_AT_COLUMN, CLOSED_AT_COLUMN)

t_b, e_b, df_b_merged_file = process_time_data(CSV_VISUAL_PR_MERGED_PATH, CREATED_AT_COLUMN, CLOSED_AT_COLUMN)

if t_a is not None and not t_a.empty and t_b is not None and not t_b.empty:
    all_stats.append(generate_descriptive_stats(t_a, t_b, METRIC_TIME, PROJECT_NAME_A, PROJECT_NAME_B))


    lr = logrank_test(t_a, t_b, event_observed_A=e_a, event_observed_B=e_b)
    print(f"Log-Rank p-value: {lr.p_value}")


    u_stat_time, _ = mannwhitneyu(t_a, t_b, alternative='two-sided')
    r_val, r_mag = calculate_rank_biserial_r(u_stat_time, len(t_a), len(t_b))
    print(f"Effect Size r: {r_val:.3f} {r_mag}")

    all_stats.append(pd.DataFrame([
        {'Metric': METRIC_TIME, 'Statistic': 'Log-Rank Test Statistic', PROJECT_NAME_A: lr.test_statistic,
         PROJECT_NAME_B: np.nan},
        {'Metric': METRIC_TIME, 'Statistic': 'Log-Rank Test P-Value', PROJECT_NAME_A: lr.p_value,
         PROJECT_NAME_B: np.nan},
        {'Metric': METRIC_TIME, 'Statistic': 'Effect Size (r)', PROJECT_NAME_A: f"{r_val:.3f} {r_mag}",
         PROJECT_NAME_B: np.nan}
    ]))
else:
    print(f"Skipping {METRIC_TIME}")


print_separator("ANALYSIS: PR STATE (ACCEPTANCE RATE)")

all_states.extend(analyze_pr_state(df_a, PROJECT_NAME_A))


try:
    print(f"\n--- Loading CSV for State data: '{CSV_VISUAL_PR_WITHOUT_OPEN_PATH}' ---")
    df_b_without_open = pd.read_csv(CSV_VISUAL_PR_WITHOUT_OPEN_PATH)
    all_states.extend(analyze_pr_state(df_b_without_open, PROJECT_NAME_B))
except Exception as e:
    print(f"Error loading '{CSV_VISUAL_PR_WITHOUT_OPEN_PATH}' for state analysis: {e}")


metrics = [
    {'col': 'addline', 'name': 'Added Lines'},
    {'col': 'deleteline', 'name': 'Deleted Lines'},
    {'col': 'total_comments', 'name': 'Total Comments'},
    {'col': 'total_commits', 'name': 'Total Commits'},
    {'col': 'changefile', 'name': 'Changed Files'}
]

for m in metrics:
    col, name = m['col'], m['name']
    print_separator(f"ANALYSIS: {name.upper()}")


    d_a = process_numerical_column_data(CSV_VRT_PR_PATH, col)


    d_b = process_numerical_column_data(CSV_VISUAL_PR_MERGED_PATH, col)

    if d_a is not None and not d_a.empty and d_b is not None and not d_b.empty:
        all_stats.append(generate_descriptive_stats(d_a, d_b, name, PROJECT_NAME_A, PROJECT_NAME_B))


        u_stat, p_val = mannwhitneyu(d_a, d_b, alternative='two-sided')
        print(f"MW U p-value: {p_val:.4f}")

       
        r_val, r_mag = calculate_rank_biserial_r(u_stat, len(d_a), len(d_b))
        print(f"Effect Size r: {r_val:.3f} {r_mag}")

        all_stats.append(pd.DataFrame([
            {'Metric': name, 'Statistic': 'Mann-Whitney U Statistic', PROJECT_NAME_A: u_stat, PROJECT_NAME_B: np.nan},
            {'Metric': name, 'Statistic': 'Mann-Whitney U P-Value', PROJECT_NAME_A: p_val, PROJECT_NAME_B: np.nan},
            {'Metric': name, 'Statistic': 'Effect Size (r)', PROJECT_NAME_A: f"{r_val:.3f} {r_mag}",
             PROJECT_NAME_B: np.nan}
        ]))
    else:
        print(f"Skipping {name}")


if all_stats or all_states:
    if all_states:
        s_df = pd.DataFrame(all_states).pivot_table(index=['Metric', 'Statistic'], columns='Project', values='Value',
                                                    aggfunc='first').reset_index()
        s_df.columns.name = None
        all_stats.insert(0, s_df)

    final_df = pd.concat(all_stats, ignore_index=True)



    def fmt(row, c):
        val = row[c]
        if pd.isna(val): return ''
        if isinstance(val, str): return val
        stat = row['Statistic']


        if stat in ['count', 'Total Count', 'Merged Count', 'Closed Count', 'sum']:
            return f"{val:.0f}"

        if stat in ['mean', 'median', 'std', 'Merged Percentage (%)',
                    'Time to Merge (days)']:  
            return f"{val:.2f}"

        if stat in fmt_config:
            cfg = fmt_config[stat]
            if abs(val) < cfg.get('sci', 0) and val != 0:
                return f"{val:.2e}"
            return f"{val:.{cfg['dec']}f}"

    
        if stat in ['mean', 'median', 'std']:
            return f"{val:.2f}"

        return f"{val:.1f}"

    for c in [PROJECT_NAME_A, PROJECT_NAME_B]:
        final_df[c] = final_df.apply(lambda r: fmt(r, c), axis=1)

    try:
        os.makedirs(os.path.dirname(OUTPUT_CSV_PATH), exist_ok=True)
        final_df.to_csv(OUTPUT_CSV_PATH, index=False)
        print_separator("DONE")
        print(f"Saved to: {OUTPUT_CSV_PATH}")
    except Exception as e:
        print(f"Save Error: {e}")
else:
    print("No results generated.")