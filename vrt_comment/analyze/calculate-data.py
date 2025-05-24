import pandas as pd
import numpy as np
from lifelines.statistics import logrank_test
from scipy.stats import mannwhitneyu

CSV_VRT_USED_PATH = '../../data/valid-vrt-merged.csv'
CSV_VRT_NOT_USED_PATH = '../../data/non_vrt/non-vrt-merged.csv'
OUTPUT_CSV_PATH = '../../results/analytics/result.csv'

CREATED_AT_COLUMN = 'created_at'
CLOSED_AT_COLUMN = 'closed_at'

def print_separator(title=""):
    if title:
        print(f"\n{'=' * 20} {title} {'=' * 20}")
    else:
        print(f"\n{'=' * 60}")

def process_time_data(csv_path, created_at_col, closed_at_col):
    print(f"\n--- Loading CSV for time data: '{csv_path}' ---")
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f"Error: CSV file not found: {csv_path}")
        return None, None
    except Exception as e:
        print(f"Error: Problem loading CSV file '{csv_path}': {e}")
        return None, None

    print(f"Original number of records: {len(df)}")

    if created_at_col not in df.columns:
        print(f"Error: Column '{created_at_col}' not found in '{csv_path}'. Please check CREATED_AT_COLUMN.")
        return None, None
    if closed_at_col not in df.columns:
        print(f"Error: Column '{closed_at_col}' not found in '{csv_path}'. Please check CLOSED_AT_COLUMN.")
        return None, None

    df[created_at_col] = pd.to_datetime(df[created_at_col], errors='coerce')
    df[closed_at_col] = pd.to_datetime(df[closed_at_col], errors='coerce')

    df.dropna(subset=[created_at_col, closed_at_col], inplace=True)
    if df.empty:
        print(
            f"Warning: Could not load valid datetime data from '{csv_path}' (NaNs after conversion or empty after drop).")
        return None, None
    print(f"Number of records after datetime conversion and NaN drop: {len(df)}")

    df['time_to_close_hours'] = (df[closed_at_col] - df[created_at_col]).dt.total_seconds() / 3600
    df = df[df['time_to_close_hours'] >= 0]

    if df.empty:
        print(f"Warning: All calculated closing times were invalid (negative) or zero in '{csv_path}'.")
        return None, None
    print(f"Number of records after calculating and filtering closing time: {len(df)}")

    durations = df['time_to_close_hours'].dropna()
    event_observed = np.ones(len(durations))

    return durations, event_observed

def process_numerical_column_data(csv_path, target_column_name):
    print(f"\n--- Loading CSV for numerical column '{target_column_name}': '{csv_path}' ---")
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f"Error: CSV file not found: {csv_path}")
        return None
    except Exception as e:
        print(f"Error: Problem loading CSV file '{csv_path}': {e}")
        return None

    print(f"Original number of records: {len(df)}")

    if target_column_name not in df.columns:
        print(
            f"Error: Column '{target_column_name}' not found in '{csv_path}'. Please check the column name for this metric.")
        return None

    df[target_column_name] = pd.to_numeric(df[target_column_name], errors='coerce')
    df.dropna(subset=[target_column_name], inplace=True)
    df = df[df[target_column_name] >= 0]

    if df.empty:
        print(
            f"Warning: No valid data for '{target_column_name}' found in '{csv_path}' after processing (NaNs, negative values, or empty).")
        return None
    print(f"Number of records after processing for '{target_column_name}': {len(df)}")

    return df[target_column_name]

def generate_descriptive_stats(data_vrt_used, data_vrt_not_used, metric_readable_name):
    print(f"\n--- Descriptive Statistics for {metric_readable_name} ---")
    stats_vrt_used = data_vrt_used.agg(['mean', 'median', 'std', 'min', 'max', 'count'])
    stats_vrt_not_used = data_vrt_not_used.agg(['mean', 'median', 'std', 'min', 'max', 'count'])

    comparison_df = pd.DataFrame({
        'VRT Used': stats_vrt_used,
        'VRT Not Used': stats_vrt_not_used
    })
    print(comparison_df)

    export_df = comparison_df.reset_index().rename(columns={'index': 'Statistic'})
    export_df.insert(0, 'Metric', metric_readable_name)
    return export_df

all_descriptive_stats_dfs = []

print_separator("ANALYSIS: TIME TO MERGE (OR CLOSE)")
METRIC_READABLE_NAME_TIME = 'Time to Merge (hours)'

durations_vrt_used, event_vrt_used = process_time_data(CSV_VRT_USED_PATH, CREATED_AT_COLUMN, CLOSED_AT_COLUMN)
durations_vrt_not_used, event_vrt_not_used = process_time_data(CSV_VRT_NOT_USED_PATH, CREATED_AT_COLUMN,
                                                               CLOSED_AT_COLUMN)

if durations_vrt_used is not None and not durations_vrt_used.empty and \
        durations_vrt_not_used is not None and not durations_vrt_not_used.empty:

    print(f"\nNumber of valid records for VRT Used PRs ({METRIC_READABLE_NAME_TIME}): {len(durations_vrt_used)}")
    print(f"Number of valid records for VRT Not Used PRs ({METRIC_READABLE_NAME_TIME}): {len(durations_vrt_not_used)}")

    desc_stats_time_df = generate_descriptive_stats(durations_vrt_used, durations_vrt_not_used,
                                                    METRIC_READABLE_NAME_TIME)
    all_descriptive_stats_dfs.append(desc_stats_time_df)

    print("\n--- Log-Rank Test ---")
    results_logrank = logrank_test(
        durations_A=durations_vrt_used,
        durations_B=durations_vrt_not_used,
        event_observed_A=event_vrt_used,
        event_observed_B=event_vrt_not_used
    )
    results_logrank.print_summary()
    p_value_logrank = results_logrank.p_value
    print(f"p-value: {p_value_logrank:.8f}")
    alpha = 0.05
    if p_value_logrank < alpha:
        print(
            f"Since the p-value ({p_value_logrank:.8f}) is less than alpha ({alpha}), there is a statistically significant difference in the distribution of time to merge between VRT used and VRT not used.")
    else:
        print(
            f"Since the p-value ({p_value_logrank:.8f}) is greater than or equal to alpha ({alpha}), there is no statistically significant difference in the distribution of time to merge between VRT used and VRT not used.")
else:
    print(f"\nError: Insufficient data for '{METRIC_READABLE_NAME_TIME}' analysis. Skipping this metric.")
    if durations_vrt_used is None or durations_vrt_used.empty:
        print(f"Reason: Data for VRT Used PRs could not be processed or was empty for {METRIC_READABLE_NAME_TIME}.")
    if durations_vrt_not_used is None or durations_vrt_not_used.empty:
        print(f"Reason: Data for VRT Not Used PRs could not be processed or was empty for {METRIC_READABLE_NAME_TIME}.")

metrics_to_analyze = [
    {'target_column': 'addline', 'readable_name': 'Added Lines'},
    {'target_column': 'deleteline', 'readable_name': 'Deleted Lines'},
    {'target_column': 'total_comments', 'readable_name': 'Total Comments'},
    {'target_column': 'total_commits', 'readable_name': 'Total Commits'},
    {'target_column': 'changefile', 'readable_name': 'Changed Files'}
]

for metric in metrics_to_analyze:
    TARGET_COLUMN = metric['target_column']
    METRIC_READABLE_NAME = metric['readable_name']
    print_separator(f"ANALYSIS: {METRIC_READABLE_NAME.upper()}")

    data_vrt_used = process_numerical_column_data(CSV_VRT_USED_PATH, TARGET_COLUMN)
    data_vrt_not_used = process_numerical_column_data(CSV_VRT_NOT_USED_PATH, TARGET_COLUMN)

    if data_vrt_used is not None and not data_vrt_used.empty and \
            data_vrt_not_used is not None and not data_vrt_not_used.empty:

        print(f"\nNumber of valid records for '{TARGET_COLUMN}' (VRT Used PRs): {len(data_vrt_used)}")
        print(f"Number of valid records for '{TARGET_COLUMN}' (VRT Not Used PRs): {len(data_vrt_not_used)}")

        desc_stats_df = generate_descriptive_stats(data_vrt_used, data_vrt_not_used, METRIC_READABLE_NAME)
        all_descriptive_stats_dfs.append(desc_stats_df)

        print(f"\n--- Mann-Whitney U Test for {METRIC_READABLE_NAME} ---")
        try:
            if len(data_vrt_used) > 0 and len(data_vrt_not_used) > 0:
                if data_vrt_used.nunique() == 1 and data_vrt_not_used.nunique() == 1 and data_vrt_used.iloc[0] == \
                        data_vrt_not_used.iloc[0]:
                    print(
                        "Both groups have identical constant values. Mann-Whitney U test is not informative (p-value will likely be 1 or NaN).")
                    statistic_mw, p_value_mw = np.nan, np.nan
                else:
                    statistic_mw, p_value_mw = mannwhitneyu(data_vrt_used, data_vrt_not_used, alternative='two-sided')

                print(f"Statistic: {statistic_mw:.4f}")
                print(f"P-value: {p_value_mw:.4f}")
                alpha = 0.05
                if pd.isna(p_value_mw):
                    print(
                        "P-value is NaN. Cannot determine statistical significance (often due to identical data or lack of variance).")
                elif p_value_mw < alpha:
                    print(
                        f"Since the p-value ({p_value_mw:.4f}) is less than alpha ({alpha}), there is a statistically significant difference in the median {METRIC_READABLE_NAME.lower()} between VRT used and VRT not used.")
                else:
                    print(
                        f"Since the p-value ({p_value_mw:.4f}) is greater than or equal to alpha ({alpha}), there is no statistically significant difference in the median {METRIC_READABLE_NAME.lower()} between VRT used and VRT not used.")
            else:
                print("Insufficient data in one or both groups to perform the Mann-Whitney U test.")

        except ValueError as e:
            print(f"Could not perform Mann-Whitney U test for {METRIC_READABLE_NAME}: {e}")

    else:
        print(f"\nError: Insufficient data for '{METRIC_READABLE_NAME}' analysis. Skipping this metric.")
        if data_vrt_used is None or data_vrt_used.empty:
            print(f"Reason: Data for VRT Used PRs ('{TARGET_COLUMN}') could not be processed or was empty.")
        if data_vrt_not_used is None or data_vrt_not_used.empty:
            print(f"Reason: Data for VRT Not Used PRs ('{TARGET_COLUMN}') could not be processed or was empty.")

if all_descriptive_stats_dfs:
    final_statistics_df = pd.concat(all_descriptive_stats_dfs, ignore_index=True)
    try:
        final_statistics_df.to_csv(OUTPUT_CSV_PATH, index=False)
        print_separator("FINAL OUTPUT")
        print(f"\nAll descriptive statistics saved to '{OUTPUT_CSV_PATH}'")
    except Exception as e:
        print(f"\nError saving final statistics to CSV: {e}")
else:
    print_separator("FINAL OUTPUT")
    print("\nNo descriptive statistics were generated to save to CSV.")

print_separator("ANALYSIS COMPLETE")