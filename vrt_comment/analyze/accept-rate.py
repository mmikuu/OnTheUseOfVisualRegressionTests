import scipy.stats as stats


vrt_data = [282, 25]  
visual_data = [259, 40]  

# 2x2の表を作成
contingency_table = [vrt_data, visual_data]

print(f"Table: {contingency_table}")


chi2, p_chi2, dof, expected = stats.chi2_contingency(contingency_table)
print(f"\n[Chi-square Test]")
print(f"p-value: {p_chi2}")


odds_ratio, p_fisher = stats.fisher_exact(contingency_table)

print(f"\n[Fisher's Exact Test]")
print(f"p-value: {p_fisher}")
print(f"Odds Ratio: {odds_ratio}")


if p_fisher < 0.05:
    print("\n=> Significant difference")
    if odds_ratio > 1:
        print(" (vrtPR > visualPR)")
    else:
        print(" (visualPR > vrtPR)")
else:
    print("\n=> No significant difference")