from pyfiles.scrpt2_data_transformation import *


# Log + z-score blocks (these columns are NOT in your provided mapping; keep original names)
log_cols = [
    "brrtc", "brrta", "brrtt", "brrtb",
    "cbrab", "cbrrb", "cbrrt", "cbrrc"
    ]

for c in log_cols:
    if c in df_loantape_anly.columns:
        df_loantape_anly[f"{c}Ln"] = np.log(num(df_loantape_anly[c]) + 1)

z_cols = [
    "brrgy", "orgtt", "brrfc", "brrtc", "brrta", "brrdt", "brrtt", "brrtb", 
    "cbrau_age", "cbrra", "cbrab", "cbrrb", "cbrrd", "cbrrt", "cbrrc",
]
for c in z_cols:
    if c in df_loantape_anly.columns:
        df_loantape_anly[f"{c}_z"] = zscore(df_loantape_anly[c])

# Citizenship NA-fill rules (these are also not in your provided mapping; keep original names)
def _fill_Y_implies_other_N(df, y_col, other_cols):
    for oc in other_cols:
        if y_col in df.columns and oc in df.columns:
            df[oc] = np.where((df[y_col] == "Y") & (df[oc].isna()), "N", df[oc])
    return df

df_loantape_anly = _fill_Y_implies_other_N(df_loantape_anly, "ctzna", ["usrsd", "vsnla"])
df_loantape_anly = _fill_Y_implies_other_N(df_loantape_anly, "usrsd", ["ctzna", "vsnla"])
df_loantape_anly = _fill_Y_implies_other_N(df_loantape_anly, "vsnla", ["ctzna", "usrsd"])

# This can be replaced by the column called "cbrry" = co_borrower_1_citizenship 
# df_loantape_anly = _fill_Y_implies_other_N(df_loantape_anly, "coborrower_us_citizen", ["coborrower_uspres", "coborrower_visa_only"])
# df_loantape_anly = _fill_Y_implies_other_N(df_loantape_anly, "coborrower_uspres", ["coborrower_us_citizen", "coborrower_visa_only"])
# df_loantape_anly = _fill_Y_implies_other_N(df_loantape_anly, "coborrower_visa_only", ["coborrower_us_citizen", "coborrower_uspres"])

# Interaction vars (original names)
df_loantape_anly["borrower_fico_dti_interxn"] = num(df_loantape_anly.get("brrfc")) * num(df_loantape_anly.get("brrdt"))
df_loantape_anly["borrower_fico_tliab_interxn"] = num(df_loantape_anly.get("brrfc")) * num(df_loantape_anly.get("brrtb"))
df_loantape_anly["borrower_fico_lasset_interxn"] = num(df_loantape_anly.get("brrfc")) * num(df_loantape_anly.get("brrtc"))

df_loantape_anly["borrower_tliab_to_tincome"] = safe_div(df_loantape_anly.get("brrtb"), df_loantape_anly.get("brrtt"))
df_loantape_anly["borrower_tliab_to_tasset"] = safe_div(df_loantape_anly.get("brrtb"), df_loantape_anly.get("brrta"))
df_loantape_anly["borrower_lasset_to_tasset"] = safe_div(df_loantape_anly.get("brrtc"), df_loantape_anly.get("brrta"))
df_loantape_anly["borrower_dti_to_liab"] = safe_div(df_loantape_anly.get("brrdt"), df_loantape_anly.get("brrtb"))

df_loantape_anly["borrower_tliab_to_tassetsLn"] = np.log(num(df_loantape_anly.get("brrtb")) + 1) - np.log(num(df_loantape_anly.get("brrta")) + 1)
df_loantape_anly["borrower_tincome_to_tassetLn"] = np.log(num(df_loantape_anly.get("brrtt")) + 1) - np.log(num(df_loantape_anly.get("brrta")) + 1)
df_loantape_anly["borrower_tliab_to_tincomeLn"] = np.log(num(df_loantape_anly.get("brrtb")) + 1) - np.log(num(df_loantape_anly.get("brrtt")) + 1)

inter_cols = [
    "borrower_fico_dti_interxn",
    "borrower_fico_tliab_interxn",
    "borrower_fico_lasset_interxn",
    "borrower_tliab_to_tincome",
    "borrower_tliab_to_tasset",
    "borrower_lasset_to_tasset",
    "borrower_dti_to_liab",
    "borrower_tliab_to_tassetsLn",
    "borrower_tincome_to_tassetLn",
    "borrower_tliab_to_tincomeLn",
]
for c in inter_cols:
    if c in df_loantape_anly.columns:
        df_loantape_anly[f"{c}_z"] = zscore(df_loantape_anly[c])
