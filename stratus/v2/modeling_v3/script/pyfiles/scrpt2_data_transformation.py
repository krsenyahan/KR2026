from pyfiles.scrpt1_correct_dtypes import *
import pandas as pd
import numpy as np

CUTOFF = "2025-12-01"

# -------------- Scheduled Payments Columns---------------

# X df_schedpay
# -------------- Interest Rate Summary Columns ---------------

# X df_interestrate_hist
# -------------- Gaps in Payment Columns ---------------

# X df_gaps_payment
# -------------- Loan Information FT Columns ---------------

df_loan_infoFT["fndng_compltd_ind"] = (
    df_loan_infoFT["fndng"]
        .astype("string")          # normalize dtype
        .str.strip()               # remove whitespace
        .replace(
            {"": pd.NA, "null": pd.NA, "NULL": pd.NA, "None": pd.NA, "none": pd.NA}
        )
        .notna()
        .astype("int8")
)

df_loan_infoFT_anly = df_loan_infoFT[["account", "lnmt", "fndng_compltd_ind", "dtlst"]]

# -------------- Flight Training FT Columns ---------------

cols = ['account', 'nps', 'flgts', 'flgtb', 'prgmc', 'prtcl', 'tthrs', 
 'hrs', 'hrsa', 'hrsb', 'hrsc', 'hrsd', 'hrse', 'nmmnt', 'mdclc', 
 'ppl', 'ifr', 'cmms', 'cmmml', 'cfx', 'cfxa', 'mxx', 'atp', 'othrc']
df_flight_recordsFT_trunc = df_flight_recordsFT[cols].copy()

id_cols = ["account"]
numeric_cols = ["tthrs", "hrs", "hrsa", "hrsb", "hrsc", "hrsd", "hrse"]

nominal_cols = [
    "nps", "flgts", "flgtb", "prgmc", "prtcl",
    "nmmnt", "mdclc",
    "ppl", "ifr", "cmms", "cmmml", "cfx", "cfxa", "mxx", "atp", "othrc",
]

def clean_nominal(s: pd.Series) -> pd.Series:
    return (
        s.astype("string")
         .str.strip()
         .replace({"": pd.NA, "null": pd.NA, "NULL": pd.NA, "None": pd.NA, "none": pd.NA})
    )

def cap_rare_levels(s: pd.Series, min_count: int = 6, other_label: str = "OTHER") -> pd.Series:
    """
    Keep levels where count >= min_count (i.e., count > 5).
    Everything else becomes OTHER. Missing stays missing.
    """
    vc = s.value_counts(dropna=True)
    keep = vc[vc >= min_count].index
    return s.where(s.isna() | s.isin(keep), other_label)

# 1) Clean + cap rare levels on nominal columns
for c in nominal_cols:
    df_flight_recordsFT_trunc[c] = clean_nominal(df_flight_recordsFT_trunc[c])
    df_flight_recordsFT_trunc[c] = cap_rare_levels(df_flight_recordsFT_trunc[c], min_count=6, other_label="OTHER")

# 2) One-hot encode with dummy_na + drop_first
df_flight_recordsFT_ohe = pd.get_dummies(
    df_flight_recordsFT_trunc,
    columns=nominal_cols,
    prefix=nominal_cols,
    prefix_sep="__",
    dummy_na=True,
    drop_first=True,
)

# 3) Optional: reorder (id + numeric first)
front = id_cols + numeric_cols
other_cols = [c for c in df_flight_recordsFT_ohe.columns if c not in front]
df_flight_recordsFT_anly = df_flight_recordsFT_ohe[front + other_cols]

# -------------- Borrower Demographics FT Columns ---------------

df_demographicsFT["emply_ind"] = (
    df_demographicsFT["emply"]
        .astype("string")          # normalize dtype
        .str.strip()               # remove whitespace
        .replace(
            {"": pd.NA, "null": pd.NA, "NULL": pd.NA, "None": pd.NA, "none": pd.NA}
        )
        .notna()
        .astype("int8")
)

df_demographicsFT_anly = df_demographicsFT[["account", "agx", "emply_ind"]]

# -------------- TMO Data Columns ---------------
tmoinfo_borrower_cols = ['account', 'is_a_citizen', 'is_us_perm_resident_or_greencard_holder', 'is_visa_only', 'declared_bankruptcy', 
                  'interest_paid_to', 'payment_due_date', 'payment_frequency', 'regular_payment', 
                  'apply_to_p_i', 'apply_to_reserve', 'apply_to_impound', 'apply_to_other', 
                  'dob', 'first_payment_date', 'maturity_date', 'term_left', 'days_late', 'paid_off_date', 
                  'note_rate', 'sold_rate', 

                  'original_total_loan_amount', 'original_amount_financed', 'final_total_amount_funded_to_schools', 
                  'principal_balance', 'trust_balance', 'impound_balance', 'reserve_balance', 'unpaid_late_charges', 'unpaid_charges', 'unpaid_interest',
                  'aggregate_appraised_value', 'closing_date', 'original_balance', 'unearned_discount', 'loan_code', 'annual_percentage_rate', 'total_of_payments', 
                  'state', 'loan_type', 'rate_type', 'loan_product', 'loan_priority'

                  'borrower_fico_score', 'borrower_strata_score', 'borrower_dti', 'borrower_total_monthly_income', 'borrower_total_assets', 'borrower_total_liquid_assets', 'borrower_total_liabilities', 
                  'co_borrower_1_fico_score', 'co_borrower_1_strata_score', 'co_borrower_1_dti', 'co_borrower_1_total_monthly_income', 'co_borrower_1_total_assets', 'co_borrower_1_total_liquid_assets', 'co_borrower_1_total_liabilities', 
                  'co_borrower_2_fico_score', 'co_borrower_2_strata_score', 'co_borrower_2_dti', 'co_borrower_2_total_monthly_income', 'co_borrower_2_total_assets', 'co_borrower_2_total_liquid_assets', 'co_borrower_2_total_liabilities', 
                  'co_borrower_3_fico_score', 'co_borrower_3_strata_score', 'co_borrower_3_dti', 'co_borrower_3_total_monthly_income', 'co_borrower_3_total_assets', 'co_borrower_3_total_liquid_assets', 'co_borrower_3_total_liabilities', 
                  'co_signer_1_fico_score', 'co_signer_1_strata_score', 'co_signer_1_dti', 'co_signer_1_total_monthly_income', 'co_signer_1_total_assets', 'co_signer_1_total_liquid_assets', 'co_signer_1_total_liabilities',
                  'co_signer_2_fico_score', 'co_signer_2_strata_score', 'co_signer_2_dti', 'co_signer_2_total_monthly_income', 'co_signer_2_total_assets', 'co_signer_2_total_liquid_assets', 'co_signer_2_total_liabilities', 
                  'co_signer_3_fico_score', 'co_signer_3_strata_score', 'co_signer_3_dti', 'co_signer_3_total_monthly_income', 'co_signer_3_total_assets', 'co_signer_3_total_liquid_assets', 'co_signer_3_total_liabilities', 
                  
                  'task_1_start_date', 'task_1_resolution_date', 'task_1_category', 'task_1_status', 'task_1_details', 'task_1_result', 
                  'task_2_start_date', 'task_2_resolution_date', 'task_2_category', 'task_2_status', 'task_2_details', 'task_2_result', 
                  'task_3_start_date', 'task_3_resolution_date', 'task_3_category', 'task_3_status', 'task_3_details', 'task_3_result', 
                  'task_4_start_date', 'task_4_resolution_date', 'task_4_category', 'task_4_status', 'task_4_details', 'task_4_result', 
                  'task_5_start_date', 'task_5_resolution_date', 'task_5_category', 'task_5_status', 'task_5_details', 'task_5_result', 

                  'payoff_request_1_start_date', 'payoff_request_1_completion_date', 'payoff_request_1_status', 'payoff_request_1_date_computation_sent', 'payoff_request_1_details', 'payoff_request_1_result',
                  'payoff_request_2_start_date', 'payoff_request_2_completion_date', 'payoff_request_2_status', 'payoff_request_2_date_computation_sent',

                  'property_state', 'property_type', 'property_occupancy', 'property_ltv', 'property_apn', 'calculated_ltv', 
                  'borrower_citizenship', 'co_borrower_1_citizenship', 'co_borrower_2_citizenship', 'co_borrower_3_citizenship', 'litigation', 'litigation_date', 
                  'training_status', 'graduated', 'graduation_date', 'active_in_flight_training', 'stopped_flight_training_date', 'cancelled', 'cancelled_date', 
                  
                  'co_borrower_1_generation', 'co_borrower_1_state', 'co_borrower_1_dob', 
                  'co_borrower_2_generation', 'co_borrower_2_state', 'co_borrower_2_dob', 
                  'co_borrower_3_generation', 'co_borrower_3_state', 'co_borrower_3_dob', 
                  'payoff_in_terms_monthly_payment', 'payoff_in_terms_monthly_payment_1']

tmoinfo_borrower_cols_short = [
    "account", "actzn", "usprm", "vsnly", "dclrd",
    "intpd", "pymtd", "pymtf", "rglrp",
    "apply", "appla", "applb", "applc",
    "dbx", "fstpy", "mtrty", "trmlf", "dyslt", "pdffd",
    "ntrt", "sldrt",

    "orgtt", "orgmt", "fnltt",
    "prnbl", "trstb", "impnd", "rsrvb", "unpdl",
    "unpdc", "unpdn",
    "aggrg", "clsng", "orgbl", "unrnd", "lncd",
    "annlp", "ttpym",
    "stx", "lntyp", "rttyp", "lnprd", "lnpr",

    "brrfc", "brrst", "brrdt", "brrtt",
    "brrta", "brrtc", "brrtb",
    "cbrra", "cbrrs", "cbrrd",
    "cbrrt", "cbrrb", "cbrab",
    "cbrrc",
    "cbrrg", "cbrrh", "cbrri",
    "cbrrj", "cbrrk", "cbrac",
    "cbrrl",
    "cbrrn", "cbrro", "cbrrp",
    "cbrrq", "cbrrr", "cbrad",
    "cbrru",
    "csgna", "csgnb", "csgnc",
    "csgnd", "csgne", "csgnv",
    "csgnf",
    "csgnh", "csgni", "csgnj",
    "csgnk", "csgnl", "csgnw",
    "csgnm",
    "csgno", "csgnp", "csgnq",
    "csgns", "csgnt", "csgnx",
    "csgnu",

    "tskst", "tskrs", "tskct", "tsksa", "tskdt", "tskra",
    "tsksb", "tskrb", "tskca", "tsksc", "tskda", "tskrc",
    "tsksd", "tskrd", "tskcb", "tskse", "tskdb", "tskre",
    "tsksf", "tskrf", "tskcc", "tsksg", "tskdc", "tskrg",
    "tsksh", "tskrh", "tskcd", "tsksi", "tskdd", "tskri",

    "pyffr", "pyffa", "pyffb",
    "pyffc", "pyffd", "pyffe",
    "pyfff", "pyffg", "pyffh",
    "pyffi",

    "prprc", "prprf", "prprg", "prprh", "prpri", "clclt",
    "brrct", "cbrry", "cbrrz", "cbraa",
    "ltgtn", "ltgta",
    "trngs", "grdtd", "grdtn", "actvf", "stppd",
    "cncll", "cncla",

    "cbram", "cbraq", "cbrau",
    "cbrax", "cbrbb", "cbrbe",
    "cbrbi", "cbrbm", "cbrbq",
    "pyffl", "pyffo",
]

tmoinfo_borrower_app_cols = ['account', 'state', 'is_a_citizen', 'is_us_perm_resident_or_greencard_holder', 'is_visa_only', 'declared_bankruptcy', 
                  'dob', 'first_payment_date', 'days_late',

                  'original_total_loan_amount', 'original_amount_financed', 'final_total_amount_funded_to_schools', 
                  'principal_balance', 'loan_type', 'rate_type', 'loan_product', 'loan_priority'

                  'borrower_fico_score', 'borrower_strata_score', 'borrower_dti', 'borrower_total_monthly_income', 'borrower_total_assets', 'borrower_total_liquid_assets', 'borrower_total_liabilities', 
                  'co_borrower_1_fico_score', 'co_borrower_1_strata_score', 'co_borrower_1_dti', 'co_borrower_1_total_monthly_income', 'co_borrower_1_total_assets', 'co_borrower_1_total_liquid_assets', 'co_borrower_1_total_liabilities', 
                  'co_borrower_2_fico_score', 'co_borrower_2_strata_score', 'co_borrower_2_dti', 'co_borrower_2_total_monthly_income', 'co_borrower_2_total_assets', 'co_borrower_2_total_liquid_assets', 'co_borrower_2_total_liabilities', 
                  'co_borrower_3_fico_score', 'co_borrower_3_strata_score', 'co_borrower_3_dti', 'co_borrower_3_total_monthly_income', 'co_borrower_3_total_assets', 'co_borrower_3_total_liquid_assets', 'co_borrower_3_total_liabilities', 
                  'co_signer_1_fico_score', 'co_signer_1_strata_score', 'co_signer_1_dti', 'co_signer_1_total_monthly_income', 'co_signer_1_total_assets', 'co_signer_1_total_liquid_assets', 'co_signer_1_total_liabilities',
                  'co_signer_2_fico_score', 'co_signer_2_strata_score', 'co_signer_2_dti', 'co_signer_2_total_monthly_income', 'co_signer_2_total_assets', 'co_signer_2_total_liquid_assets', 'co_signer_2_total_liabilities', 
                  'co_signer_3_fico_score', 'co_signer_3_strata_score', 'co_signer_3_dti', 'co_signer_3_total_monthly_income', 'co_signer_3_total_assets', 'co_signer_3_total_liquid_assets', 'co_signer_3_total_liabilities', 

                  'property_state', 'property_type', 'property_occupancy', 'property_ltv', 'property_apn', 'calculated_ltv', 
                  'borrower_citizenship', 'co_borrower_1_citizenship', 'co_borrower_2_citizenship', 'co_borrower_3_citizenship', 'litigation', 'litigation_date', 
                  'training_status', 'graduated', 'graduation_date', 'active_in_flight_training', 'stopped_flight_training_date', 'cancelled', 'cancelled_date', 
                  
                  'co_borrower_1_generation', 'co_borrower_1_state', 'co_borrower_1_dob', 
                  'co_borrower_2_generation', 'co_borrower_2_state', 'co_borrower_2_dob', 
                  'co_borrower_3_generation', 'co_borrower_3_state', 'co_borrower_3_dob']

tmoinfo_applicant_cols_short = [
    "account", "stx", "actzn", "usprm", "vsnly", "dclrd",
    "dbx", "fstpy", "dyslt",

    "orgtt", "orgmt", "fnltt",
    "prnbl", "lntyp", "rttyp", "lnprd", "lnpr",

    "brrfc", "brrst", "brrdt", "brrtt",
    "brrta", "brrtc", "brrtb",
    "cbrra", "cbrrs", "cbrrd",
    "cbrrt", "cbrrb", "cbrab",
    "cbrrc",
    "cbrrg", "cbrrh", "cbrri",
    "cbrrj", "cbrrk", "cbrac",
    "cbrrl",
    "cbrrn", "cbrro", "cbrrp",
    "cbrrq", "cbrrr", "cbrad",
    "cbrru",
    "csgna", "csgnb", "csgnc",
    "csgnd", "csgne", "csgnv",
    "csgnf",
    "csgnh", "csgni", "csgnj",
    "csgnk", "csgnl", "csgnw",
    "csgnm",
    "csgno", "csgnp", "csgnq",
    "csgns", "csgnt", "csgnx",
    "csgnu",

    "prprc", "prprf", "prprg", "prprh", "prpri", "clclt",
    "brrct", "cbrry", "cbrrz", "cbraa",
    "ltgtn", "ltgta",
    "trngs", "grdtd", "grdtn", "actvf", "stppd",
    "cncll", "cncla",

    "cbram", "cbraq", "cbrau",
    "cbrax", "cbrbb", "cbrbe",
    "cbrbi", "cbrbm", "cbrbq",
]

df_tmoinfo_borrower_anly = df_tmoinfo[tmoinfo_borrower_cols_short].copy()
df_tmoinfo_borrower_app_anly = df_tmoinfo[tmoinfo_applicant_cols_short].copy()


# -------------- Loan Tape Columns ---------------

loantape_borrower_cols = ['account', 'borrower_age_yrs', 'citizenship_ind', 'usresident_greencard_ind', 'visa_only',
                          'state', 'bankrupt_status', 'lncnt', 'loan_age_months', 'remaining_term_months', 
                          'loan_product', 'interest_rate_type', 'principal_balance', 'actual_balance', 'original_total_loan_amount', 
                          'principal_payoff', 'principal_payments', 'principal_paydown', 'principal_adjustments', 'original_points', 
                          'reduce_points', 'discount_points', 'current_total_loan_amount', 'current_points', 'modified_status', 
                          'loan_price', 'loan_sold', 'loan_owner', 'pledged_to', 'loan_sold_rate', 'flight_school', 'flight_school_state',
                          'flight_school_program', 'flight_school_program_total_hours', 'flight_training_0_hours',
                          'flight_training_40_50_hour_milestone', 'flight_training_70_120_hour_milestone', 
                          'flight_training_170_180_hour_milestone', 'flight_training_200_220_hour_milestone', 
                          'flight_training_230_hour_milestone', 'current_monthly_payment', 'current_interest_rate', 
                          'loan_status', 'active_status', 'delinquency_bucket', 'days_past_due', 'amortization_type', 
                          'payment_schedule_1_amount', 'payment_schedule_2_amount', 'payment_schedule_3_amount', 
                          'payment_schedule_4_amount', 'payment_schedule_1_note_rate', 'payment_schedule_2_note_rate',
                          'payment_schedule_3_note_rate', 'payment_schedule_4_note_rate', 'with_co_borrower', 'with_co_signer',
                          'fico_score', 'dti', 'borrower_fico_score', 'co_borrower_1_fico_score', 'co_borrower_2_fico_score', 
                          'co_borrower_3_fico_score', 'co_signer_1_fico_score', 'co_signer_2_fico_score', 'co_signer_3_fico_score', 
                          'borrower_dti', 'co_borrower_1_dti', 'co_borrower_2_dti', 'co_borrower_3_dti', 'co_signer_1_dti', 
                          'co_signer_2_dti', 'co_signer_3_dti']

loantape_borrower_cols_short = ["account", "brrgy", "ctzna", "usrsd", "vsnla", "stx", "bnkra", "lncnt", "lngmn", 
                                "rmnng", "lnprd", "intrt", "prnbl", "actlb", "orgtt", "prnpa", "prnpb", "prnpc", 
                                "prndj", "orgpn", "rdcpn", "dscnb", "crttl", "crpnt", "mdstt", "lnprc", "lnslb", 
                                "lnwnr", "pldgd", "lnsla", "flgtc", "flgts", "flgtd", "flgte", "flgtt", "flgtf", 
                                "flgtg", "flgth", "flgti", "flgtj", "crmnt", "crntr", "lnstt", "actvs", "dlqbc", 
                                "dysps", "amrtz", "pymtj", "pymtk", "pymtl", "pymtm", "pymtn", "pymto", "pymtp", 
                                "pymtq", "cbrr", "csgny", "fcscr", "dtx", "brrfc", "cbrra", "cbrrg", "cbrrn", 
                                "csgna", "csgnh", "csgno", "brrdt", "cbrrd", "cbrri", "cbrrp", "csgnc", "csgnj", 
                                "csgnq"]

loantape_applicant_cols = ['account', 'borrower_age_yrs', 'citizenship_ind', 'usresident_greencard_ind', 
                           'visa_only', 'state', 'loan_contract_date', 'loan_product', 'interest_rate_type', 
                           'original_total_loan_amount', 'modified_status', 'flight_school', 'flight_school_state',
                           'flight_school_program', 'flight_school_program_total_hours', 'loan_status', 
                           'active_status', 'delinquency_bucket', 'days_past_due', 'amortization_type', 
                           'with_co_borrower', 'with_co_signer', 'fico_score', 'dti', 'borrower_fico_score', 
                           'co_borrower_1_fico_score', 'co_borrower_2_fico_score', 'co_borrower_3_fico_score', 
                           'co_signer_1_fico_score', 'co_signer_2_fico_score', 'co_signer_3_fico_score', 
                           'borrower_dti', 'co_borrower_1_dti', 'co_borrower_2_dti', 'co_borrower_3_dti', 
                           'co_signer_1_dti', 'co_signer_2_dti', 'co_signer_3_dti']

loantape_applicant_cols_short = [ "account", "brrgy", "ctzna", "usrsd", "vsnla", "stx", "lncnt", "lnprd", "intrt", 
                                 "orgtt", "mdstt", "flgtc", "flgts", "flgtd", "flgte", "lnstt", "actvs", "dlqbc", 
                                 "dysps", "amrtz", "cbrr", "csgny", "fcscr", "dtx", "brrfc", "cbrra", "cbrrg", 
                                 "cbrrn", "csgna", "csgnh", "csgno", "brrdt", "cbrrd", "cbrri", "cbrrp", "csgnc", 
                                 "csgnj", "csgnq"]

df_loantape_borrower_anly = df_loantape[loantape_borrower_cols_short].copy()
df_loantape_borrower_app_anly = df_loantape[loantape_applicant_cols_short].copy()


# LOAN TAPE FILTER
incl = ["current", "default", "delinquent", "past due"]

df_loantape_anly = df_loantape[
    df_loantape["lnstt"].str.lower().isin(incl)
]

# ------------------------------------------------------------
# R -> Python (pandas) conversion
# Preference: apply shorthand renaming FIRST, then do processing.
# df_loans -> df_loantape_anly
# ------------------------------------------------------------
import numpy as np
import pandas as pd
from io import StringIO
from pandas.tseries.offsets import DateOffset

# ------------------------------------------------------------
# Load mapping from CSV and build rename dict (table_name == "loantape")
# CSV: v2/modeling_v3/output/preprocess_data/dim_column_equivalency.csv
# ------------------------------------------------------------
MAPPING_CSV_PATH = "/Users/klemanroy/Downloads/stratus/v2/modeling_v3/output/preprocess_data/dim_column_equivalency.csv"

map_df = pd.read_csv(MAPPING_CSV_PATH)

# Keep only loantape rows
map_loantape = map_df.loc[map_df["table_name"].astype(str).str.strip().str.lower() == "loantape"].copy()
map_tmoinfo = map_df.loc[map_df["table_name"].astype(str).str.strip().str.lower() == "tmoinfo"].copy()

# Build dict: {original_column_norm -> short_code}
# (This matches your “use the short hand notation of the columns” requirement.)
rename_dict = dict(
    zip(
        map_loantape["original_column_norm"].astype(str).str.strip(),
        map_loantape["short_code"].astype(str).str.strip(),
    )
)

rename_dict_tmo = dict(
    zip(
        map_tmoinfo["original_column_norm"].astype(str).str.strip(),
        map_tmoinfo["short_code"].astype(str).str.strip(),
    )
)

# Apply renaming to df_loantape_anly (and to others later if you want)
df_loantape_anly = df_loantape_anly.rename(columns={k: v for k, v in rename_dict.items() if k in df_loantape_anly.columns})

# -----------------------------
# 1) Helpers (after renaming)
# -----------------------------
def zscore(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    mu = s.mean(skipna=True)
    sd = s.std(skipna=True, ddof=1)
    if sd == 0 or pd.isna(sd):
        return pd.Series(np.nan, index=s.index)
    return (s - mu) / sd

def safe_div(numer, denom):
    numer = pd.to_numeric(numer, errors="coerce")
    denom = pd.to_numeric(denom, errors="coerce")
    return pd.Series(np.where(denom == 0, np.nan, numer / denom), index=numer.index)

num = lambda s: pd.to_numeric(s, errors="coerce")

# -----------------------------
# 2) Data processing (using SHORT column names where available)
# -----------------------------
dateNow = pd.Timestamp.today().strftime("%Y-%m-%d")

df_loantape_anly["lncnt"] = pd.to_datetime(df_loantape_anly["lncnt"], errors="coerce", format="%Y-%m-%d")

# lncnt_%dyr = lncnt + 1 month + N years
for n in range(1, 8):
    # df_loantape_anly[f"lncnt_{n}y"] = df_loantape_anly["lncnt"] + DateOffset(months=1, years=n)
    df_loantape_anly[f"lncnt_{n}y"] = df_loantape_anly["lncnt"] + DateOffset(years=n)

# contract_year
df_loantape_anly["cntyr"] = df_loantape_anly["lncnt"].dt.year.astype("int64")

df_loantape_anly["dysps"] = (
    df_loantape_anly["dysps"]
    .astype("string")
    .str.replace(r"[^0-9\.\-]", "", regex=True)
)
df_loantape_anly["dysps"] = pd.to_numeric(df_loantape_anly["dysps"], errors="coerce")

# Defaults
df_loantape_anly["dflt90"] = np.where(df_loantape_anly["dysps"] > 90, "default", "nondefault")
df_loantape_anly["dflt120"] = np.where(df_loantape_anly["dysps"] > 120, "default", "nondefault")

df_loantape_anly["dflt120_chr"] = df_loantape_anly["dflt120"].astype(str)
df_loantape_anly["dflt90_chr"] = df_loantape_anly["dflt90"].astype(str)

df_loantape_anly["dflt120_fct"] = pd.Categorical(df_loantape_anly["dflt120_chr"], categories=["nondefault", "default"], ordered=True)
df_loantape_anly["dflt90_fct"] = pd.Categorical(df_loantape_anly["dflt90_chr"], categories=["nondefault", "default"], ordered=True)


# Extract the Credit Data in TMO
# Keywords to match (case-insensitive)
kw = ["fico", "strata", "dti", "income", "asset", "liab", "dob", "citizen"]
pattern = re.compile(r"(" + "|".join(map(re.escape, kw)) + r")", flags=re.IGNORECASE)

# Filter map_tmoinfo where original_column contains any keyword
tmoinfo_hits = map_tmoinfo.loc[
    map_tmoinfo["original_column"].astype(str).str.contains(pattern, na=False),
    ["table_name", "original_column", "original_column_norm", "short_code", "kept_as_is"]
].sort_values(["table_name", "original_column"])

# Show unique original_column values (optional)
unique_cols_og = tmoinfo_hits["original_column"].dropna().drop_duplicates()
unique_cols_sh = tmoinfo_hits["short_code"].dropna().drop_duplicates()

# print(tmoinfo_hits.to_string(index=False))
# or, if you only want the column names:
# print(unique_cols_og.to_string(index=False))

df_creditmetrics_anly = df_tmoinfo[['account'] + list(unique_cols_sh) + ["fstpy", "trngs", "grdtd", "grdtn", "actvf", "stppd"]]

df_loantape_anly = df_loantape_anly.merge(
    df_creditmetrics_anly,
    on="account",
    how="left",
    suffixes=("", "_crm")
)

# POPULATION AND DEFAULT DEFINITIONS: 
# Included:
# 1. active loans
# 2. delinquent loans
# 3. past due loans
# 4. paid-off loans (labeled as nondefault)

# Excluded:
# 1. loans under forbearance (temporary simplification) 

def make_default_type_current_dpd(
    df: pd.DataFrame,
    cutoff,
    dpd_threshold: int = 90,
    max_years: int = 7,
    label_prefix: str = "default w/in",
    after_label: str = "default after",
) -> pd.Series:
    """
    Create a categorical default_type label based on CURRENT delinquency as of cutoff,
    with timing inferred from last payment date relative to loan contract date.

    Logic (matches your prior approach):
    - base_exposure: lncnt < mtrty AND lstdd < mtrty
    - exclude paid-off: pdffd notna -> nondefault
    - current default: days_since_paid > dpd_threshold
    - bucket by year: lstdd compared to lncnt + k years
        * < 1y -> "default w/in 1yr"
        * [1y,2y) -> "default w/in 2yrs"
        * ...
        * >= max_years -> "default after {max_years}yrs"
    - else -> "nondefault"
    """

    cutoff = pd.Timestamp(cutoff)
    lstdd = df["lstdd"]
    lncnt = df["lncnt"]

    days_since_paid = (cutoff - lstdd).dt.days

    base_exposure = (lncnt < df["mtrty"]) & (lstdd < df["mtrty"])
    is_paid_off = df["pdffd"].notna()

    is_current_default = base_exposure & (~is_paid_off) & (days_since_paid > dpd_threshold)

    # Years since loan contract at the last payment date (continuous)
    years_from_contract = (lstdd - lncnt).dt.days / 365.25

    # Bin edges: [0,1), [1,2), ... [max_years-1, max_years), [max_years, +inf)
    bins = list(range(0, max_years + 1)) + [np.inf]

    # Labels: within 1..max_years, then after
    within_labels = [
        f"{label_prefix} {k}yr" if k == 1 else f"{label_prefix} {k}yrs"
        for k in range(1, max_years + 1)
    ]
    labels = within_labels + [f"{after_label} {max_years}yrs"]

    bucket = pd.cut(
        years_from_contract,
        bins=bins,
        right=False,            # [a,b)
        labels=labels,
        include_lowest=True
    ).astype("string")

    # Final label: bucket only if current default, else nondefault
    out = pd.Series(np.where(is_current_default, bucket, "nondefault"), index=df.index, dtype="string")

    # If years_from_contract is missing/invalid, bucket becomes <NA>; force to nondefault unless you want explicit "unknown"
    out = out.fillna("nondefault")

    return out

df_loantape_anly["default_type90"] = make_default_type_current_dpd(
    df_loantape_anly,
    cutoff=CUTOFF,
    dpd_threshold=90,
    max_years=7
)

df_loantape_anly["default_type120"] = make_default_type_current_dpd(
    df_loantape_anly,
    cutoff=CUTOFF,
    dpd_threshold=120,
    max_years=7
)

# Default Definitions (within 1yr / 2 yrs)

# A guardrail override - not the main credit risk model
df_loantape_anly["PD90_1yr"] = np.where(
    df_loantape_anly["default_type90"].eq("default w/in 1yr"),
    "default", "nondefault"
)

# The main credit risk model for predicting default

# A borrower is labeled as default if:
# 1. the loan is active (not past maturity),
# 2. the loan is not paid off,
# 3. the borrower is ≥90 DPD as of the cutoff date, and
# 4. the borrower’s last payment date occurred within two years of the loan contract date.

# PD90_2yr represents the probability that a borrower is currently 90 or more days past due 
# (DPD) as of the observation cutoff date, where the delinquency is attributable to a payment 
# lapse occurring within two (2) years of loan origination.

df_loantape_anly["PD90_2yr"] = np.where(
    df_loantape_anly["default_type90"].isin(["default w/in 1yr", "default w/in 2yrs"]),
    "default", "nondefault"
)

# USAGE POLICY 
# 1. PD90_2yr is the single primary risk score used for pricing and approval

# 2. PD90_1yr is a guardrail used for early-loss protection
# PD targets are not additive and are not treated as independent signals

# SOME STATISTICS
print("\n")
print("Empirical Distribution of Default 90 DPD")
dist = (
    df_loantape_anly["default_type90"]
    .value_counts(dropna=False)
    .rename_axis("default_type90")
    .reset_index(name="count")
)

dist["pct"] = dist["count"] / dist["count"].sum()

print(dist)

print("\n")
print("Default Definition Counts")

print(df_loantape_anly["PD90_1yr"].value_counts(dropna=False))
print(df_loantape_anly["PD90_2yr"].value_counts(dropna=False))

# Nesting check: PD90_1yr defaults must be a subset of PD90_2yr defaults
bad = (df_loantape_anly["PD90_1yr"].eq("default") & df_loantape_anly["PD90_2yr"].ne("default")).sum()
print("Nesting violations:", bad)
