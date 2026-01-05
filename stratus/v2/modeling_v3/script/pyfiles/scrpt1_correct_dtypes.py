import pandas as pd
from pathlib import Path
import re
import hashlib
from collections import defaultdict

BASE_DIR = "/Users/klemanroy/Downloads/stratus/v2"
OUT_DIR = Path(BASE_DIR) / "modeling_v3" / "output" / "preprocess_data"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Load actual data extracted/processed from the analytics_db
df_schedpay = pd.read_csv(f"{OUT_DIR}/schedpay_short.csv")
df_loantape = pd.read_csv(f"{OUT_DIR}/loantape_short.csv")
df_tmoinfo = pd.read_csv(f"{OUT_DIR}/tmoinfo_short.csv")
df_interestrate_hist = pd.read_csv(f"{OUT_DIR}/interestrate_hist_short.csv")
df_gaps_payment = pd.read_csv(f"{OUT_DIR}/gaps_payment_short.csv")
df_loan_infoFT = pd.read_csv(f"{OUT_DIR}/loan_infoFT_short.csv")
df_flight_recordsFT = pd.read_csv(f"{OUT_DIR}/flight_recordsFT_short.csv")
df_demographicsFT = pd.read_csv(f"{OUT_DIR}/demographicsFT_short.csv")

# -------------- Scheduled Payments Columns---------------

# Convert identifiers to string
df_schedpay["account"] = df_schedpay["account"].astype("string")
df_schedpay["schdp"]   = df_schedpay["schdp"].astype("string")

# Convert date column
df_schedpay["sched_paydate"] = pd.to_datetime(
    df_schedpay["sched_paydate"],
    errors="coerce"   # invalid parses → NaT (safer than crashing)
)
df_schedpay["yrm"] = df_schedpay["yrm"].astype("string")

# Consistent rounding for money fields
amt_cols = ["expct", "intpy", "prnpy", "drvtt", "drvnt"]
df_schedpay[amt_cols] = df_schedpay[amt_cols].round(2)

# -------------- Interest Rate Summary Columns ---------------

df_interestrate_hist["account"] = df_interestrate_hist["account"].astype("string")

intrst8_cols = ["mnntr", "avgnt", "mxntr"]
df_interestrate_hist[intrst8_cols] = df_interestrate_hist[intrst8_cols].round(3)

# -------------- Gaps in Payment Columns ---------------
df_gaps_payment["account"] = df_gaps_payment["account"].astype("string")

# -------------- Loan Information FT Columns ---------------
df_loan_infoFT["account"] = df_loan_infoFT["account"].astype("string")
df_loan_infoFT["dtlst"] = pd.to_datetime(
    df_loan_infoFT["dtlst"],
    errors="coerce"  
)
# -------------- Flight Training FT Columns ---------------
df_flight_recordsFT["account"] = df_flight_recordsFT["account"].astype("string")

flight_recordsFT_cols = ["hrs", "hrsa", "hrsb", "hrsc", "hrsd", "hrse"]

df_flight_recordsFT[flight_recordsFT_cols] = df_flight_recordsFT[flight_recordsFT_cols].apply(
    lambda s: pd.to_datetime(s, errors="coerce")
)

# -------------- Borrower Demographics FT Columns ---------------
df_demographicsFT["account"] = df_demographicsFT["account"].astype("string")
df_demographicsFT["borrowerid"] = df_demographicsFT["borrowerid"].astype("string")
df_demographicsFT["ctzns"] = df_demographicsFT["ctzns"].astype("string")
df_demographicsFT["bdy"] = pd.to_datetime(
    df_demographicsFT["bdy"],
    errors="coerce"  
)

# -------------- TMO Data Columns ---------------
df_tmoinfo["account"] = df_tmoinfo["account"].astype("string")

tmoinfo_date_cols = ["apprs", "bkngd", "bnkrp", "chrga", "clsng", "cncla", "dbx", "dtddd", "frbra", 
                     "frbrc", "frbrd", "frbrf", "frbrg", "frbri", "frbrk", "frbrl", "frbrn", "frbro", 
                     "frbrp", "frbrr", "fstpy", "grdtn", "ltgta", "lstcm", "mddt", "mddta", "lnsld", 
                     "mtrty", "pdffd", "prchs", "pyffa", "pyffc", "pyfff", "pyffg", "pyffm", "pyffn", 
                     "pyffp", "pyffr", "pyfft", "pymtd", "stppd", "sttlm", "tskrb", "tskrd", "tskrf", 
                     "tskrh", "tskrs", "tsksb", "tsksd", "tsksf", "tsksh", "tskst"]

df_tmoinfo[tmoinfo_date_cols] = df_tmoinfo[tmoinfo_date_cols].apply(
    lambda s: pd.to_datetime(s, errors="coerce")
)

# -------------- Loan Tape Columns ---------------
df_loantape["account"] = df_loantape["account"].astype("string")

loantape_date_cols = ["bnkrb", "chrga", "dfrra", "dfrrd", "dtrn", "dtrvw", "expca", "frbrs", "frbrt", 
                      "intna", "intnl", "lncnt", "lnsld", "lstdd", "mtrty", "nxtdd", "pdffd", "pscha", 
                      "pschd", "pymta", "pymtb", "pymtc", "pymte", "pymtg", "pymth", "pymti", "pymts"]

df_loantape[loantape_date_cols] = df_loantape[loantape_date_cols].apply(
    lambda s: pd.to_datetime(s, errors="coerce")
)
