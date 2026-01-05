import pandas as pd
from pathlib import Path
import re
import hashlib
from collections import defaultdict

BASE_DIR = "/Users/klemanroy/Downloads/stratus/v2"
OUT_DIR = Path(BASE_DIR) / "modeling_v3" / "output" / "preprocess_data"
OUT_DIR.mkdir(parents=True, exist_ok=True)

demographicsFT_file = "aa_borrower_demographics_FT_extract.csv"
flight_recordsFT_file = "aa_borrower_flight_records_FT_extract.csv"
loan_infoFT_file = "aa_borrower_loan_information_FT_extract.csv"
gaps_payment_file = "aa_gaps_in_payment_extract.csv"
interest_rate_hist_file = "aa_interest_rate_summary_extract.csv"
tmo_info_file = "aa_latest_tmo_loan_information_extract.csv"
tmo_all_file = "aa_latest_tmo_all_loan_extract.csv"
loantape_file = "aa_loan_tape_extract.csv"
schedpay_file = "aa_scheduled_payment_extract.csv"

# Load actual data extracted/processed from the analytics_db
df_schedpay = pd.read_csv(f"{BASE_DIR}/data_extract_from_sql/{schedpay_file}")
df_loantape = pd.read_csv(f"{BASE_DIR}/data_extract_from_sql/{loantape_file}")
df_tmoinfo = pd.read_csv(f"{BASE_DIR}/data_extract_from_sql/{tmo_info_file}")
df_tmoall = pd.read_csv(f"{BASE_DIR}/data_extract_from_sql/{tmo_all_file}")
df_interestrate_hist = pd.read_csv(f"{BASE_DIR}/data_extract_from_sql/{interest_rate_hist_file}")
df_gaps_payment = pd.read_csv(f"{BASE_DIR}/data_extract_from_sql/{gaps_payment_file}")
df_loan_infoFT = pd.read_csv(f"{BASE_DIR}/data_extract_from_sql/{loan_infoFT_file}")
df_flight_recordsFT = pd.read_csv(f"{BASE_DIR}/data_extract_from_sql/{flight_recordsFT_file}")
df_demographicsFT = pd.read_csv(f"{BASE_DIR}/data_extract_from_sql/{demographicsFT_file}")

# -------------- Scheduled Payments Columns---------------
schedpay_OGcols = ['Account', 'Date', 'Payment_Schedule_ID', 'Expected_Payment_Amount',
       'Interest_Payment', 'Principal_Payment', 'Total', 'YearMonth', 'rnPS',
       'rn', 'ScheduleYearNbr', 'InterestRate']
schedpay_NWcols = ['account', 'sched_paydate', 'sched_payID', 'expected_payamt',
       'interest_payamt', 'principal_payamt', 'derived_total_payamt', 'yearmo', 'rnPS',
       'rn', 'sched_yrnbr', 'derived_interest_rate']

# -------------- Interest Rate Summary Columns ---------------
interestrat_hist_OGcols = ['Account', 'ScheduleYearNbr', 'minYearMo', 'maxYearMo',
       'minInterestRate', 'avgInterestRate', 'maxInterestRate']
interestrat_hist_NWcols = ['account', 'schedyrnbr', 'min_yearmo', 'max_yearmo',
       'min_interestrate', 'avg_interestrate', 'max_interestrate']

# -------------- Gaps in Payment Columns ---------------
gaps_payment_OGcols = ['Account', 'ScheduleYearNbr', 'PaidInd', 'minYM_yrly', 'maxYM_yrly',
       'DPD_yrly', 'DPD_alltime', 'DPD60Ind_yrly', 'DPD90Ind_yrly',
       'DPD120Ind_yrly', 'DPD60Ind_alltime', 'DPD90Ind_alltime',
       'DPD120Ind_alltime']
gaps_payment_NWcols =  ['account', 'schedyrnbr', 'paid_ind', 'min_yearmo_yrly', 'max_yearmo_yrly',
       'dpd_yrly', 'dpd_alltime', 'dpd60_ind_yrly', 'dpd90I_ind_yrly',
       'dpd120_ind_yrly', 'dpd60_ind_alltime', 'dpd90_ind_alltime',
       'dpd120_ind_alltime']

# -------------- Loan Information FT Columns ---------------

loan_infoFT_OGcols = ['Account', 'Loan_Amount', 'Funding_Status', 'Date_Last_Funding']
loan_infoFT_NWcols = ['account', 'loan_amount', 'funding_status', 'date_last_funding']

# -------------- Flight Training FT Columns ---------------

flight_recordsFT_OGcols = ['Account', 'School', 'NPS', 'Flight_School_State', 'Flight_School_Tier',
       'Flight_School_Tier_Clean', 'Program', 'Program_Clean', 'Particulars',
       'Total_Hours', '0_hours', '40_50_hours', '70_120_hours',
       '170_180_hours', '200_220_hours', '230_hours',
       'Num_Months_Training_Completed', 'Medical_Class', 'PPL', 'IFR',
       'COMM_SE', 'COMM_Multi', 'CFI', 'CFII', 'MEI', 'ATP', 'OTHER_CERT_FAA']
flight_recordsFT_NWcols = ['account', 'school_name', 'nps', 'flight_school_state', 'flight_school_tier',
       'flight_school_tier_clean', 'program', 'program_clean', 'particulars',
       'total_hours', '0_hours', '40_50_hours', '70_120_hours',
       '170_180_hours', '200_220_hours', '230_hours',
       'num_months_training_completed', 'medical_class', 'ppl', 'ifr',
       'comm_se', 'comm_multi', 'cf1', 'cf2', 'mei', 'atp', 'other_cert_faa']

# -------------- Borrower Demographics FT Columns ---------------

demographicsFT_OGcols = ['Account', 'BorrowerID', 'Name', 'Citizenship', 'Country', 'Birthday',
       'Age', 'Employment']
demographicsFT_NWcols = ['account', 'borrowerID', 'name', 'citizenship', 'country', 'birthday',
       'age', 'employment']

# -------------- TMO Data Columns ---------------

tmoinfo_OGcols = ['ach', 'hold', 'account', 'borrower_name', 'by_last_name', 'first_name',
 'mi', 'last_name', 'interest_paid_to', 'payment_due_date', 'payment_frequency', 'regular_payment', 
 'apply_to_p_i', 'apply_to_reserve', 'apply_to_impound', 'apply_to_other', 'maturity_date', 'term_left', 
 'days_late', 'paid_off_date', 'note_rate', 'sold_rate', 'loan_priority', 'principal_balance', 'trust_balance',
 'impound_balance', 'reserve_balance', 'unpaid_late_charges', 'unpaid_charges', 'unpaid_interest', 'street', 
 'city', 'state', 'zip_code', 'home_phone', 'cell_phone', 'tin', 'loan_type', 'rate_type', 'email_address', 
 'aggregate_appraised_value', 'closing_date', 'first_payment_date', 'next_revision', 'original_balance', 
 'unearned_discount', 'loan_code', 'aggregate_senior_liens', 'dob', 'primary_servicing_associate', 
 'secondary_servicing_associate', 'last_communication_date', 'annual_percentage_rate', 'total_of_payments', 
 'original_total_loan_amount', 'original_amount_financed', 'original_initial_finance_charges', 
 'final_total_amount_funded_to_schools', 'modified_1_total_loan_amount', 'modified_1_amount_financed', 
 'loan_product', 'loan_owner', 'borrower_fico_score', 'borrower_strata_score', 'borrower_dti', 
 'borrower_total_monthly_income', 'borrower_total_assets', 'borrower_total_liabilities', 'co_borrower_1_full_name',
 'co_borrower_1_fico_score', 'co_borrower_1_strata_score', 'co_borrower_1_dti', 'co_borrower_1_total_monthly_income',
 'co_borrower_1_total_assets', 'co_borrower_1_total_liabilities', 'co_borrower_2_full_name', 'co_borrower_2_fico_score',
 'co_borrower_2_strata_score', 'co_borrower_2_dti', 'co_borrower_2_total_monthly_income', 'co_borrower_2_total_assets',
 'co_borrower_2_total_liabilities', 'co_borrower_3_full_name', 'co_borrower_3_fico_score', 'co_borrower_3_strata_score',
 'co_borrower_3_dti', 'co_borrower_3_total_monthly_income', 'co_borrower_3_total_assets', 'co_borrower_3_total_liabilities',
 'co_signer_1_full_name', 'co_signer_1_fico_score', 'co_signer_1_strata_score', 'co_signer_1_dti',
 'co_signer_1_total_monthly_income', 'co_signer_1_total_assets', 'co_signer_1_total_liabilities', 'co_signer_2_full_name',
 'co_signer_2_fico_score', 'co_signer_2_strata_score', 'co_signer_2_dti', 'co_signer_2_total_monthly_income',
 'co_signer_2_total_assets', 'co_signer_2_total_liabilities', 'co_signer_3_full_name', 'co_signer_3_fico_score',
 'co_signer_3_strata_score', 'co_signer_3_dti', 'co_signer_3_total_monthly_income', 'co_signer_3_total_assets',
 'co_signer_3_total_liabilities', 'is_a_citizen', 'is_us_perm_resident_or_greencard_holder', 'is_visa_only',
 'modified_1_finance_charges', 'modified_1_date', 'modified_1_reason', 'modified_2_total_loan_amount',
 'modified_2_amount_financed', 'forbearance_1_start_date', 'forbearance_1_end_date', 'forbearance_1_amount',
 'forbearance_2_start_date', 'forbearance_2_end_date', 'forbearance_2_amount', 'modified_2_finance_charges',
 'modified_2_date', 'modified_2_reason', 'loan_sold_date', 'loan_sold_rate',
 'declared_bankruptcy', 'bankruptcy_date', 'date_added', 'purchase_date',
 'booking_date', 'task_1_start_date', 'task_1_resolution_date', 'task_1_category',
 'task_1_status', 'task_1_details', 'task_1_result', 'task_2_start_date',
 'task_2_resolution_date', 'task_2_category', 'task_2_status', 'task_2_details',
 'task_2_result', 'task_3_start_date', 'task_3_resolution_date', 'task_3_category',
 'task_3_status', 'task_3_details', 'task_3_result', 'task_4_start_date',
 'task_4_resolution_date', 'task_4_category', 'task_4_status', 'task_4_details',
 'task_4_result', 'task_5_start_date', 'task_5_resolution_date', 'task_5_category', 'task_5_status',
 'task_5_details', 'task_5_result', 'payoff_request_1_start_date', 'payoff_request_1_completion_date', 
 'payoff_request_1_status', 'payoff_request_1_date_computation_sent', 'payoff_request_1_details', 'payoff_request_1_result',
 'payoff_request_2_start_date', 'payoff_request_2_completion_date', 'payoff_request_2_status', 'payoff_request_2_date_computation_sent',
 'payoff_request_2_details', 'payoff_request_2_result', 'first_and_last_name', 'work_phone', 'fax_phone',
 'property_description', 'property_street', 'property_city', 'property_state', 'property_zip', 'property_county',
 'property_type', 'property_occupancy', 'property_ltv', 'property_apn', 'calculated_ltv', 'appraisal_date',
 'loan_officer', 'preferred_communication', 'forbearance_2b_start_date', 'forbearance_2b_end_date',
 'forbearance_2b_amount', 'training_status', 'forbearance_3_start_date', 'forbearance_3_amount',
 'forbearance_3_end_date', 'forbearance_4_start_date', 'forbearance_4_amount', 'forbearance_4_end_date',
 'forbearance_5_start_date', 'forbearance_5_amount', 'forbearance_5_end_date', 'co_borrower_1_phone_number',
 'co_borrower_2_phone_number', 'co_borrower_3_phone_number', 'borrower_citizenship', 'co_borrower_1_citizenship',
 'co_borrower_2_citizenship', 'co_borrower_3_citizenship', 'charged_off', 'charged_off_date', 'litigation',
 'litigation_date', 'graduated', 'graduation_date', 'active_in_flight_training', 'stopped_flight_training_date',
 'cancelled', 'cancelled_date', 'pledged_to', 'planned_to_be_pledged_to', 'borrower_total_liquid_assets',
 'co_borrower_1_total_liquid_assets', 'co_borrower_2_total_liquid_assets', 'co_signer_1_total_liquid_assets', 
 'co_signer_2_total_liquid_assets', 'co_borrower_3_total_liquid_assets', 'co_signer_3_total_liquid_assets', 
 'language_preference', 'school_name', 'co_borrower_1_middle_name', 'co_borrower_1_association_code',
 'co_borrower_2_middle_name', 'co_borrower_2_association_code', 'co_borrower_3_middle_name', 'co_borrower_3_association_code', 
 'outsanding_balance_as_of_settlement_date', 'settlement_date', 'type_of_settlement', 'settlement_amount',
 'monthly_installment', 'settlement_status', 'settlement_remarks', 'co_borrower_1_first_name',
 'co_borrower_1_last_name', 'co_borrower_1_generation', 'co_borrower_1_1st_line_of_address', 'co_borrower_1_2nd_line_of_address',
 'co_borrower_1_city', 'co_borrower_1_state', 'co_borrower_1_zip_code', 'co_borrower_1_email',
 'co_borrower_1_ssn', 'co_borrower_1_dob', 'co_borrower_2_first_name', 'co_borrower_2_last_name', 'co_borrower_2_generation',
 'co_borrower_2_1st_line_of_address', 'co_borrower_2_2nd_line_of_address', 'co_borrower_2_city', 'co_borrower_2_state',
 'co_borrower_2_zip_code', 'co_borrower_2_email', 'co_borrower_2_dob', 'co_borrower_2_ssn', 'co_borrower_3_first_name',
 'co_borrower_3_last_name', 'co_borrower_3_generation', 'co_borrower_3_1st_line_of_address', 'co_borrower_3_2nd_line_of_address',
 'co_borrower_3_city', 'co_borrower_3_state', 'co_borrower_3_zip_code', 'co_borrower_3_email',
 'co_borrower_3_ssn', 'co_borrower_3_dob', 'payoff_in_terms_end_date', 'payoff_in_terms_monthly_payment',
 'payoff_in_terms_start_date', 'payoff_in_terms_end_date_1', 'payoff_in_terms_monthly_payment_1', 
 'payoff_in_terms_start_date_1', 'rn']

tmoinfo_NWcols = ['ach', 'hold', 'account', 'borrower_name', 'by_last_name', 'first_name',
 'mi', 'last_name', 'interest_paid_to', 'payment_due_date', 'payment_frequency', 'regular_payment', 
 'apply_to_p_i', 'apply_to_reserve', 'apply_to_impound', 'apply_to_other', 'maturity_date', 'term_left', 
 'days_late', 'paid_off_date', 'note_rate', 'sold_rate', 'loan_priority', 'principal_balance', 'trust_balance',
 'impound_balance', 'reserve_balance', 'unpaid_late_charges', 'unpaid_charges', 'unpaid_interest', 'street', 
 'city', 'state', 'zip_code', 'home_phone', 'cell_phone', 'tin', 'loan_type', 'rate_type', 'email_address', 
 'aggregate_appraised_value', 'closing_date', 'first_payment_date', 'next_revision', 'original_balance', 
 'unearned_discount', 'loan_code', 'aggregate_senior_liens', 'dob', 'primary_servicing_associate', 
 'secondary_servicing_associate', 'last_communication_date', 'annual_percentage_rate', 'total_of_payments', 
 'original_total_loan_amount', 'original_amount_financed', 'original_initial_finance_charges', 
 'final_total_amount_funded_to_schools', 'modified_1_total_loan_amount', 'modified_1_amount_financed', 
 'loan_product', 'loan_owner', 'borrower_fico_score', 'borrower_strata_score', 'borrower_dti', 
 'borrower_total_monthly_income', 'borrower_total_assets', 'borrower_total_liabilities', 'co_borrower_1_full_name',
 'co_borrower_1_fico_score', 'co_borrower_1_strata_score', 'co_borrower_1_dti', 'co_borrower_1_total_monthly_income',
 'co_borrower_1_total_assets', 'co_borrower_1_total_liabilities', 'co_borrower_2_full_name', 'co_borrower_2_fico_score',
 'co_borrower_2_strata_score', 'co_borrower_2_dti', 'co_borrower_2_total_monthly_income', 'co_borrower_2_total_assets',
 'co_borrower_2_total_liabilities', 'co_borrower_3_full_name', 'co_borrower_3_fico_score', 'co_borrower_3_strata_score',
 'co_borrower_3_dti', 'co_borrower_3_total_monthly_income', 'co_borrower_3_total_assets', 'co_borrower_3_total_liabilities',
 'co_signer_1_full_name', 'co_signer_1_fico_score', 'co_signer_1_strata_score', 'co_signer_1_dti',
 'co_signer_1_total_monthly_income', 'co_signer_1_total_assets', 'co_signer_1_total_liabilities', 'co_signer_2_full_name',
 'co_signer_2_fico_score', 'co_signer_2_strata_score', 'co_signer_2_dti', 'co_signer_2_total_monthly_income',
 'co_signer_2_total_assets', 'co_signer_2_total_liabilities', 'co_signer_3_full_name', 'co_signer_3_fico_score',
 'co_signer_3_strata_score', 'co_signer_3_dti', 'co_signer_3_total_monthly_income', 'co_signer_3_total_assets',
 'co_signer_3_total_liabilities', 'is_a_citizen', 'is_us_perm_resident_or_greencard_holder', 'is_visa_only',
 'modified_1_finance_charges', 'modified_1_date', 'modified_1_reason', 'modified_2_total_loan_amount',
 'modified_2_amount_financed', 'forbearance_1_start_date', 'forbearance_1_end_date', 'forbearance_1_amount',
 'forbearance_2_start_date', 'forbearance_2_end_date', 'forbearance_2_amount', 'modified_2_finance_charges',
 'modified_2_date', 'modified_2_reason', 'loan_sold_date', 'loan_sold_rate',
 'declared_bankruptcy', 'bankruptcy_date', 'date_added', 'purchase_date',
 'booking_date', 'task_1_start_date', 'task_1_resolution_date', 'task_1_category',
 'task_1_status', 'task_1_details', 'task_1_result', 'task_2_start_date',
 'task_2_resolution_date', 'task_2_category', 'task_2_status', 'task_2_details',
 'task_2_result', 'task_3_start_date', 'task_3_resolution_date', 'task_3_category',
 'task_3_status', 'task_3_details', 'task_3_result', 'task_4_start_date',
 'task_4_resolution_date', 'task_4_category', 'task_4_status', 'task_4_details',
 'task_4_result', 'task_5_start_date', 'task_5_resolution_date', 'task_5_category', 'task_5_status',
 'task_5_details', 'task_5_result', 'payoff_request_1_start_date', 'payoff_request_1_completion_date', 
 'payoff_request_1_status', 'payoff_request_1_date_computation_sent', 'payoff_request_1_details', 'payoff_request_1_result',
 'payoff_request_2_start_date', 'payoff_request_2_completion_date', 'payoff_request_2_status', 'payoff_request_2_date_computation_sent',
 'payoff_request_2_details', 'payoff_request_2_result', 'first_and_last_name', 'work_phone', 'fax_phone',
 'property_description', 'property_street', 'property_city', 'property_state', 'property_zip', 'property_county',
 'property_type', 'property_occupancy', 'property_ltv', 'property_apn', 'calculated_ltv', 'appraisal_date',
 'loan_officer', 'preferred_communication', 'forbearance_2b_start_date', 'forbearance_2b_end_date',
 'forbearance_2b_amount', 'training_status', 'forbearance_3_start_date', 'forbearance_3_amount',
 'forbearance_3_end_date', 'forbearance_4_start_date', 'forbearance_4_amount', 'forbearance_4_end_date',
 'forbearance_5_start_date', 'forbearance_5_amount', 'forbearance_5_end_date', 'co_borrower_1_phone_number',
 'co_borrower_2_phone_number', 'co_borrower_3_phone_number', 'borrower_citizenship', 'co_borrower_1_citizenship',
 'co_borrower_2_citizenship', 'co_borrower_3_citizenship', 'charged_off', 'charged_off_date', 'litigation',
 'litigation_date', 'graduated', 'graduation_date', 'active_in_flight_training', 'stopped_flight_training_date',
 'cancelled', 'cancelled_date', 'pledged_to', 'planned_to_be_pledged_to', 'borrower_total_liquid_assets',
 'co_borrower_1_total_liquid_assets', 'co_borrower_2_total_liquid_assets', 'co_signer_1_total_liquid_assets', 
 'co_signer_2_total_liquid_assets', 'co_borrower_3_total_liquid_assets', 'co_signer_3_total_liquid_assets', 
 'language_preference', 'school_name', 'co_borrower_1_middle_name', 'co_borrower_1_association_code',
 'co_borrower_2_middle_name', 'co_borrower_2_association_code', 'co_borrower_3_middle_name', 'co_borrower_3_association_code', 
 'outsanding_balance_as_of_settlement_date', 'settlement_date', 'type_of_settlement', 'settlement_amount',
 'monthly_installment', 'settlement_status', 'settlement_remarks', 'co_borrower_1_first_name',
 'co_borrower_1_last_name', 'co_borrower_1_generation', 'co_borrower_1_1st_line_of_address', 'co_borrower_1_2nd_line_of_address',
 'co_borrower_1_city', 'co_borrower_1_state', 'co_borrower_1_zip_code', 'co_borrower_1_email',
 'co_borrower_1_ssn', 'co_borrower_1_dob', 'co_borrower_2_first_name', 'co_borrower_2_last_name', 'co_borrower_2_generation',
 'co_borrower_2_1st_line_of_address', 'co_borrower_2_2nd_line_of_address', 'co_borrower_2_city', 'co_borrower_2_state',
 'co_borrower_2_zip_code', 'co_borrower_2_email', 'co_borrower_2_dob', 'co_borrower_2_ssn', 'co_borrower_3_first_name',
 'co_borrower_3_last_name', 'co_borrower_3_generation', 'co_borrower_3_1st_line_of_address', 'co_borrower_3_2nd_line_of_address',
 'co_borrower_3_city', 'co_borrower_3_state', 'co_borrower_3_zip_code', 'co_borrower_3_email',
 'co_borrower_3_ssn', 'co_borrower_3_dob', 'payoff_in_terms_end_date', 'payoff_in_terms_monthly_payment',
 'payoff_in_terms_start_date', 'payoff_in_terms_end_date_1', 'payoff_in_terms_monthly_payment_1', 
 'payoff_in_terms_start_date_1', 'rn']

# -------------- Loan Tape Columns ---------------

loantape_OGcols = ['Account', 'Borrower Age (Y)', 'Citizen?', 'US Permanent Resident/Greencard holder', 'Visa Only',
 'State', 'BK Status', 'BK date', 'Loan Contract Date', 'Loan Age (M)', 'Maturity Date', 'Remaining Term (M)',
 'Loan Product', 'Interest Rate Type', 'Principal Balance', 'Actual Balance', 'Original Total Loan Amount', 'Principal Payoff',
 'Principal Payments', 'Principal Paydown', 'Discount to Principal Balance', 'Recapitalized Interest', 'Principal Adjustments', 
 'Original Amount to be Funded to Schools', 'Refund from School', 'Reduce Amount to be Funded to Schools', 
 'Discount to Amount to be Funded to Schools', 'Original Points', 'Reduce Points', 'Discount to Points',
 'Amount Funded to Schools', 'Actual Amount Funded to Schools', 'Current Total Loan Amount', 'Current Amount to be Funded to Schools',
 'Current Points', 'Modified Status', 'Price of Loan', 'Loan Sold', 'Loan Owner', 'Pledged To',
 'Planned To Be Pledged To', 'Loan Sold Date', 'Loan Sold Rate', 'Flight School', 'Flight School State',
 'Flight School Program', 'Flight School Program Total Hours', 'Flight Training 0 Hours', 
 'Flight Training 40/50-hour Milestone', 'Flight Training 70/120-hour Milestone', 'Flight Training 170/180-hour Milestone',
 'Flight Training 200/220-hour Milestone', 'Flight Training 230-hour Milestone', 'Expected First Payment Date', 
 'Current Monthly Payment', 'Current Interest Rate', 'Paid Off Date', 'Last Due Date Paid',
 'Next Due Date', 'Loan Status', 'Active Status', 'Charged-off Status', 'Charged-off Date',
 'Consolidated with', 'Delinquency Bucket', 'Days Past Due', 'Amortization Type', 'Deferred Schedule Start Date',
 'Deferred Schedule End Date', 'Interest Only Schedule Start Date', 'Interest Only Schedule End Date', 'P&I Schedule Start Date',
 'P&I Schedule End Date', 'Payment Schedule 1 Start Date', 'Payment Schedule 1 End Date', 'Payment Schedule 2 Start Date',
 'Payment Schedule 2 End Date','Payment Schedule 3 Start Date','Payment Schedule 3 End Date', 'Payment Schedule 4 Start Date',
 'Payment Schedule 4 End Date', 'Payment Schedule 1 Amount', 'Payment Schedule 2 Amount', 'Payment Schedule 3 Amount', 
 'Payment Schedule 4 Amount', 'Payment Schedule 1 Note Rate', 'Payment Schedule 2 Note Rate',
 'Payment Schedule 3 Note Rate', 'Payment Schedule 4 Note Rate', 'With Co Borrower', 'With Co Signer',
 'FICO Score', 'STRATA Score', 'DTI', 'Forbearance Start Date', 'Forbearance End Date', 'Forbearance Amount',
 'Borrower FICO Score', 'Co Borrower 1 FICO Score', 'Co Borrower 2 FICO Score', 'Co Borrower 3 FICO Score',
 'Co Signer 1 FICO Score', 'Co Signer 2 FICO Score', 'Co Signer 3 FICO Score', 'Borrower STRATA Score',
 'Co Borrower 1 STRATA Score', 'Co Borrower 2 STRATA Score', 'Co Borrower 3 STRATA Score', 'Co Signer 1 STRATA Score',
 'Co Signer 2 STRATA Score', 'Co Signer 3 STRATA Score', 'Borrower DTI', 'Co Borrower 1 DTI', 'Co Borrower 2 DTI',
 'Co Borrower 3 DTI', 'Co Signer 1 DTI', 'Co Signer 2 DTI', 'Co Signer 3 DTI', 'tal_db', 'tlh_db',
 '@date_run', '@date_review_tlh']

loantape_NWcols = ['account', 'borrower_age_yrs', 'citizenship_ind', 'usresident_greencard_ind', 'visa_only',
 'state', 'bankrupt_status', 'bankrupt_date', 'loan_contract_date', 'loan_age_months', 'maturity_date', 'remaining_term_months', 
 'loan_product', 'interest_rate_type', 'principal_balance', 'actual_balance', 'original_total_loan_amount', 'principal_payoff',
 'principal_payments', 'principal_paydown', 'discount_principal_balance', 'recapitalized_interest', 'principal_adjustments',
 'original_amount_2fund_2schools', 'refund_from_school', 'reduce_amount_2und_2schools', 
 'discount_2amount_2fund_2schools', 'original_points', 'reduce_points', 'discount_points',
 'amount_funded_schools', 'actual_amount_funded_2schools', 'current_total_loan_amount', 'current_amount_2fund_2schools',
 'current_points', 'modified_status', 'loan_price', 'loan_sold', 'loan_owner', 'pledged_to',
 'planned_2pledged_to', 'loan_sold_date', 'loan_sold_rate', 'flight_school', 'flight_school_state',
 'flight_school_program', 'flight_school_program_total_hours', 'flight_training_0_hours',
 'flight_training_40_50_hour_milestone',   'flight_training_70_120_hour_milestone', 'flight_training_170_180_hour_milestone',
 'flight_training_200_220_hour_milestone', 'flight_training_230_hour_milestone', 'expected_first_payment_date',
 'current_monthly_payment', 'current_interest_rate', 'paid_off_date', 'last_due_date_paid',
 'next_due_date', 'loan_status', 'active_status', 'charged_off_status', 'charged_off_date',
 'consolidated_with', 'delinquency_bucket', 'days_past_due', 'amortization_type', 'deferred_schedule_start_date',
 'deferred_schedule_end_date', 'interest_only_schedule_start_date', 'interest_only_schedule_end_date', 'pi_schedule_start_date',
 'pi_schedule_end_date', 'payment_schedule_1_start_date', 'payment_schedule_1_end_date', 'payment_schedule_2_start_date', 
 'payment_schedule_2_end_date', 'payment_schedule_3_start_date', 'payment_schedule_3_end_date', 'payment_schedule_4_start_date',
 'payment_schedule_4_end_date', 'payment_schedule_1_amount', 'payment_schedule_2_amount', 'payment_schedule_3_amount',
 'payment_schedule_4_amount', 'payment_schedule_1_note_rate', 'payment_schedule_2_note_rate',
 'payment_schedule_3_note_rate', 'payment_schedule_4_note_rate', 'with_co_borrower', 'with_co_signer',
 'fico_score', 'strata_score', 'dti', 'forbearance_start_date', 'forbearance_end_date', 'forbearance_amount',
 'borrower_fico_score', 'co_borrower_1_fico_score', 'co_borrower_2_fico_score', 'co_borrower_3_fico_score', 
 'co_signer_1_fico_score', 'co_signer_2_fico_score', 'co_signer_3_fico_score', 'borrower_strata_score',
 'co_borrower_1_strata_score', 'co_borrower_2_strata_score', 'co_borrower_3_strata_score', 'co_signer_1_strata_score',
 'co_signer_2_strata_score', 'co_signer_3_strata_score', 'borrower_dti', 'co_borrower_1_dti', 'co_borrower_2_dti',
 'co_borrower_3_dti', 'co_signer_1_dti', 'co_signer_2_dti', 'co_signer_3_dti', 'tal_db', 'tlh_db',
 'date_run', 'date_review_tlh']

# -----------------------------
def validate_and_rename(df, og_cols, new_cols):
    """
    Validate that all og_cols exist in df, restrict to og_cols only,
    rename them to new_cols, and return the updated DataFrame.

    Parameters:
        df (pd.DataFrame): input dataframe
        og_cols (list): list of original (expected) column names
        new_cols (list): list of final/replacement column names

    Returns:
        pd.DataFrame: processed dataframe with restricted and renamed columns
    """
    # Validate lengths
    if len(og_cols) != len(new_cols):
        raise ValueError("Length of og_cols and new_cols must match.")

    # Check for missing columns
    missing_cols = [c for c in og_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Restrict to only OG columns
    df_out = df[og_cols].copy()

    # Build rename map and apply
    rename_map = dict(zip(og_cols, new_cols))
    df_out = df_out.rename(columns=rename_map)

    return df_out
# -----------------------------

# -----------------------------
# 1) Helpers: tokenization + mnemonic abbreviations
# -----------------------------
_SPLIT_RE = re.compile(r"[^a-z0-9]+")
_VOWELS = set("aeiou")

# Common expansions to keep codes meaningful
_ABBR = {
    "account": "acct",
    "borrower": "borr",
    "co": "co",
    "signer": "sgnr",
    "cosigner": "sgnr",
    "coborrower": "cobr",
    "date": "dt",
    "birthday": "bday",
    "payment": "pymt",
    "pay": "pay",
    "principal": "prin",
    "interest": "int",
    "amount": "amt",
    "balance": "bal",
    "total": "tot",
    "rate": "rt",
    "year": "yr",
    "month": "mo",
    "yearmo": "yrmo",
    "status": "stat",
    "indicator": "ind",
    "ind": "ind",
    "average": "avg",
    "minimum": "min",
    "maximum": "max",
    "delinquency": "dlq",
    "days": "dys",
    "past": "pst",
    "due": "due",
    "dpd": "dpd",
    "school": "schl",
    "flight": "flgt",
    "program": "prgm",
    "training": "trng",
    "hours": "hrs",
    "fico": "fico",
    "strata": "strt",
    "dti": "dti",
    "ltv": "ltv",
    "apr": "apr",
    "zip": "zip",
    "state": "st",
    "city": "cty",
    "street": "strt",
    "email": "eml",
    "phone": "phn",
    "home": "home",
    "cell": "cell",
    "work": "work",
    "first": "fst",
    "last": "lst",
    "middle": "mid",
    "name": "name",
    "id": "id",
    "tier": "tier",
    "clean": "cln",
    "derived": "drv",
    "original": "org",
    "current": "cur",
    "modified": "mod",
    "sold": "sold",
    "owner": "ownr",
    "priority": "prio",
}

# Words we usually skip when building mnemonics
_STOP = {
    "the", "and", "or", "of", "to", "for", "in", "on", "as", "is",
    "with", "without", "by", "from", "at", "alltime", "all",
}

def _normalize(col: str) -> str:
    return col.strip().lower()

def _tokens(col: str):
    s = _normalize(col)
    parts = [p for p in _SPLIT_RE.split(s) if p]
    # break snakecase-ish chunks like "co_borrower_1" already handled by split
    return [p for p in parts if p not in _STOP]

def _compress_letters(s: str) -> str:
    """Remove non-letters, then optionally drop vowels after first letter for compactness."""
    letters = re.sub(r"[^a-z]", "", s.lower())
    if not letters:
        return ""
    # keep first letter + consonants
    out = [letters[0]]
    out.extend([ch for ch in letters[1:] if ch not in _VOWELS])
    return "".join(out)

def _mnemonic_base(col: str, min_len=3, max_len=5) -> str:
    """
    Build a mnemonic alpha-only base code, 3-5 chars if possible, before collision handling.
    """
    toks = _tokens(col)

    # Apply abbreviation map where possible
    norm_toks = []
    for t in toks:
        # keep numeric tokens out of mnemonic (we'll encode collisions instead)
        if t.isdigit():
            continue
        norm_toks.append(_ABBR.get(t, t))

    # If nothing left, fallback
    if not norm_toks:
        return ""

    # Strategy:
    # - prefer 1-2 informative tokens
    # - produce compact alpha-only string
    # - then trim/pad to 3-5
    # Examples:
    #   "principal_balance" -> "prnbal" -> trim to "prnba" (5)
    #   "loan_amount" -> "loanamt" -> "lnamt" -> pad/trim
    joined = "".join(norm_toks[:3])  # keep first up to 3 tokens for meaning
    comp = _compress_letters(joined)

    # Ensure alpha-only
    comp = re.sub(r"[^a-z]", "", comp)

    # Enforce length 3-5
    if len(comp) >= min_len:
        return comp[:max_len]

    # If too short, add more token letters
    extra = "".join(_compress_letters(t) for t in norm_toks[3:6])
    comp2 = (comp + extra)
    comp2 = re.sub(r"[^a-z]", "", comp2)
    if len(comp2) >= min_len:
        return comp2[:max_len]

    # Still too short: pad with 'x'
    return (comp2 + "xxx")[:min_len]

def _hash_letters(col: str, length: int) -> str:
    """
    Deterministic letters-only fallback using hash -> base26 letters.
    """
    h = hashlib.md5(col.encode("utf-8")).digest()
    # convert first bytes to an int, then base26
    n = int.from_bytes(h[:6], "big")
    letters = []
    for _ in range(length):
        n, r = divmod(n, 26)
        letters.append(chr(ord("a") + r))
    return "".join(letters)

def _resolve_collision(base: str, used: set, col: str, min_len=3, max_len=5) -> str:
    """
    If base already used for a DIFFERENT column, create a new code:
    - try base + suffix letter(s) within max_len
    - else fallback to hashed letters (3-5)
    """
    if base and base not in used:
        return base

    # Try suffix letters A..Z (as lowercase) while staying within max_len
    if base:
        for ch in "abcdefghijklmnopqrstuvwxyz":
            cand = (base[: max_len - 1] + ch) if max_len >= 4 else (base[:3])  # max_len always >=3
            if cand not in used and len(cand) >= min_len:
                return cand

        # Try 2-letter suffix if space allows
        if max_len >= 5:
            stem = base[: max_len - 2]
            for a in "abcdefghijklmnopqrstuvwxyz":
                for b in "abcdefghijklmnopqrstuvwxyz":
                    cand = stem + a + b
                    if cand not in used:
                        return cand

    # Fallback: hash-derived letters; try lengths 3..5
    for L in range(min_len, max_len + 1):
        cand = _hash_letters(col, L)
        if cand not in used:
            return cand

    # Worst-case: keep trying hashed variants with salt
    salt = 0
    while True:
        salt += 1
        cand = _hash_letters(f"{col}__{salt}", max_len)
        if cand not in used:
            return cand


# -----------------------------
# 2) Main: build GLOBAL mapping across multiple tables
# -----------------------------
def build_global_column_codes(
    tables_cols: dict[str, list[str]],
    keep_as_is_by_table: dict[str, set[str]] | None = None,
    min_len: int = 3,
    max_len: int = 5,
) -> dict[str, dict[str, str]]:
    """
    Returns: {table_name: {old_col: new_code_or_kept_col}}
    Rules:
      - Global uniqueness across all columns, except:
        if the original column name is identical, it gets the same code everywhere.
      - Per-table keep-as-is overrides mapping (exact match after lowercasing).
      - Codes are alpha-only, length 3-5.
    """
    keep_as_is_by_table = keep_as_is_by_table or {}

    # Global registry: normalized original -> code
    global_code_for_col: dict[str, str] = {}
    used_codes: set[str] = set()

    # Also track which originals produced which code (for debugging)
    # code_to_cols = defaultdict(set)

    out: dict[str, dict[str, str]] = {}

    for table, cols in tables_cols.items():
        keep_set = {c.lower() for c in keep_as_is_by_table.get(table, set())}
        table_map: dict[str, str] = {}

        for col in cols:
            col_norm = _normalize(col)

            # Keep-as-is (but keep original casing as provided in input list)
            if col_norm in keep_set:
                table_map[col] = col_norm  # standardize to lowercase per your style
                continue

            # If we already assigned a global code for this exact original name, reuse it
            if col_norm in global_code_for_col:
                table_map[col] = global_code_for_col[col_norm]
                continue

            # Create new global code
            base = _mnemonic_base(col_norm, min_len=min_len, max_len=max_len)
            code = _resolve_collision(base, used_codes, col_norm, min_len=min_len, max_len=max_len)

            global_code_for_col[col_norm] = code
            used_codes.add(code)
            # code_to_cols[code].add(col_norm)

            table_map[col] = code

        out[table] = table_map

    return out


def rename_df_using_mapping(df, mapping: dict[str, str]):
    """
    mapping is {old_name: new_name} for that df/table.
    """
    return df.rename(columns=mapping)

# -----------------------------

def build_equivalency_dimension(
    tables_cols: dict[str, list[str]],
    mapping_by_table: dict[str, dict[str, str]],
    keep_as_is_by_table: dict[str, set[str]] | None = None,
) -> pd.DataFrame:
    keep_as_is_by_table = keep_as_is_by_table or {}

    rows = []
    for table, cols in tables_cols.items():
        keep_set = {c.lower() for c in keep_as_is_by_table.get(table, set())}
        m = mapping_by_table[table]

        for col in cols:
            col_norm = col.strip().lower()
            short_code = m[col]
            kept = col_norm in keep_set

            rows.append(
                {
                    "table_name": table,
                    "original_column": col,
                    "original_column_norm": col_norm,
                    "short_code": short_code,
                    "kept_as_is": kept,
                }
            )

    dim = pd.DataFrame(rows)

    # Helpful uniqueness checks
    # 1) Within a table, short_code should be unique
    dup_within_table = (
        dim.groupby(["table_name", "short_code"])
           .size()
           .reset_index(name="n")
           .query("n > 1")
    )
    if not dup_within_table.empty:
        raise ValueError(
            "Collision detected within table on short_code. "
            "Inspect dup_within_table:\n"
            f"{dup_within_table}"
        )

    # 2) If the *same* original_column_norm appears across tables,
    #    it should map to the same short_code (by design).
    check_same_name = (
        dim.groupby("original_column_norm")["short_code"]
           .nunique()
           .reset_index(name="n_codes")
           .query("n_codes > 1")
    )
    if not check_same_name.empty:
        raise ValueError(
            "Same original column name maps to multiple codes across tables. "
            "Inspect check_same_name:\n"
            f"{check_same_name}"
        )

    # Sort for readability
    dim = dim.sort_values(["table_name", "short_code", "original_column_norm"]).reset_index(drop=True)
    return dim


def save_equivalency_dimension(dim_df: pd.DataFrame, path_csv: str, path_parquet: str | None = None):
    dim_df.to_csv(path_csv, index=False)
    if path_parquet:
        dim_df.to_parquet(path_parquet, index=False)

# -----------------------------
def save_preprocess_csv(df, filename: str, out_dir: Path):
    """
    Save dataframe to CSV with standardized options.
    """
    path = out_dir / filename
    df.to_csv(path, index=False)
    print(f"[SAVED] {path}")

def assert_no_duplicate_columns(df, name):
    if df.columns.duplicated().any():
        dups = df.columns[df.columns.duplicated()].tolist()
        raise ValueError(f"[{name}] Duplicate columns detected: {dups}")
    
# Update the Actual column names to a properly syntaxed field so that it conforms with the best practices.
df_schedpay_clean = validate_and_rename(df_schedpay, schedpay_OGcols, schedpay_NWcols)
df_interestrate_hist_clean = validate_and_rename(df_interestrate_hist, interestrat_hist_OGcols, interestrat_hist_NWcols)
df_gaps_payment_clean = validate_and_rename(df_gaps_payment, gaps_payment_OGcols, gaps_payment_NWcols)
df_loantape_clean = validate_and_rename(df_loantape, loantape_OGcols, loantape_NWcols)
df_tmoinfo_clean = validate_and_rename(df_tmoinfo, tmoinfo_OGcols, tmoinfo_NWcols)
df_loan_infoFT_clean = validate_and_rename(df_loan_infoFT, loan_infoFT_OGcols, loan_infoFT_NWcols)
df_flight_recordsFT_clean = validate_and_rename(df_flight_recordsFT, flight_recordsFT_OGcols, flight_recordsFT_NWcols)
df_demographicsFT_clean = validate_and_rename(df_demographicsFT, demographicsFT_OGcols, demographicsFT_NWcols)

tables_cols = {
    "schedpay": schedpay_NWcols,
    "interestrat_hist": interestrat_hist_NWcols,
    "gaps_payment": gaps_payment_NWcols,
    "loan_infoFT": loan_infoFT_NWcols,
    "flight_recordsFT": flight_recordsFT_NWcols,
    "demographicsFT": demographicsFT_NWcols,
    "tmoinfo": tmoinfo_NWcols,
    "loantape": loantape_NWcols,
}

keep_as_is = {
    "schedpay": {"account", "sched_paydate"},
    "interestrat_hist": {"account", "schedyrnbr"},
    "gaps_payment": {"account", "schedyrnbr"},
    "loan_infoFT": {"account"},
    "flight_recordsFT": {"account"},
    "demographicsFT": {"account", "borrowerID"},
    "tmoinfo": {"account"},
    "loantape": {"account"},
}

mapping_by_table = build_global_column_codes(tables_cols, keep_as_is_by_table=keep_as_is)

# Update the Column Names using the short-hand notation for easy analysis
df_schedpay_short = rename_df_using_mapping(df_schedpay_clean, mapping_by_table["schedpay"])
df_interestrate_hist_short = rename_df_using_mapping(df_interestrate_hist_clean, mapping_by_table["interestrat_hist"])
df_gaps_payment_short = rename_df_using_mapping(df_gaps_payment_clean, mapping_by_table["gaps_payment"])
df_loantape_short = rename_df_using_mapping(df_loantape_clean, mapping_by_table["loantape"])
df_tmoinfo_short = rename_df_using_mapping(df_tmoinfo_clean, mapping_by_table["tmoinfo"])
df_loan_infoFT_short = rename_df_using_mapping(df_loan_infoFT_clean, mapping_by_table["loan_infoFT"])
df_flight_recordsFT_short = rename_df_using_mapping(df_flight_recordsFT_clean, mapping_by_table["flight_recordsFT"])
df_demographicsFT_short = rename_df_using_mapping(df_demographicsFT_clean, mapping_by_table["demographicsFT"])

dim_cols_equiv = build_equivalency_dimension(
    tables_cols=tables_cols,
    mapping_by_table=mapping_by_table,
    keep_as_is_by_table=keep_as_is,
)

# Save the actual/long- and short-hand notation of the field names of each each tables
save_equivalency_dimension(
    dim_cols_equiv,
    path_csv=f"{OUT_DIR}/dim_column_equivalency.csv",
    path_parquet=f"{OUT_DIR}/dim_column_equivalency.parquet",
)

# COLUMN/FIELD PROCESSING: Save all new pre-processed data 
save_preprocess_csv(df_schedpay_short,               "schedpay_short.csv",               OUT_DIR)
save_preprocess_csv(df_interestrate_hist_short,      "interestrate_hist_short.csv",      OUT_DIR)
save_preprocess_csv(df_gaps_payment_short,           "gaps_payment_short.csv",           OUT_DIR)
save_preprocess_csv(df_loantape_short,               "loantape_short.csv",               OUT_DIR)
save_preprocess_csv(df_tmoinfo_short,                "tmoinfo_short.csv",                OUT_DIR)
save_preprocess_csv(df_loan_infoFT_short,            "loan_infoFT_short.csv",            OUT_DIR)
save_preprocess_csv(df_flight_recordsFT_short,       "flight_recordsFT_short.csv",       OUT_DIR)
save_preprocess_csv(df_demographicsFT_short,         "demographicsFT_short.csv",         OUT_DIR)

# Check and Confirm if there are duplicated columns
assert_no_duplicate_columns(df_schedpay_short, "df_schedpay_short")
assert_no_duplicate_columns(df_interestrate_hist_short, "df_loantape_short")
assert_no_duplicate_columns(df_gaps_payment_short, "df_schedpay_short")
assert_no_duplicate_columns(df_loantape_short, "df_loantape_short")
assert_no_duplicate_columns(df_tmoinfo_short, "df_schedpay_short")
assert_no_duplicate_columns(df_loan_infoFT_short, "df_loantape_short")
assert_no_duplicate_columns(df_flight_recordsFT_short, "df_schedpay_short")
assert_no_duplicate_columns(df_demographicsFT_short, "df_loantape_short")