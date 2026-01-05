SELECT * FROM analytics_db.payment_tape;
SELECT * FROM analytics_db.loan_tape;
SELECT * FROM analytics_db.loan_tape_history;
SELECT * FROM analytcis_db.net_disbursement;
SELECT * FROM analytics_db.schedule_payment_tape;

SELECT * FROM analytics_db.tmo_all_loans;


# SELECT * FROM analytics_db.xero_loans_disbursement;
# SELECT * FROM analytics_db.xero_loans_payment;
# SELECT * FROM analytics_db.xero_invoice_description;
# SELECT * FROM analytics_db.tmo_payment_schedule;

/*
SELECT * 
	 , round(`Interest Payment` * 100 / `Expected Payment Amount`, 2) as InterestRate
FROM analytics_db.scheduled_payment_tape
*/

# FLIGHT AND SCHOOL DETAILS
# SELECT * FROM analytics_db.raw_flight_training;
# SELECT * FROM analytics_db.raw_salesforce_education_program;
# SELECT * FROM analytics_db.raw_salesforce_loan_applicant;
# SELECT * FROM analytics_db.raw_salesforce_loan_application;
# SELECT * FROM analytics_db.raw_salesforce_loan_product;
# SELECT * FROM analytics_db.raw_school_funding_xero;
# SELECT * FROM analytics_db.salesforce_loan_credit_report; #FICO?
# SELECT * FROM analytics_db.salesforce_tmo;   
# SELECT * FROM analytics_db.payment_with_dpd;
# SELECT * FROM analytics_db.payment_tape
# SELECT * FROM analytics_db.net_disbursements;
# SELECT * FROM analytics_db.loan_tape;
# SELECT * FROM analytics_db.loan_tape_data_history where Account = '100000101';
# SELECT * FROM analytics_db.co_borrowers_reference
SELECT * FROM analytics_db.dlqncy_loan_hist



