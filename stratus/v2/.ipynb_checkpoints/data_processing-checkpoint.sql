/*
# SELECT * FROM analytics_db.xero_loans_disbursement;
# SELECT * FROM analytics_db.xero_loans_payment;
# SELECT * FROM analytics_db.xero_invoice_description;
# SELECT * FROM analytics_db.tmo_payment_schedule;
*/

/*
SELECT * 
	 , round(`Interest Payment` * 100 / `Expected Payment Amount`, 2) as InterestRate
FROM analytics_db.scheduled_payment_tape
*/

/*
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
# SELECT * FROM analytics_db.dlqncy_loan_hist
*/


CREATE OR REPLACE VIEW analytics_db.vw_scheduled_payment_summary AS
SELECT 
    Account,
    SUBSTR(Date, 1, 4) AS year_nbr,
    COUNT(*) AS expected_row_cnt,
    MIN(Date) AS expected_frst_pymnt_mnth,
    MAX(Date) AS expected_last_pymnt_mnth,
    SUM(`Expected Payment Amount`) AS total_loan_amount,
    SUM(`Interest Payment`) AS total_interest_amount,
    SUM(`Principal Payment`) AS total_principal_amount,
    MIN(interest_rate_mnthly) AS min_interest_rate_mnthly,
    MAX(interest_rate_mnthly) AS max_interest_rate_mnthly
FROM (
        SELECT 
            sched.Account,
            sched.Date,
            sched.`Expected Payment Amount`,
            sched.`Interest Payment`,
            sched.`Principal Payment`,
            ROUND(`Interest Payment` * 100 / `Expected Payment Amount`, 3) AS interest_rate_mnthly
        FROM analytics_db.scheduled_payment_tape sched
        JOIN analytics_db.loan_tape loan 
            ON sched.Account = loan.Account
    ) AS a
GROUP BY 
    Account, 
    SUBSTR(Date, 1, 4);


/*
# Nov 21, 2025 Work
SELECT Account,
	SUBSTR(Date, 1, 4) AS year_nbr,
	COUNT(*) AS expected_row_cnt,
    MIN(Date) AS expected_frst_pymnt_mnth,
    MAX(Date) AS expected_last_pymnt_mnth,
    round(SUM(`Expected Payment Amount`), 2) AS total_loan_amount,
    round(SUM(`Interest Payment`), 2) AS total_interest_amount,
    round(SUM(`Principal Payment`), 2) AS total_principal_amount,
    MIN(interest_rate_mnthly) AS min_interest_rate_mnthly,
    MAX(interest_rate_mnthly) AS max_interest_rate_mnthly
FROM (
	SELECT distinct
		sched.Account,
		sched.Date,
		sched.`Expected Payment Amount`,
		sched.`Interest Payment`,
		sched.`Principal Payment`,
		ROUND(`Interest Payment` * 100 / `Expected Payment Amount`, 3) AS interest_rate_mnthly
	FROM analytics_db.scheduled_payment_tape sched
	JOIN analytics_db.loan_tape loan 
		ON sched.Account = loan.Account
	WHERE sched.Account in (100000101, 100001501, 100002301, 100002801, 100004501)
	) a
GROUP BY Account, SUBSTR(Date, 1, 4)
ORDER BY Account, year_nbr;
*/


# 100000101 = 67328.00
# 100001501 = 82864.00
/*
SELECT * 
FROM analytics_db.scheduled_payment_tape 
WHERE Account in (100000101)
*/
/*
SELECT 
	sched.Account,
	sched.Date,
	sched.`Expected Payment Amount`,
	sched.`Interest Payment`,
	sched.`Principal Payment`,
	ROUND(`Interest Payment` * 100 / `Expected Payment Amount`, 3) AS interest_rate_mnthly
FROM analytics_db.scheduled_payment_tape sched
JOIN analytics_db.loan_tape loan 
	ON sched.Account = loan.Account
*/

/*
SELECT 
    TABLE_NAME
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND COLUMN_NAME = 'Account';
 */ 

/*
SELECT *
FROM analytics_db.applicant_id_mapping a
JOIN analytics_db.raw_flight_training b on a.Account = b.Account
JOIN analytics_db.loan_tape c on a.Account = c.Account
*/

