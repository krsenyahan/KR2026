WITH tables_with_account AS (
	SELECT 
		TABLE_NAME
	FROM information_schema.COLUMNS
	WHERE TABLE_SCHEMA = DATABASE()
	  # AND COLUMN_NAME = 'Account'
      AND COLUMN_NAME like '%liquid%'
), loan_tape AS (
	SELECT *
    FROM analytics_db.loan_tape
),
id_mapping AS (
	SELECT distinct Account, `Salesforce_Loan Applicant ID`, `Reference (XERO)`, `Borrower Name` 
	FROM analytics_db.applicant_id_mapping
), 
flight_training AS (
	SELECT Account, 
		Borrower as BorrowerID, 
		Name, 
		`Date of Loan Application` as Loan_Application_Date,
		Citizenship,
		Country,
		Birthday, 
		School,
		NPS, 
		`Flight School State` as Flight_School_State,
		`Flight School Tier` as Flight_School_Tier,
		Program,
		Particulars,
		`Loan Amount` as Loan_Amount,
		`Total Hours` as Total_Hours,
		`0 hours` as `0_hours`,
		`40/50 hours` as `40_50_hours`,
		`70/120 hours` as `70_120_hours`,
		`170/180 hours` as `170_180_hours`,
		`200/220 hours` as `200_220_hours`,
		`230 hours` as `230_hours`,
		`No. of Months`,
		`Funding Status`,
		`Date of Last Funding`,
		`Medical Class`,
		PPL,
		IFR, 
		`COMM SE`,
		`COMM Multi`,
		CFI, 
		CFII,
		MEI,
		ATP,
		`Certificates based on FAA`,
		`Other Certificates based on FAA`,
		Employment
	FROM analytics_db.raw_flight_training
),
payment_historical AS (
	SELECT Account, YearMonth, LoanAccount, DateDue, 
       DateRec, LoanBalance, TotalAmount, ToPrincipal_RegPmt, 
       ToInterest_RegPmt, ToPRincipal_Paydown, ToPrincipal_Payoff
       Notes, DaysPastDue
    FROM analytics_db.payment_with_dpd
),
net_disbursement AS (
	# revisit later if disbursement is needed
	SELECT *
    FROM analytics_db.net_disbursements
),
scheduled_payment AS (
	SELECT *
		 , row_number() over (partition by base.Account, base.Payment_Schedule_ID order by base.Account, base.Date) as rnPS
	FROM (
		SELECT distinct a.Account, a.Date, `Payment Schedule ID` AS Payment_Schedule_ID
			 , `Expected Payment Amount` as Expected_Payment_Amount
			 ,`Interest Payment` AS Interest_Payment
			 , `Principal Payment` AS Principal_Payment
			 , round(`Interest Payment` + `Principal Payment`, 2) as Total
			 , substring(a.Date, 1, 7) as YearMonth
		FROM analytics_db.scheduled_payment_tape a
		INNER JOIN loan_tape b on a.Account = b.Account
	) base 
	WHERE base.Account in (100000901, 100000201, 100000301, 100000401)
	  and Expected_Payment_Amount = Total
),
scheduled_payment2 AS (
	SELECT *
         , FLOOR ((rn - 1) / 12) as ScheduleYearNbr
	     , round(Interest_Payment * 100 / Expected_Payment_Amount, 2) as InterestRate
    FROM (
		SELECT *
			 , row_number() over (partition by Account order by Account, Date) as rn
		FROM scheduled_payment
		WHERE rnPS = 1    
    ) base 
),
payment_history AS (
	SELECT Account
		 , DateRec
         , YearMonth
		 , avg(ExpectedPaymentAmount) as ExpectedPaymentAmount
         , sum(ActualPaymentAmount) as ActualPaymentAmount
    FROM (
		SELECT distinct Account,
			   ExpectedPaymentAmount,
			   round(ToInterest_RegPmt + ToPrincipal_RegPmt, 2) AS ActualPaymentAmount,
			   cast(DateRec as Date) as DateRec,
			   substring(DateRec, 1, 7) as YearMonth
		FROM analytics_db.payment_dpd_raw
    ) base
    GROUP BY Account, DateRec, YearMonth
)
, payment_gap_raw AS (
	SELECT c.Account
		 , c.Date as ScheduledDate
		 , c.YearMonth as ScheduledYM
         , c.ScheduleYearNbr
		 , b.Date as NextScheduledDate
		 , c.Payment_Schedule_ID
		 , c.Expected_Payment_Amount
		 , a.ActualPaymentAmount
		 , case when a.DateRec is Null then c.Date else a.DateRec end as PaymentDate
		 , a.YearMonth as PaymentYM
		 , datediff(DateRec, c.Date) as DPD
		 , datediff(case when a.DateRec is Null 
                           or a.ActualPaymentAmount is Null 
                           or a.ActualPaymentAmount < 0
                           or c.Expected_Payment_Amount > a.ActualPaymentAmount then b.Date else a.DateRec end, c.Date) as DPD2
		 , case when a.ActualPaymentAmount is not Null and c.Expected_Payment_Amount > a.ActualPaymentAmount then "missed payment" 
                when a.ActualPaymentAmount is Null then "missed payment" else "paid" end as PaidInd
	FROM scheduled_payment2 c 
	LEFT JOIN scheduled_payment2 b on c.Account = b.Account and c.rn = b.rn-1
	LEFT JOIN payment_history a on a.Account = c.Account and a.YearMonth = c.YearMonth 
	WHERE 1 = 1
	  # and c.Account = 100000901
	  and c.YearMonth <= "2025-12"
	ORDER BY c.Account, c.YearMonth
),
payment_gap_raw2 AS ( 
	SELECT
		q.*,
		-- Group segments between zeroes
		SUM(CASE WHEN DPD2 = 0 THEN 1 ELSE 0 END) OVER (
			PARTITION BY Account
			ORDER BY Payment_Schedule_ID
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS zero_grp
	FROM payment_gap_raw q
), 
payment_gap_raw2_result AS (
	SELECT
		a.*,
		CASE
			WHEN DPD2 > 0 THEN
				SUM(DPD2) OVER (
					PARTITION BY Account, zero_grp, PaidInd
					ORDER BY Payment_Schedule_ID
					ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
				)
			ELSE 0
		END AS DPD2_running_sum_all,
		CASE
			WHEN DPD2 > 0 THEN
				SUM(DPD2) OVER (
					PARTITION BY Account, zero_grp, PaidInd, ScheduleYearNbr # , substr(ScheduledYM, 1, 4)
					ORDER BY Payment_Schedule_ID
					ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
				)
			ELSE 0
		END AS DPD2_running_sum_yrly
	FROM payment_gap_raw2 as a
	ORDER BY Account, Payment_Schedule_ID
)
, gaps_in_payment AS (
	SELECT *
		 , case when DPD_yrly > 60 then 1 else 0 end as DPD60Ind_yrly
		 , case when DPD_yrly > 90 then 1 else 0 end as DPD90Ind_yrly
		 , case when DPD_yrly > 120 then 1 else 0 end as DPD120Ind_yrly
		 , case when DPD_alltime > 60 then 1 else 0 end as DPD60Ind_alltime
		 , case when DPD_alltime > 90 then 1 else 0 end as DPD90Ind_alltime
		 , case when DPD_alltime > 120 then 1 else 0 end as DPD120Ind_alltime
	FROM (
		SELECT distinct Account
		   # , substr(ScheduledYM, 1, 4) as YearNbr
             , ScheduleYearNbr
             , PaidInd
		   # , max(DPD2_running_sum_yrly) over (partition by Account, substr(ScheduledYM, 1, 4)) as DPD_yrly
			 , min(ScheduledYM) over (partition by Account, ScheduleYearNbr) as minYM_yrly
             , max(ScheduledYM) over (partition by Account, ScheduleYearNbr) as maxYM_yrly
             , max(DPD2_running_sum_yrly) over (partition by Account, ScheduleYearNbr) as DPD_yrly
             , max(DPD2_running_sum_all) over (partition by Account) as DPD_alltime
		FROM payment_gap_raw2_result
		) base
	WHERE PaidInd = "missed payment"
)
, interest_rate_summary AS (
	SELECT Account
		 , ScheduleYearNbr
		 , min(YearMonth) as minYearMo
		 , max(YearMonth) as maxYearMo
		 , min(InterestRate) as minInterestRate
		 , round(avg(InterestRate), 2) as avgInterestRate
		 , max(InterestRate) as maxInterestRate
	FROM scheduled_payment2 
	GROUP BY Account, ScheduleYearNbr
	# ORDER BY Account, ScheduleYearNbr
)
# SELECT * FROM tables_with_account 
# SELECT * FROM gaps_in_payment ORDER BY Account, ScheduleYearNbr # (historical)
# SELECT * FROM flight_training
# SELECT * FROM loan_tape
# SELECT * FROM interest_rate_summary # (historical)
# SELECT * FROM borrower_demographics_FT
# SELECT * FROM borrower_loan_information_FT
# SELECT * FROM borrower_flight_records_FT
, borrower_demographics_FT AS (
	SELECT Account
		 , BorrowerID
		 , Name
		 , Citizenship
         , Country
		 , Birthday
         , ceiling(datediff("2025-12-31", Birthday)/ 365.25) as Age
         , Employment
	FROM flight_training 
	ORDER BY Account
)
, borrower_loan_information_FT AS (
	SELECT Account
         , Loan_Amount
         , `Funding Status` AS Funding_Status
         , `Date of Last Funding` AS Date_Last_Funding
	FROM flight_training 
	ORDER BY Account
)
, borrower_flight_records_FT AS (
	SELECT Account
		 , School
         , NPS
         , Flight_School_State
         , Flight_School_Tier
         , TRIM(REGEXP_REPLACE(Flight_School_Tier, '\\s*\\(.*$', '')) AS Flight_School_Tier_Clean
         , Program
         , TRIM(REGEXP_REPLACE(Program, '\\s*\\(.*$', '')) AS Program_Clean
         , Particulars
         , Total_Hours
         , 0_hours
         , 40_50_hours
         , 70_120_hours
         , 170_180_hours
         , 200_220_hours
         , 230_hours
         , `No. of Months` AS Num_Months_Training_Completed
         , `Medical Class` AS Medical_Class
         , PPL
         , IFR
         , `COMM SE` AS COMM_SE
         , `COMM Multi` AS COMM_Multi
         , CFI
         , CFII
         , MEI
         , ATP
         , `Other Certificates based on FAA` AS OTHER_CERT_FAA
	FROM flight_training 
	ORDER BY Account
), 
latest_tmo_loan_information AS (
	SELECT *
	FROM (
		SELECT * 
			 , row_number() over (partition by Account Order by date_added desc) as rn
		FROM analytics_db.tmo_all_loans_consolidated 
		# WHERE Account in (100000901, 100000201, 100000301, 100000401)
	) base
	WHERE rn = 1
)
# To Extract and Save
# SELECT * FROM loan_tape
# SELECT * FROM gaps_in_payment
# SELECT * FROM interest_rate_summary
# SELECT * FROM borrower_demographics_FT 
# SELECT * FROM borrower_flight_records_FT
# SELECT * FROM borrower_flight_records_FT
# SELECT * FROM latest_tmo_loan_information

# Do not delete - potential data may be taken here
# SELECT * FROM tables_with_account 
# SELECT * from analytics_db.tmo_all_loans
# SELECT * FROM analytics_db.dpd_results WHERE Account in (100000901, 100000201, 100000301, 100000401) ORDER BY Account, EOM # (match it with dpd in gaps in payment)
# SELECT * FROM analytics_db.historical_monthly_loan_status # (historical)
# SELECT * FROM analytics_db.fact_school_funding_xero ORDER BY Account, `Funding Date` # (historical)

# Done Exploring
# SELECT * FROM analytics_db.loan_ownership_tape
# SELECT * FROM analytics_db.loan_tape
# SELECT * FROM analytics_db.applicant_id_mapping
# SELECT * FROM analytics_db.raw_flight_training
# SELECT * FROM analytics_db.payment_with_dpd
# SELECT * FROM analytics_db.net_disbursements # (revisit later if disbursement is needed)
# SELECT * FROM analytics_db.scheduled_payment_tape
# SELECT * FROM analytics_db.loan_tape_history # (this is empty)
# SELECT * FROM analytics_db.loan_tape_data_history # (very huge but it's like loan tape but it is distinct with generated_date)
# SELECT * FROM analytics_db.salesforce_tmo WHERE `Deal Stage` = "Funded" # (seems incomplete)
# SELECT * FROM analytics_db.tmo_all_loans # (very huge but it's like loan tape but it is distinct with generated_date)
# SELECT * FROM analytics_db.tmo_all_loans_consolidated 
# SELECT * FROM analytics_db.xero_loans_payment
# SELECT * FROM analytics_db.xero_loans_disbursement
# SELECT * FROM analytics_db.xero_current_loan_ownership



