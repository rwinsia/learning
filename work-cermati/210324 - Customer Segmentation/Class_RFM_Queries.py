import pandas as pd
from google.cloud import bigquery

class RFM_Queries (object):
    def get_raw_data_user_segmentation_CLI(self):
        client = bigquery.Client(project="athena-179008")
        sql = """
            with data_score as (
                SELECT DISTINCT
                    userId
                    , FIRST_VALUE(latest_model_score_norescore) over(PARTITION BY userId ORDER BY application_createdat ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_score
                    , LAST_VALUE(latest_model_score_norescore) over(PARTITION BY userId ORDER BY application_createdat ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_score
                FROM athena-179008.vayu_data_mart.dm_application_risk_analytics
                WHERE latest_model_score_norescore IS NOT NULL
            )
            , data_appsflyer as (
                SELECT DISTINCT
                    a.applicantIdNumber
                    , COALESCE(
                        LAST_VALUE(a.appsflyerId IGNORE NULLS) 
                            over(PARTITION BY a.applicantIdNumber ORDER BY a.createdAt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                        , LAST_VALUE(d.appsflyerId IGNORE NULLS) 
                            over(PARTITION BY a.applicantIdNumber ORDER BY coalesce(d.createdAt,a.createdAt) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                    ) as last_appsflyerId
                FROM data-platform-indodana.vayu.indodana_athena_applications a 
                LEFT JOIN data-platform-indodana.vayu.indodana_athena_user_devices ud ON ud.userId=a.userId
                LEFT JOIN data-platform-indodana.vayu.indodana_athena_devices d ON d.deviceId=ud.deviceId AND d.appsflyerId is not null
                WHERE a.applicantIdNumber is not null
            )
            -- DATA CLI USED
            -- Notes : For Contracts calculation, exclude Restructure & SUBSCRIPTION_FEE. For payments calculation include Restructure & SUBSCRIPTION_FEE.
            , raw_data_cli_used as (
                select
                    COALESCE(applicantIdNumber,userId) as applicantIdNumber
                    , CASE WHEN applicantIdNumber IS NULL THEN userId ELSE last_userId END as last_userId
                    , count(distinct userId) as users
                    -- data paylater account
                    , COUNT(distinct partner) as partner_count
                    , LOGICAL_OR(from_whitelabel) as has_whitelabel_app
                    , LOGICAL_OR(NOT from_whitelabel) as has_indodana_app
                    , STRING_AGG(DISTINCT CONCAT(partner,' ',FORMAT("%'d",CAST(cli_limit AS INT64))),'; ') as cli_limit
                    , CAST(MAX(cli_limit) AS INT64) as max_limit
                    , CAST(MIN(cli_limit) AS INT64) as min_limit
                    
                    -- data merchant
                    , COUNT(DISTINCT merchant_name) as merchant_count
                    , STRING_AGG(DISTINCT merchant_name,';') as merchant_list
                    , APPROX_TOP_COUNT(merchant_name,1)[OFFSET(0)].value as top_merchant
                    , APPROX_TOP_COUNT(merchant_name,1)[OFFSET(0)].count as top_merchant_trx
                    , APPROX_TOP_COUNT(merchant_category,1)[OFFSET(0)].value as top_merchant_category
                    , APPROX_TOP_COUNT(merchant_category,1)[OFFSET(0)].count as top_merchant_category_trx
                    , APPROX_TOP_COUNT(item_category,1)[OFFSET(0)].value as top_item_category
                    , APPROX_TOP_COUNT(item_category,1)[OFFSET(0)].count as top_item_category_trx
                    
                    -- data contracts & payments
                    , sum(case when contract_type not in ('RESTRUCTURE_LOAN_CONTRACT','SUBSCRIPTION_FEE') then 1 else 0 end) as contracts
                    , sum(case when contract_type not in ('RESTRUCTURE_LOAN_CONTRACT','SUBSCRIPTION_FEE') and coupon_used then 1 else 0 end) as coupon_used
                    , sum(coalesce(numberOfPayment,0)) as payments
                    , cast(sum(case when contract_type not in ('RESTRUCTURE_LOAN_CONTRACT','SUBSCRIPTION_FEE') then purchase_amount else 0 end) as float64) as loan_amount
                    , cast(sum(case when contract_type not in ('RESTRUCTURE_LOAN_CONTRACT','SUBSCRIPTION_FEE') then realised_purchase_amount else 0 end) as float64) as realised_loan_amount
                    , cast(sum(coalesce(totalAmountPayment,0)) as float64) as paid_amount
                    -- , cast(sum(coalesce(totalPrincipalPayment,0)+coalesce(totalInterestPayment,0)) as float64) as paid_principal_interest_amount
                    -- , cast(sum(coalesce(totalPrincipalPayment,0)) as float64) as paid_principal_amount
                    , cast(sum(coalesce(totalAmountPayment,0)) - sum(case when contract_type not in ('RESTRUCTURE_LOAN_CONTRACT','SUBSCRIPTION_FEE') then realised_purchase_amount else 0 end) as float64) as revenue
                    , sum(case when last_due_date>current_datetime and contractStatus ='ACTIVE' then 1 else 0 end) as contracts_not_due_active
                    , sum(case when last_due_date>current_datetime and contractStatus ='FINISHED' then 1 else 0 end) as contracts_not_due_finished
                    , sum(case when last_due_date<=current_datetime and contractStatus ='ACTIVE' then 1 else 0 end) as contracts_due_active
                    , sum(case when last_due_date<=current_datetime and contractStatus ='FINISHED' then 1 else 0 end) as contracts_due_finished
                    , max(maxDayPastDue) as max_dpd
                    , sum(count_late_payment) as count_late_payment
                    , CASE 
                        WHEN sum(count_late_payment)>1 OR max(maxDayPastDue)>0 THEN 'Ever Late' 
                        WHEN sum(case when contractStatus ='ACTIVE' AND next_due_date>current_timestamp THEN 1 ELSE 0 END)>0 THEN 'Active Contracts Not Due Yet'
                        ELSE 'Never Late' 
                    END as ever_late
                    , CASE WHEN max(maxDayPastDue)>=7 THEN 'YES' ELSE 'NO' END as ever_late_7d
                    , CASE WHEN max(maxDayPastDue)>=15 THEN 'YES' ELSE 'NO' END as ever_late_15d
                    , CASE WHEN max(maxDayPastDue)>=30 THEN 'YES' ELSE 'NO' END as ever_late_30d
                    , AVG(approvedLoanTenureInMonth) as avg_tenure
                    -- data date
                    , case when sum(case when contractStatus = 'ACTIVE' then 1 else 0 end)>0 then true else false end as has_active_contracts
                    , date(min(approvedDate),'Asia/Jakarta') as first_approved_date
                    , date(max(case when contract_type not in ('RESTRUCTURE_LOAN_CONTRACT','SUBSCRIPTION_FEE') then approvedDate end),'Asia/Jakarta') as last_approved_date
                    , date(min(firstPayment),'Asia/Jakarta') as first_payment_date
                    -- , date(min(lastPayment),'Asia/Jakarta') as last_payment_date
                    
                    , date_diff(current_date,date(min(approvedDate),'Asia/Jakarta'),day) as day_since_first_approved
                    , date_diff(current_date,date(max(case when contract_type not in ('RESTRUCTURE_LOAN_CONTRACT','SUBSCRIPTION_FEE') then approvedDate end),'Asia/Jakarta'),day) as day_since_last_approved
                    -- , date_diff(current_date,date(max(lastPayment),'Asia/Jakarta'),day) as day_since_last_paid
                    -- , FORMAT("%T", ARRAY_AGG(day_beetween_trx IGNORE NULLS ORDER BY approvedDate)) as array_day_beetween_trx
                    -- , APPROX_QUANTILES(day_beetween_trx,100 IGNORE NULLS)[OFFSET(50)] AS Median_day_beetween_trx
                    -- , APPROX_TOP_COUNT(day_beetween_trx,1)[OFFSET(0)].value as modus_day_beetween_trx
                    -- , AVG(day_beetween_trx) as mean_day_between_trx
                from athena-179008.vayu_data_mart.indodanamarketing_users_transaction
                where data_type<>'CASH_LOAN'
                group by 1,2
            )

            , data_cli_used as (
                select 
                    a.applicantIdNumber
                    , ds.first_score
                    , ds.last_score
                    -- User Demography
                    , u.firstAppChannel
                    , u.firstPartner
                    , u.firstproductType
                    , u.age_grouping
                    , u.applicantlasteducationlevel_grouping
                    , u.type_of_jobs_grouping
                    , u.income_grouping
                    , u.spending_grouping
                    , u.income_minus_spending_grouping
                    , CASE 
                        WHEN has_indodana_app AND has_whitelabel_app THEN '1. Has Both Indodana & Whitelabel App'
                        WHEN has_indodana_app THEN '2. Has Only Indodana App'
                        WHEN has_whitelabel_app THEN '3. Has Only Whitelabel App'
                    END as partner_category
                    , a.* EXCEPT(applicantIdNumber,last_userId,has_indodana_app,has_whitelabel_app,count_late_payment)
                    
                    -- classification
                    , case 
                        when a.day_since_last_approved<=9 THEN 3
                        when a.day_since_last_approved<=31 THEN 2
                        else 1
                    END as recency
                    , case 
                        when a.contracts<=3 THEN 1
                        when a.contracts<=10 THEN 2
                        else 3
                    END as frequency
                    , case 
                        when a.revenue <= 200000 THEN 1
                        when a.revenue<=700000 THEN 2
                        else 3
                    END as monetization
                    , dap.last_appsflyerId
                from raw_data_cli_used a 
                LEFT JOIN athena-179008.vayu_data_mart.indodanamarketing_user_segmentation u ON u.userId=a.last_userId
                LEFT JOIN data_score ds ON ds.userid=a.last_userId
                -- Data Appsflyer
                LEFT JOIN data_appsflyer dap ON dap.applicantIdNumber=a.applicantIdNumber
            )
            -- DATA CLI NOT USED
            , raw_data_cli_not_used as (
                SELECT 
                    a.applicantIdNumber
                    , a.userId
                    , a.partner
                    , a.approvedDate
                    , a.cli_limit
                    , CASE WHEN a.partner='INDODANA' THEN False ELSE TRUE END as from_whitelabel
                    , LAST_VALUE(a.userId) over(PARTITION BY a.applicantIdNumber ORDER BY a.approvedDate ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_userId
                from athena-179008.vayu_data_mart.indodanamarketing_cli_application a 
                left join (
                    SELECT borrower_id
                    FROM athena-179008.vayu_data_mart.view_cli_contracts
                    WHERE active_trx+finished_trx>0
                    GROUP BY 1
                ) c ON c.borrower_id=a.userId
                where a.status='Approved'
                AND c.borrower_id is null
                AND a.cli_status='ACTIVE'
            )
            , aggregate_data_cli_not_used as (
                SELECT 
                    applicantIdNumber
                    , last_userId
                    , COUNT(distinct userId) as users
                    , COUNT(DISTINCT partner) as partner_count
                    , LOGICAL_OR(from_whitelabel) as has_whitelabel_app
                    , LOGICAL_OR(NOT from_whitelabel) as has_indodana_app
                    , STRING_AGG(DISTINCT CONCAT(partner,' ',FORMAT("%'d",CAST(cli_limit AS INT64))),'; ') as cli_limit
                    , CAST(MAX(cli_limit) AS INT64) as max_limit
                    , CAST(MIN(cli_limit) AS INT64) as min_limit
                    
                    , date(Min(approvedDate),"Asia/Jakarta") as first_approved_date
                    , date(Max(approvedDate),"Asia/Jakarta") as last_approved_date
                    , date_diff(current_date,date(min(approvedDate),'Asia/Jakarta'),day) as day_since_first_approved
                FROM raw_data_cli_not_used
                GROUP BY 1,2
            )
            , data_cli_not_used as (
                select 
                    a.applicantIdNumber
                    , ds.first_score
                    , ds.last_score
                    -- User Demography
                    , u.firstAppChannel
                    , u.firstPartner
                    , u.firstproductType
                    , u.age_grouping
                    , u.applicantlasteducationlevel_grouping
                    , u.type_of_jobs_grouping
                    , u.income_grouping
                    , u.spending_grouping
                    , u.income_minus_spending_grouping
                    , CASE 
                        WHEN has_indodana_app AND has_whitelabel_app THEN '1. Has Both Indodana & Whitelabel App'
                        WHEN has_indodana_app THEN '2. Has Only Indodana App'
                        WHEN has_whitelabel_app THEN '3. Has Only Whitelabel App'
                    END as partner_category
                    , a.* EXCEPT(applicantIdNumber,last_userId,has_whitelabel_app,has_indodana_app,first_approved_date,last_approved_date,day_since_first_approved)
                    , null as merchant_count
                    , '' as merchant_list
                    , '' as top_merchant
                    , null as top_merchant_trx
                    , '' as top_merchant_category
                    , null as top_merchant_category_trx
                    , '' as top_item_category
                    , null as top_item_category_trx
                    
                    , 0 as contracts
                    , 0 as coupon_used
                    , 0 as payments
                    , 0 as loan_amount
                    , 0 as realised_loan_amount
                    , 0 as paid_amount
                    , 0 as revenue
                    , 0 as contracts_not_due_active
                    , 0 as contracts_not_due_finished
                    , 0 as contracts_due_active
                    , 0 as contracts_due_finished
                    , null as max_dpd
                    , 'Never Transact' as ever_late
                    , 'Never Transact' as ever_late_7d
                    , 'Never Transact' as ever_late_15d
                    , 'Never Transact' as ever_late_30d
                    , NULL as avg_tenure
                    , FALSE as has_active_contracts
                    , a.first_approved_date
                    , cast(null as date) as last_approved_date
                    , cast(null as date) as first_payment_date
                    , a.day_since_first_approved
                    ,-99 as day_since_last_approved
                    -- classification
                    , 1 as recency
                    , 1 as frequency
                    , 1 as monetization
                    , dap.last_appsflyerId
                from aggregate_data_cli_not_used a 
                INNER JOIN athena-179008.vayu_data_mart.indodanamarketing_user_segmentation u ON u.userId=a.last_userId
                -- Data Score
                LEFT JOIN data_score ds ON ds.userid=a.last_userId
                -- Data Appsflyer
                LEFT JOIN data_appsflyer dap ON dap.applicantIdNumber=a.applicantIdNumber
            )

            SELECT 
                *
                , 'CLI Used' as category
                , case when last_appsflyerId is null then 'NO' ELSE 'YES' END as has_appsflyerID 
                , CASE 
                    WHEN  
                        (frequency IN (2,3) AND monetization IN (2,3))
                        OR 
                        (recency IN (2,3) AND monetization IN (2,3))
                        OR
                        (recency IN (3) AND frequency IN (2,3))
                    THEN 'Seeding'
                    ELSE 'Non-Seeding'
                END as Seeding_category
            FROM data_cli_used
            UNION ALL
            SELECT 
                *
                , 'CLI Not Used' as category
                , case when last_appsflyerId is null then 'NO' ELSE 'YES' END as has_appsflyerID 
                , 'Non-Seeding' as Seeding_category
            FROM data_cli_not_used;
        """

        query_job = client.query(sql)
        df = query_job.to_dataframe()
        return df