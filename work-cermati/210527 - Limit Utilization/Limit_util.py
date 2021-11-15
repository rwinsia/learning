import pandas as pd
from google.cloud import bigquery

class Limit_util (object):
    def get_group_cla(self):
        client = bigquery.Client(project="athena-179008")
        sql = """
            WITH group_upgrade as (
                SELECT * FROM (
                    SELECT 
                        date(clal.created_at,"Asia/Jakarta") as log_date
                        , cla.fee_scheme
                        , clal.updater_role
                        , clal.action
                        , format_timestamp("%Y-%m-%d - ",clal.created_at,"Asia/Jakarta") || clal.updater_role || ' - ' || clal.action as description
                        , count(1) as total
                    FROM data-platform-indodana.vayu.indodana_credit_credit_limit_account_logs clal
                    INNER JOIN data-platform-indodana.vayu.indodana_credit_credit_limit_accounts cla ON cla.id=clal.credit_limit_account_id
                    WHERE clal.action IN  ('UPGRADE_CREDIT_LIMIT','UPGRADE_CREDIT_LIMIT_ACCOUNT')
                    AND date(clal.created_at,"Asia/Jakarta")>='2020-12-01'
                    group by 1,2,3,4,5
                    having count(1)>100
                )
                WHERE total>10000 OR log_date<='2021-04-14'
                ORDER BY 1 DESC
            )
            , group_upgrade_cla as (
                SELECT 
                    g.* EXCEPT(total)
                    , clal.credit_limit_account_id
                    , cli.orderId
                    , cli.partner
                FROM data-platform-indodana.vayu.indodana_credit_credit_limit_account_logs clal 
                INNER JOIN athena-179008.vayu_data_mart.indodanamarketing_cli_application cli ON cli.credit_limit_account_id=clal.credit_limit_account_id
                INNER JOIN group_upgrade g ON g.description=format_timestamp("%Y-%m-%d - ",clal.created_at,"Asia/Jakarta") || clal.updater_role || ' - ' || clal.action
                GROUP BY 1,2,3,4,5,6,7,8
            )

            select * from group_upgrade_cla
        """

        query_job = client.query(sql)
        df = query_job.to_dataframe()
        return df
    def get_historical_data(self, day_before, day_after):
        client = bigquery.Client(project="athena-179008")
        sql = """
            WITH comparison_date_config as (
                SELECT {days_before} as m, 'Before Upgrade' as notes
                UNION ALL
                SELECT {days_after} as m, 'After Upgrade' as notes
                UNION ALL
                SELECT 0 as m, 'Upgrade' as notes
            )
            , group_upgrade as (
                SELECT * FROM (
                    SELECT 
                        date(clal.created_at,"Asia/Jakarta") as log_date
                        , cla.fee_scheme
                        , clal.updater_role
                        , clal.action
                        , format_timestamp("%Y-%m-%d - ",clal.created_at,"Asia/Jakarta") || clal.updater_role || ' - ' || clal.action as description
                        , count(1) as total
                    FROM data-platform-indodana.vayu.indodana_credit_credit_limit_account_logs clal
                    INNER JOIN data-platform-indodana.vayu.indodana_credit_credit_limit_accounts cla ON cla.id=clal.credit_limit_account_id
                    WHERE clal.action IN  ('UPGRADE_CREDIT_LIMIT','UPGRADE_CREDIT_LIMIT_ACCOUNT')
                    AND date(clal.created_at,"Asia/Jakarta")>='2020-12-01'
                    group by 1,2,3,4,5
                    having count(1)>100
                )
                WHERE total>10000 OR log_date<='2021-04-14'
                ORDER BY 1 DESC
            )
            , group_upgrade_cla as (
                SELECT 
                    g.* EXCEPT(total)
                    , clal.credit_limit_account_id
                    , cli.orderId
                    , cli.partner
                FROM data-platform-indodana.vayu.indodana_credit_credit_limit_account_logs clal 
                INNER JOIN athena-179008.vayu_data_mart.indodanamarketing_cli_application cli ON cli.credit_limit_account_id=clal.credit_limit_account_id
                INNER JOIN group_upgrade g ON g.description=format_timestamp("%Y-%m-%d - ",clal.created_at,"Asia/Jakarta") || clal.updater_role || ' - ' || clal.action
                GROUP BY 1,2,3,4,5,6,7,8
            )
            , list_date_compare as (
                SELECT 
                    group_upgrade.description
                    , DATE_ADD(group_upgrade.log_date,INTERVAL comparison_date_config.m DAY) as compare_date
                    , comparison_date_config.notes
                    , comparison_date_config.m
                FROM group_upgrade, comparison_date_config
            )
            , data as (
                SELECT 
                    cla.fee_scheme as partner
                    , cla.dataDate
                    , cla.id as credit_limit_account_id
                    , cli.orderId
                    , cla.limit
                    , cla.limit_balance
                    , cla.status
                    , '2021-04-16 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT' as cla_group
                    , l.notes
                    , l.m
                    , COUNT(c.entity_id) as contracts
                    , min(date(c.approved_date,"Asia/Jakarta")) first_trx
                    , max(date(c.approved_date,"Asia/Jakarta")) last_trx
                FROM `athena-179008.vayu_data_mart.indodana_credit_credit_limit_accounts` cla
                LEFT JOIN athena-179008.vayu_data_mart.indodanamarketing_cli_application cli ON cla.id=cli.credit_limit_account_id
                LEFT JOIN athena-179008.vayu_data_mart.view_trx_contracts c ON c.credit_limit_account_id=cla.id AND c.type not like '%RESTRUCTURE%'
                INNER JOIN list_date_compare l ON l.description='2021-04-16 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT' AND l.compare_date=cla.dataDate
                WHERE cla.dataDate BETWEEN '2020-12-01' AND '2021-07-31'
                AND cla.id IN (SELECT credit_limit_account_id FROM group_upgrade_cla WHERE description='2021-04-16 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                AND cla.fee_scheme IN (SELECT fee_scheme FROM group_upgrade_cla WHERE description='2021-04-16 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                GROUP by 1,2,3,4,5,6,7,8,9,10
                UNION ALL
                SELECT 
                    cla.fee_scheme as partner
                    , cla.dataDate
                    , cla.id as credit_limit_account_id
                    , cli.orderId
                    , cla.limit
                    , cla.limit_balance
                    , cla.status
                    , '2021-04-14 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT' as cla_group
                    , l.notes
                    , l.m
                    , COUNT(c.entity_id) as contracts
                    , min(date(c.approved_date,"Asia/Jakarta")) first_trx
                    , max(date(c.approved_date,"Asia/Jakarta")) last_trx
                FROM `athena-179008.vayu_data_mart.indodana_credit_credit_limit_accounts` cla
                LEFT JOIN athena-179008.vayu_data_mart.indodanamarketing_cli_application cli ON cla.id=cli.credit_limit_account_id
                LEFT JOIN athena-179008.vayu_data_mart.view_trx_contracts c ON c.credit_limit_account_id=cla.id AND c.type not like '%RESTRUCTURE%'
                INNER JOIN list_date_compare l ON l.description='2021-04-14 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT' AND l.compare_date=cla.dataDate
                WHERE cla.dataDate BETWEEN '2020-12-01' AND '2021-07-31'
                AND cla.id IN (SELECT credit_limit_account_id FROM group_upgrade_cla WHERE description='2021-04-14 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                AND cla.fee_scheme IN (SELECT fee_scheme FROM group_upgrade_cla WHERE description='2021-04-14 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                GROUP by 1,2,3,4,5,6,7,8,9,10
                UNION ALL
                SELECT 
                    cla.fee_scheme as partner
                    , cla.dataDate
                    , cla.id as credit_limit_account_id
                    , cli.orderId
                    , cla.limit
                    , cla.limit_balance
                    , cla.status
                    , '2021-02-09 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT' as cla_group
                    , l.notes
                    , l.m
                    , COUNT(c.entity_id) as contracts
                    , min(date(c.approved_date,"Asia/Jakarta")) first_trx
                    , max(date(c.approved_date,"Asia/Jakarta")) last_trx
                FROM `athena-179008.vayu_data_mart.indodana_credit_credit_limit_accounts` cla
                LEFT JOIN athena-179008.vayu_data_mart.indodanamarketing_cli_application cli ON cla.id=cli.credit_limit_account_id
                LEFT JOIN athena-179008.vayu_data_mart.view_trx_contracts c ON c.credit_limit_account_id=cla.id AND c.type not like '%RESTRUCTURE%'
                INNER JOIN list_date_compare l ON l.description='2021-02-09 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT' AND l.compare_date=cla.dataDate
                WHERE cla.dataDate BETWEEN '2020-12-01' AND '2021-07-31'
                AND cla.id IN (SELECT credit_limit_account_id FROM group_upgrade_cla WHERE description='2021-02-09 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                AND cla.fee_scheme IN (SELECT fee_scheme FROM group_upgrade_cla WHERE description='2021-02-09 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                GROUP by 1,2,3,4,5,6,7,8,9,10
            )
            , compare_data as (
                SELECT 
                    d1.cla_group
                    , d1.partner
                    , CAST(ABS(d1.m) AS String) || 'd ' || d1.notes || ' to ' || CAST(d3.m as STRING) || 'd ' || d3.notes as comparison_notes
                    , d1.orderId
                    , d2.dataDate as upgrade_limit_date
                    , d3.status as paylater_status
                    , d1.limit as old_limit
                    , d3.limit as new_limit
                    
                    , d3.limit-d3.limit_balance as limit_usage_new
                    , d1.limit-d1.limit_balance as limit_usage_old
                    , CASE WHEN d1.first_trx is null then 'Never Trx' WHEN d1.first_trx>=d2.dataDate THEN 'Yes' Else 'No' END as first_trx_after_upgrade
                FROM data d1
                LEFT JOIN data d2 ON d2.credit_limit_account_id=d1.credit_limit_account_id AND d2.notes='Upgrade' AND d1.cla_group=d2.cla_group
                LEFT JOIN data d3 ON d3.credit_limit_account_id=d1.credit_limit_account_id AND d3.notes='After Upgrade' AND d1.cla_group=d3.cla_group
                WHERE d1.notes='Before Upgrade'
            )
            SELECT * FROM compare_data WHERE coalesce(comparison_notes,'')<>''
        """.format(days_before=day_before, days_after=day_after)

        query_job = client.query(sql)
        df = query_job.to_dataframe()
        return df
    def get_historical_data_1(self, day_before, day_after):
        client = bigquery.Client(project="athena-179008")
        sql = """
            WITH comparison_date_config as (
                SELECT {days_before} as m, 'Before Upgrade' as notes
                UNION ALL
                SELECT {days_after} as m, 'After Upgrade' as notes
                UNION ALL
                SELECT 0 as m, 'Upgrade' as notes
            )
            , group_upgrade as (
                SELECT * FROM (
                    SELECT 
                        date(clal.created_at,"Asia/Jakarta") as log_date
                        , cla.fee_scheme
                        , clal.updater_role
                        , clal.action
                        , format_timestamp("%Y-%m-%d - ",clal.created_at,"Asia/Jakarta") || clal.updater_role || ' - ' || clal.action as description
                        , count(1) as total
                    FROM data-platform-indodana.vayu.indodana_credit_credit_limit_account_logs clal
                    INNER JOIN data-platform-indodana.vayu.indodana_credit_credit_limit_accounts cla ON cla.id=clal.credit_limit_account_id
                    WHERE clal.action IN  ('UPGRADE_CREDIT_LIMIT','UPGRADE_CREDIT_LIMIT_ACCOUNT')
                    AND date(clal.created_at,"Asia/Jakarta")>='2020-12-01'
                    group by 1,2,3,4,5
                    having count(1)>100
                )
                WHERE total>10000 OR log_date<='2021-04-14'
                ORDER BY 1 DESC
            )
            , group_upgrade_cla as (
                SELECT 
                    g.* EXCEPT(total)
                    , clal.credit_limit_account_id
                    , cli.orderId
                    , cli.partner
                FROM data-platform-indodana.vayu.indodana_credit_credit_limit_account_logs clal 
                INNER JOIN athena-179008.vayu_data_mart.indodanamarketing_cli_application cli ON cli.credit_limit_account_id=clal.credit_limit_account_id
                INNER JOIN group_upgrade g ON g.description=format_timestamp("%Y-%m-%d - ",clal.created_at,"Asia/Jakarta") || clal.updater_role || ' - ' || clal.action
                GROUP BY 1,2,3,4,5,6,7,8
            )
            , list_date_compare as (
                SELECT 
                    group_upgrade.description
                    , DATE_ADD(group_upgrade.log_date,INTERVAL comparison_date_config.m DAY) as compare_date
                    , comparison_date_config.notes
                    , comparison_date_config.m
                FROM group_upgrade, comparison_date_config
            )
            , data as (
                SELECT 
                    cla.fee_scheme as partner
                    , cla.dataDate
                    , cla.id as credit_limit_account_id
                    , cli.orderId
                    , cla.limit
                    , cla.limit_balance
                    , cla.status
                    , '2021-06-21 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT' as cla_group
                    , l.notes
                    , l.m
                    , COUNT(c.entity_id) as contracts
                    , min(date(c.approved_date,"Asia/Jakarta")) first_trx
                    , max(date(c.approved_date,"Asia/Jakarta")) last_trx
                FROM `athena-179008.vayu_data_mart.indodana_credit_credit_limit_accounts` cla
                LEFT JOIN athena-179008.vayu_data_mart.indodanamarketing_cli_application cli ON cla.id=cli.credit_limit_account_id
                LEFT JOIN athena-179008.vayu_data_mart.view_trx_contracts c ON c.credit_limit_account_id=cla.id AND c.type not like '%RESTRUCTURE%'
                INNER JOIN list_date_compare l ON l.description='2021-06-21 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT' AND l.compare_date=cla.dataDate
                WHERE cla.dataDate BETWEEN '2020-12-01' AND '2021-07-31'
                AND cla.id IN (SELECT credit_limit_account_id FROM group_upgrade_cla WHERE description='2021-06-21 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                AND cla.fee_scheme IN (SELECT fee_scheme FROM group_upgrade_cla WHERE description='2021-06-21 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                GROUP by 1,2,3,4,5,6,7,8,9,10
                UNION ALL
                SELECT 
                    cla.fee_scheme as partner
                    , cla.dataDate
                    , cla.id as credit_limit_account_id
                    , cli.orderId
                    , cla.limit
                    , cla.limit_balance
                    , cla.status
                    , '2021-06-23 - developers - UPGRADE_CREDIT_LIMIT_ACCOUNT' as cla_group
                    , l.notes
                    , l.m
                    , COUNT(c.entity_id) as contracts
                    , min(date(c.approved_date,"Asia/Jakarta")) first_trx
                    , max(date(c.approved_date,"Asia/Jakarta")) last_trx
                FROM `athena-179008.vayu_data_mart.indodana_credit_credit_limit_accounts` cla
                LEFT JOIN athena-179008.vayu_data_mart.indodanamarketing_cli_application cli ON cla.id=cli.credit_limit_account_id
                LEFT JOIN athena-179008.vayu_data_mart.view_trx_contracts c ON c.credit_limit_account_id=cla.id AND c.type not like '%RESTRUCTURE%'
                INNER JOIN list_date_compare l ON l.description='2021-06-23 - developers - UPGRADE_CREDIT_LIMIT_ACCOUNT' AND l.compare_date=cla.dataDate
                WHERE cla.dataDate BETWEEN '2020-12-01' AND '2021-07-31'
                AND cla.id IN (SELECT credit_limit_account_id FROM group_upgrade_cla WHERE description='2021-06-23 - developers - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                AND cla.fee_scheme IN (SELECT fee_scheme FROM group_upgrade_cla WHERE description='2021-06-23 - developers - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                GROUP by 1,2,3,4,5,6,7,8,9,10
                UNION ALL
                SELECT 
                    cla.fee_scheme as partner
                    , cla.dataDate
                    , cla.id as credit_limit_account_id
                    , cli.orderId
                    , cla.limit
                    , cla.limit_balance
                    , cla.status
                    , '2021-07-23 - WORKER - UPGRADE_CREDIT_LIMIT_ACCOUNT' as cla_group
                    , l.notes
                    , l.m
                    , COUNT(c.entity_id) as contracts
                    , min(date(c.approved_date,"Asia/Jakarta")) first_trx
                    , max(date(c.approved_date,"Asia/Jakarta")) last_trx
                FROM `athena-179008.vayu_data_mart.indodana_credit_credit_limit_accounts` cla
                LEFT JOIN athena-179008.vayu_data_mart.indodanamarketing_cli_application cli ON cla.id=cli.credit_limit_account_id
                LEFT JOIN athena-179008.vayu_data_mart.view_trx_contracts c ON c.credit_limit_account_id=cla.id AND c.type not like '%RESTRUCTURE%'
                INNER JOIN list_date_compare l ON l.description='2021-07-23 - WORKER - UPGRADE_CREDIT_LIMIT_ACCOUNT' AND l.compare_date=cla.dataDate
                WHERE cla.dataDate BETWEEN '2020-12-01' AND '2021-07-31'
                AND cla.id IN (SELECT credit_limit_account_id FROM group_upgrade_cla WHERE description='2021-07-23 - WORKER - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                AND cla.fee_scheme IN (SELECT fee_scheme FROM group_upgrade_cla WHERE description='2021-07-23 - WORKER - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                GROUP by 1,2,3,4,5,6,7,8,9,10
            )
            , compare_data as (
                SELECT 
                    d1.cla_group
                    , d1.partner
                    , CAST(ABS(d1.m) AS String) || 'd ' || d1.notes || ' to ' || CAST(d3.m as STRING) || 'd ' || d3.notes as comparison_notes
                    , d1.orderId
                    , d2.dataDate as upgrade_limit_date
                    , d3.status as paylater_status
                    , d1.limit as old_limit
                    , d3.limit as new_limit
                    
                    , d3.limit-d3.limit_balance as limit_usage_new
                    , d1.limit-d1.limit_balance as limit_usage_old
                    , CASE WHEN d1.first_trx is null then 'Never Trx' WHEN d1.first_trx>=d2.dataDate THEN 'Yes' Else 'No' END as first_trx_after_upgrade
                FROM data d1
                LEFT JOIN data d2 ON d2.credit_limit_account_id=d1.credit_limit_account_id AND d2.notes='Upgrade' AND d1.cla_group=d2.cla_group
                LEFT JOIN data d3 ON d3.credit_limit_account_id=d1.credit_limit_account_id AND d3.notes='After Upgrade' AND d1.cla_group=d3.cla_group
                WHERE d1.notes='Before Upgrade'
            )
            SELECT * FROM compare_data WHERE coalesce(comparison_notes,'')<>''
        """.format(days_before=day_before, days_after=day_after)

        query_job = client.query(sql)
        df = query_job.to_dataframe()
        return df
    def get_historical_data_toCurrentDate(self, day_before):
        client = bigquery.Client(project="athena-179008")
        sql = """
            WITH comparison_date_config as (
                SELECT {days_before} as m, 'Before Upgrade' as notes
                UNION ALL
                SELECT 0 as m, 'After Upgrade' as notes
                UNION ALL
                SELECT 0 as m, 'Upgrade' as notes
            )
            , group_upgrade as (
                SELECT * FROM (
                    SELECT 
                        date(clal.created_at,"Asia/Jakarta") as log_date
                        , cla.fee_scheme
                        , clal.updater_role
                        , clal.action
                        , format_timestamp("%Y-%m-%d - ",clal.created_at,"Asia/Jakarta") || clal.updater_role || ' - ' || clal.action as description
                        , count(1) as total
                    FROM data-platform-indodana.vayu.indodana_credit_credit_limit_account_logs clal
                    INNER JOIN data-platform-indodana.vayu.indodana_credit_credit_limit_accounts cla ON cla.id=clal.credit_limit_account_id
                    WHERE clal.action IN  ('UPGRADE_CREDIT_LIMIT','UPGRADE_CREDIT_LIMIT_ACCOUNT')
                    AND date(clal.created_at,"Asia/Jakarta")>='2020-12-01'
                    group by 1,2,3,4,5
                    having count(1)>100
                )
                WHERE total>10000 OR log_date<='2021-04-14'
                ORDER BY 1 DESC
            )
            , group_upgrade_cla as (
                SELECT 
                    g.* EXCEPT(total)
                    , clal.credit_limit_account_id
                    , cli.orderId
                    , cli.partner
                FROM data-platform-indodana.vayu.indodana_credit_credit_limit_account_logs clal 
                INNER JOIN athena-179008.vayu_data_mart.indodanamarketing_cli_application cli ON cli.credit_limit_account_id=clal.credit_limit_account_id
                INNER JOIN group_upgrade g ON g.description=format_timestamp("%Y-%m-%d - ",clal.created_at,"Asia/Jakarta") || clal.updater_role || ' - ' || clal.action
                GROUP BY 1,2,3,4,5,6,7,8
            )
            , list_date_compare as (
                SELECT 
                    group_upgrade.description
                    , CASE 
                        WHEN comparison_date_config.notes='After Upgrade' THEN DATE_ADD(CURRENT_DATE,INTERVAL -1 DAY)
                        ELSE DATE_ADD(group_upgrade.log_date,INTERVAL comparison_date_config.m DAY) 
                    END as compare_date
                    , comparison_date_config.notes
                    , CASE 
                        WHEN comparison_date_config.notes='After Upgrade' THEN DATE_DIFF(DATE_ADD(CURRENT_DATE,INTERVAL -1 DAY),group_upgrade.log_date,DAY) 
                        ELSE comparison_date_config.m
                    END as m
                FROM group_upgrade, comparison_date_config
            )
            , data as (
                SELECT 
                    cla.fee_scheme as partner
                    , cla.dataDate
                    , cla.id as credit_limit_account_id
                    , cli.orderId
                    , cla.limit
                    , cla.limit_balance
                    , cla.status
                    , '2021-04-16 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT' as cla_group
                    , l.notes
                    , l.m
                    , COUNT(c.entity_id) as contracts
                    , min(date(c.approved_date,"Asia/Jakarta")) first_trx
                    , max(date(c.approved_date,"Asia/Jakarta")) last_trx
                FROM `athena-179008.vayu_data_mart.indodana_credit_credit_limit_accounts` cla
                LEFT JOIN athena-179008.vayu_data_mart.indodanamarketing_cli_application cli ON cla.id=cli.credit_limit_account_id
                LEFT JOIN athena-179008.vayu_data_mart.view_trx_contracts c ON c.credit_limit_account_id=cla.id AND c.type not like '%RESTRUCTURE%'
                INNER JOIN list_date_compare l ON l.description='2021-04-16 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT' AND l.compare_date=cla.dataDate
                WHERE cla.dataDate > '2020-12-01' 
                AND cla.id IN (SELECT credit_limit_account_id FROM group_upgrade_cla WHERE description='2021-04-16 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                AND cla.fee_scheme IN (SELECT fee_scheme FROM group_upgrade_cla WHERE description='2021-04-16 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                GROUP by 1,2,3,4,5,6,7,8,9,10
                UNION ALL
                SELECT 
                    cla.fee_scheme as partner
                    , cla.dataDate
                    , cla.id as credit_limit_account_id
                    , cli.orderId
                    , cla.limit
                    , cla.limit_balance
                    , cla.status
                    , '2021-04-14 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT' as cla_group
                    , l.notes
                    , l.m
                    , COUNT(c.entity_id) as contracts
                    , min(date(c.approved_date,"Asia/Jakarta")) first_trx
                    , max(date(c.approved_date,"Asia/Jakarta")) last_trx
                FROM `athena-179008.vayu_data_mart.indodana_credit_credit_limit_accounts` cla
                LEFT JOIN athena-179008.vayu_data_mart.indodanamarketing_cli_application cli ON cla.id=cli.credit_limit_account_id
                LEFT JOIN athena-179008.vayu_data_mart.view_trx_contracts c ON c.credit_limit_account_id=cla.id AND c.type not like '%RESTRUCTURE%'
                INNER JOIN list_date_compare l ON l.description='2021-04-14 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT' AND l.compare_date=cla.dataDate
                WHERE cla.dataDate > '2020-12-01' 
                AND cla.id IN (SELECT credit_limit_account_id FROM group_upgrade_cla WHERE description='2021-04-14 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                AND cla.fee_scheme IN (SELECT fee_scheme FROM group_upgrade_cla WHERE description='2021-04-14 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                GROUP by 1,2,3,4,5,6,7,8,9,10
                UNION ALL
                SELECT 
                    cla.fee_scheme as partner
                    , cla.dataDate
                    , cla.id as credit_limit_account_id
                    , cli.orderId
                    , cla.limit
                    , cla.limit_balance
                    , cla.status
                    , '2021-02-09 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT' as cla_group
                    , l.notes
                    , l.m
                    , COUNT(c.entity_id) as contracts
                    , min(date(c.approved_date,"Asia/Jakarta")) first_trx
                    , max(date(c.approved_date,"Asia/Jakarta")) last_trx
                FROM `athena-179008.vayu_data_mart.indodana_credit_credit_limit_accounts` cla
                LEFT JOIN athena-179008.vayu_data_mart.indodanamarketing_cli_application cli ON cla.id=cli.credit_limit_account_id
                LEFT JOIN athena-179008.vayu_data_mart.view_trx_contracts c ON c.credit_limit_account_id=cla.id AND c.type not like '%RESTRUCTURE%'
                INNER JOIN list_date_compare l ON l.description='2021-02-09 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT' AND l.compare_date=cla.dataDate
                WHERE cla.dataDate > '2020-12-01' 
                AND cla.id IN (SELECT credit_limit_account_id FROM group_upgrade_cla WHERE description='2021-02-09 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                AND cla.fee_scheme IN (SELECT fee_scheme FROM group_upgrade_cla WHERE description='2021-02-09 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                GROUP by 1,2,3,4,5,6,7,8,9,10
            )
            , compare_data as (
                SELECT 
                    d1.cla_group
                    , d1.partner
                    , CAST(ABS(d1.m) AS String) || 'd ' || d1.notes || ' to ' || CAST(d3.m as STRING) || 'd ' || d3.notes as comparison_notes
                    , d1.orderId
                    , d2.dataDate as upgrade_limit_date
                    , d3.status as paylater_status
                    , d1.limit as old_limit
                    , d3.limit as new_limit
                    
                    , d3.limit-d3.limit_balance as limit_usage_new
                    , d1.limit-d1.limit_balance as limit_usage_old
                    , CASE WHEN d1.first_trx is null then 'Never Trx' WHEN d1.first_trx>=d2.dataDate THEN 'Yes' Else 'No' END as first_trx_after_upgrade
                FROM data d1
                LEFT JOIN data d2 ON d2.credit_limit_account_id=d1.credit_limit_account_id AND d2.notes='Upgrade' AND d1.cla_group=d2.cla_group
                LEFT JOIN data d3 ON d3.credit_limit_account_id=d1.credit_limit_account_id AND d3.notes='After Upgrade' AND d1.cla_group=d3.cla_group
                WHERE d1.notes='Before Upgrade'
            )
            SELECT * FROM compare_data WHERE coalesce(comparison_notes,'')<>''
        """.format(days_before=day_before)

        query_job = client.query(sql)
        df = query_job.to_dataframe()
        return df
    def get_historical_data_1_toCurrentDate(self, day_before):
        client = bigquery.Client(project="athena-179008")
        sql = """
            WITH comparison_date_config as (
                SELECT {days_before} as m, 'Before Upgrade' as notes
                UNION ALL
                SELECT 0 as m, 'After Upgrade' as notes
                UNION ALL
                SELECT 0 as m, 'Upgrade' as notes
            )
            , group_upgrade as (
                SELECT * FROM (
                    SELECT 
                        date(clal.created_at,"Asia/Jakarta") as log_date
                        , cla.fee_scheme
                        , clal.updater_role
                        , clal.action
                        , format_timestamp("%Y-%m-%d - ",clal.created_at,"Asia/Jakarta") || clal.updater_role || ' - ' || clal.action as description
                        , count(1) as total
                    FROM data-platform-indodana.vayu.indodana_credit_credit_limit_account_logs clal
                    INNER JOIN data-platform-indodana.vayu.indodana_credit_credit_limit_accounts cla ON cla.id=clal.credit_limit_account_id
                    WHERE clal.action IN  ('UPGRADE_CREDIT_LIMIT','UPGRADE_CREDIT_LIMIT_ACCOUNT')
                    AND date(clal.created_at,"Asia/Jakarta")>='2020-12-01'
                    group by 1,2,3,4,5
                    having count(1)>100
                )
                WHERE total>10000 OR log_date<='2021-04-14'
                ORDER BY 1 DESC
            )
            , group_upgrade_cla as (
                SELECT 
                    g.* EXCEPT(total)
                    , clal.credit_limit_account_id
                    , cli.orderId
                    , cli.partner
                FROM data-platform-indodana.vayu.indodana_credit_credit_limit_account_logs clal 
                INNER JOIN athena-179008.vayu_data_mart.indodanamarketing_cli_application cli ON cli.credit_limit_account_id=clal.credit_limit_account_id
                INNER JOIN group_upgrade g ON g.description=format_timestamp("%Y-%m-%d - ",clal.created_at,"Asia/Jakarta") || clal.updater_role || ' - ' || clal.action
                GROUP BY 1,2,3,4,5,6,7,8
            )
            , list_date_compare as (
                SELECT 
                    group_upgrade.description
                    , CASE 
                        WHEN comparison_date_config.notes='After Upgrade' THEN DATE_ADD(CURRENT_DATE,INTERVAL -1 DAY)
                        ELSE DATE_ADD(group_upgrade.log_date,INTERVAL comparison_date_config.m DAY) 
                    END as compare_date
                    , comparison_date_config.notes
                    , CASE 
                        WHEN comparison_date_config.notes='After Upgrade' THEN DATE_DIFF(DATE_ADD(CURRENT_DATE,INTERVAL -1 DAY),group_upgrade.log_date,DAY) 
                        ELSE comparison_date_config.m
                    END as m
                FROM group_upgrade, comparison_date_config
            )
            , data as (
                SELECT 
                    cla.fee_scheme as partner
                    , cla.dataDate
                    , cla.id as credit_limit_account_id
                    , cli.orderId
                    , cla.limit
                    , cla.limit_balance
                    , cla.status
                    , '2021-06-21 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT' as cla_group
                    , l.notes
                    , l.m
                    , COUNT(c.entity_id) as contracts
                    , min(date(c.approved_date,"Asia/Jakarta")) first_trx
                    , max(date(c.approved_date,"Asia/Jakarta")) last_trx
                FROM `athena-179008.vayu_data_mart.indodana_credit_credit_limit_accounts` cla
                LEFT JOIN athena-179008.vayu_data_mart.indodanamarketing_cli_application cli ON cla.id=cli.credit_limit_account_id
                LEFT JOIN athena-179008.vayu_data_mart.view_trx_contracts c ON c.credit_limit_account_id=cla.id AND c.type not like '%RESTRUCTURE%'
                INNER JOIN list_date_compare l ON l.description='2021-06-21 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT' AND l.compare_date=cla.dataDate
                WHERE cla.dataDate > '2020-12-01' 
                AND cla.id IN (SELECT credit_limit_account_id FROM group_upgrade_cla WHERE description='2021-06-21 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                AND cla.fee_scheme IN (SELECT fee_scheme FROM group_upgrade_cla WHERE description='2021-06-21 - data-analyst - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                GROUP by 1,2,3,4,5,6,7,8,9,10
                UNION ALL
                SELECT 
                    cla.fee_scheme as partner
                    , cla.dataDate
                    , cla.id as credit_limit_account_id
                    , cli.orderId
                    , cla.limit
                    , cla.limit_balance
                    , cla.status
                    , '2021-06-23 - developers - UPGRADE_CREDIT_LIMIT_ACCOUNT' as cla_group
                    , l.notes
                    , l.m
                    , COUNT(c.entity_id) as contracts
                    , min(date(c.approved_date,"Asia/Jakarta")) first_trx
                    , max(date(c.approved_date,"Asia/Jakarta")) last_trx
                FROM `athena-179008.vayu_data_mart.indodana_credit_credit_limit_accounts` cla
                LEFT JOIN athena-179008.vayu_data_mart.indodanamarketing_cli_application cli ON cla.id=cli.credit_limit_account_id
                LEFT JOIN athena-179008.vayu_data_mart.view_trx_contracts c ON c.credit_limit_account_id=cla.id AND c.type not like '%RESTRUCTURE%'
                INNER JOIN list_date_compare l ON l.description='2021-06-23 - developers - UPGRADE_CREDIT_LIMIT_ACCOUNT' AND l.compare_date=cla.dataDate
                WHERE cla.dataDate > '2020-12-01' 
                AND cla.id IN (SELECT credit_limit_account_id FROM group_upgrade_cla WHERE description='2021-06-23 - developers - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                AND cla.fee_scheme IN (SELECT fee_scheme FROM group_upgrade_cla WHERE description='2021-06-23 - developers - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                GROUP by 1,2,3,4,5,6,7,8,9,10
                UNION ALL
                SELECT 
                    cla.fee_scheme as partner
                    , cla.dataDate
                    , cla.id as credit_limit_account_id
                    , cli.orderId
                    , cla.limit
                    , cla.limit_balance
                    , cla.status
                    , '2021-07-23 - WORKER - UPGRADE_CREDIT_LIMIT_ACCOUNT' as cla_group
                    , l.notes
                    , l.m
                    , COUNT(c.entity_id) as contracts
                    , min(date(c.approved_date,"Asia/Jakarta")) first_trx
                    , max(date(c.approved_date,"Asia/Jakarta")) last_trx
                FROM `athena-179008.vayu_data_mart.indodana_credit_credit_limit_accounts` cla
                LEFT JOIN athena-179008.vayu_data_mart.indodanamarketing_cli_application cli ON cla.id=cli.credit_limit_account_id
                LEFT JOIN athena-179008.vayu_data_mart.view_trx_contracts c ON c.credit_limit_account_id=cla.id AND c.type not like '%RESTRUCTURE%'
                INNER JOIN list_date_compare l ON l.description='2021-07-23 - WORKER - UPGRADE_CREDIT_LIMIT_ACCOUNT' AND l.compare_date=cla.dataDate
                WHERE cla.dataDate > '2020-12-01' 
                AND cla.id IN (SELECT credit_limit_account_id FROM group_upgrade_cla WHERE description='2021-07-23 - WORKER - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                AND cla.fee_scheme IN (SELECT fee_scheme FROM group_upgrade_cla WHERE description='2021-07-23 - WORKER - UPGRADE_CREDIT_LIMIT_ACCOUNT')
                GROUP by 1,2,3,4,5,6,7,8,9,10
            )
            , compare_data as (
                SELECT 
                    d1.cla_group
                    , d1.partner
                    , CAST(ABS(d1.m) AS String) || 'd ' || d1.notes || ' to ' || CAST(d3.m as STRING) || 'd ' || d3.notes as comparison_notes
                    , d1.orderId
                    , d2.dataDate as upgrade_limit_date
                    , d3.status as paylater_status
                    , d1.limit as old_limit
                    , d3.limit as new_limit
                    
                    , d3.limit-d3.limit_balance as limit_usage_new
                    , d1.limit-d1.limit_balance as limit_usage_old
                    , CASE WHEN d1.first_trx is null then 'Never Trx' WHEN d1.first_trx>=d2.dataDate THEN 'Yes' Else 'No' END as first_trx_after_upgrade
                FROM data d1
                LEFT JOIN data d2 ON d2.credit_limit_account_id=d1.credit_limit_account_id AND d2.notes='Upgrade' AND d1.cla_group=d2.cla_group
                LEFT JOIN data d3 ON d3.credit_limit_account_id=d1.credit_limit_account_id AND d3.notes='After Upgrade' AND d1.cla_group=d3.cla_group
                WHERE d1.notes='Before Upgrade'
            )
            SELECT * FROM compare_data WHERE coalesce(comparison_notes,'')<>''
        """.format(days_before=day_before)

        query_job = client.query(sql)
        df = query_job.to_dataframe()
        return df