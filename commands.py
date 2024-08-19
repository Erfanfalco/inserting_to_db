# commands for postgres cubing job

daily_usable_credit_cmd = ('''SELECT  customer_data.branch_name, SUM(usable_credit), date_to_ge
                    FROM customer_credit_data
                    INNER join customer_data ON customer_data.account_number = customer_credit_data.account_number
                    WHERE TO_DATE(customer_credit_data.date_to_ge, 'DD/MM/YYYY') = '{0}'
                    GROUP BY ROLLUP (customer_data.branch_name, date_to_ge)
                    ORDER BY date_to_ge;
                    ''')


daily_final_credit_cmd = ('''SELECT cd.branch_name, sum(fn.credit)::float as FinalCredit , fn.tr_ge_date::text
                    FROM financial_status_data fn
                    INNER join customer_data cd
                    on cd.account_number = fn.account_number
                    WHERE fn.tr_ge_date = '{0}'
                    GROUP by cd.branch_name, fn.tr_ge_date
                    order by branch_name, FinalCredit DESC;
                    ''')


daily_transactions_cmd = (''' WITH datas AS (
                    SELECT 
                        tr.branch_id,
                        bool_or(tr.is_a_purchase) AS is_a_purchase,
                        SUM(tr.amount) AS total_amount,
                        MIN(tr.transaction_date_ge) AS earliest_transaction_date,
                        cs.stock_ins_max_l_code
                    FROM 
                        public.transaction_agg_data tr
                    INNER JOIN 
                        customer_stock cs ON tr.dbs_account_number = cs.account_number
                    WHERE tr.transaction_date_ge = '{0}'
                    GROUP BY 
                        tr.branch_id, cs.stock_ins_max_l_code
                    )
                    SELECT 
                        cd.branch_name,
                        bool_or(datas.is_a_purchase) AS is_a_purchase,
                        MIN(datas.earliest_transaction_date) AS Earliest_transaction_date,
                        MAX(datas.total_amount) AS Total_amount,
                        datas.stock_ins_max_l_code
                    FROM 
                        datas
                    INNER JOIN 
                        customer_data cd ON cd.branch_id = datas.branch_id
                    GROUP BY 
                        cd.branch_name, datas.stock_ins_max_l_code;
                    ''')


weekly_wage_cmd = ('''SELECT DATE_PART('week', (tr_ge_date + INTERVAL '2 days' - INTERVAL '12 weeks')::date) AS Week_Number,
                    SUM(interest) AS Total_Interest,
                    MIN(tr_ge_date) as tr_ge_date
                    FROM public.financial_status_data fs
                    WHERE fs.tr_ge_date = '{0}'
                    GROUP BY Week_Number 
                    ORDER BY tr_ge_date DESC
                    ''')


# weekly queries
weekly_wage_cube_query = 'INSERT INTO public.weekly_wage_cube (week_number, total_interest, tr_ge_date) VALUES (%s, %s, %s) ;'

# daily queries
usable_credit_cube_query = 'INSERT INTO usable_credit_cube (branch_name, sum_credit, date) VALUES (%s, %s, %s) ;'

final_credit_cube_query = 'INSERT INTO final_credit_cube (branch_name, final_credit, tr_ge_date) VALUES (%s, %s, %s) ;'

transaction_cube_query = 'INSERT INTO transaction_cube (branch_name, is_a_purchase, date, total_amount, stock_code) VALUES (%s, %s, %s, %s, %s) ;'


checking_weekly_wage_cube = ("SELECT * FROM public.weekly_wage_cube "
                             "WHERE week_number = DATE_PART('week', ('{0}'::date + INTERVAL '2 days' - INTERVAL '12 weeks')::date);")

checking_usable_credit_cube = "SELECT * FROM public.usable_credit_cube WHERE date = '{0}';"

checking_final_credit_cube = "SELECT * FROM public.final_credit_cube WHERE tr_ge_date = '{0}';"

checking_transaction_cube = "SELECT * FROM public.transaction_cube WHERE date = '{0}';"
