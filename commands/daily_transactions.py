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
                        MAX(datas.total_amount) AS Total_amount,
                        MIN(datas.earliest_transaction_date) AS Earliest_transaction_date,
                        bool_or(datas.is_a_purchase) AS is_a_purchase,
                        datas.stock_ins_max_l_code
                    FROM 
                        datas
                    INNER JOIN 
                        customer_data cd ON cd.branch_id = datas.branch_id
                    GROUP BY 
                        cd.branch_name, datas.stock_ins_max_l_code;
                    ''')


transaction_cube_query = 'INSERT INTO transaction_cube (branch_name, total_amount, date, is_a_purchase, stock_code) VALUES (%s, %s, %s, %s, %s) ;'


checking_date_transaction_cube = "SELECT * FROM public.transaction_cube WHERE date = '{0}';"


updating_amount_transaction_cube = "UPDATE public.transaction_cube SET total_amount = '{0}' WHERE date = '{1}';"
