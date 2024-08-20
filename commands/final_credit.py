
daily_final_credit_cmd = ('''SELECT cd.branch_name, sum(fn.credit)::float as FinalCredit , fn.tr_ge_date::text
                    FROM financial_status_data fn
                    INNER join customer_data cd
                    on cd.account_number = fn.account_number
                    WHERE fn.tr_ge_date = '{0}'
                    GROUP by cd.branch_name, fn.tr_ge_date
                    order by branch_name, FinalCredit DESC;
                    ''')


final_credit_cube_query = 'INSERT INTO final_credit_cube (branch_name, final_credit, tr_ge_date) VALUES (%s, %s, %s) ;'


checking_date_final_credit_cube = "SELECT * FROM public.final_credit_cube WHERE tr_ge_date = '{0}';"


updating_amount_final_credit_cube = "UPDATE public.final_credit_cube SET final_credit = '{0}' WHERE tr_ge_date = '{1}';"
