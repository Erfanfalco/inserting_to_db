
daily_usable_credit_cmd = ('''SELECT  customer_data.branch_name, SUM(usable_credit), date_to_ge
                    FROM customer_credit_data
                    INNER join customer_data ON customer_data.account_number = customer_credit_data.account_number
                    WHERE TO_DATE(customer_credit_data.date_to_ge, 'DD/MM/YYYY') = '{0}'
                    GROUP BY ROLLUP (customer_data.branch_name, date_to_ge)
                    ORDER BY date_to_ge;
                    ''')

usable_credit_cube_query = 'INSERT INTO usable_credit_cube (branch_name, sum_credit, date) VALUES (%s, %s, %s) ;'


checking_usable_credit_cube = "SELECT * FROM public.usable_credit_cube WHERE date = '{0}';"

