

portfo_composition_cmd = ('''SELECT stock_ins_max_l_code, Sum(stock_price) as stock_price, cs.date_to_ge, sum(cd.usable_credit) as usable_credit
                            from customer_stock cs
                            INNER join customer_credit_data cd
                            on cd.account_number = cs.account_number
                            WHERE TO_DATE(cs.date_to_ge, 'YYYY-MM-DD') = '2024-05-21'
                            GROUP by cs.stock_ins_max_l_code, cs.date_to_ge;
                            ''')

portfo_composition_cube_query = ('''INSERT INTO portfo_composition_cube (stock_code, stock_price, date_to_ge, usable_credit) VALUES (%s, %s, %s, %s)''')


checking_date_portfo_composition_cube = "SELECT * FROM public.portfo_composition_cube WHERE date_to_ge = '{0}';"


updating_amount_portfo_composition_cube = "UPDATE public.portfo_composition_cube SET usable_credit = '{0}' WHERE date_to_ge = '{1}';"