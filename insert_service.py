from apscheduler.schedulers.blocking import BlockingScheduler
import configparser
import psycopg2
import logging
from termcolor import colored
import datetime as dt
from commands import usable_credit_cube_query, transaction_cube_query, final_credit_cube_query, daily_usable_credit_cmd, \
    daily_transactions_cmd, daily_final_credit_cmd, weekly_wage_cmd, weekly_wage_cube_query, \
    checking_usable_credit_cube, checking_transaction_cube, checking_final_credit_cube, checking_weekly_wage_cube

logging.basicConfig(level=logging.INFO)

config = configparser.ConfigParser()
path = r"C:\Users\erfan\Downloads\projects\ETL\Prediction\App\routers\DbInfo.ini"
config.read(path)


def get_data(cur,  date, period='daily'):
    command_map = {
        'daily': [
            daily_usable_credit_cmd,
            daily_transactions_cmd,
            daily_final_credit_cmd
        ],
        'weekly': [
            weekly_wage_cmd
        ]
    }

    cmd_list = command_map[period]
    exe_list = []

    for query in cmd_list:
        try:
            cur.execute(query.format(date))
            exe_list.append(cur.fetchall())
        except Exception as e:
            logging.error(f"An error occurred: {e}")
    return exe_list


def insert_data(cur, conn, period, queries):
    limit_days = 0
    while limit_days < 7:
        date = dt.datetime.now().date() - dt.timedelta(days=limit_days)
        conn.autocommit = True

        inserted_data = get_data(cur, date, period)
        flags = is_exist(conn, inserted_data, period)

        for que, data, flag in zip(queries, inserted_data, flags):

            if any(any(item is not None for item in sub_data) for sub_data in data):
                if not flag:
                    try:
                        cur.executemany(que, data)
                        logging.info(colored("Data inserted successfully.", "green", force_color=True))
                    except Exception as e:
                        logging.error(f"An error occurred: {e}")
                        conn.rollback()
                        continue
                else:
                    logging.info("Data is exist!")
            else:
                logging.info(colored("query return None from db", "yellow", force_color=True))
        limit_days += 1


def is_exist(conn, data, period):
    cur = conn.cursor()

    command_map = {
        'daily': [
            checking_usable_credit_cube,
            checking_transaction_cube,
            checking_final_credit_cube
        ],
        'weekly': [
            checking_weekly_wage_cube
        ]
    }

    select_query = command_map[period]
    results = []
    for query, table in zip(select_query, data):
        hasResponse = False
        for row in table:
            cur.execute(query.format(row[2]))
            response = cur.fetchone()

            if response is not None:
                hasResponse = True
                break

        results.append(hasResponse)
    return results


def create_conn():
    host = config.get('my_db', 'host')
    dbname = config.get('my_db', 'dbname')
    user = config.get('my_db', 'user')
    password = config.get('my_db', 'password')
    conn_string = f"host={host} dbname={dbname} user={user} password={password}"
    conn = psycopg2.connect(conn_string)
    return conn


# def schedule_insert_job():
#     scheduler = BlockingScheduler()
#     conn = create_conn()
#     with conn.cursor() as cur:
#         scheduler.add_job(lambda: insert_data(cur, conn, 'daily',
#                                               [usable_credit_cube_query, transaction_cube_query,
#                                                final_credit_cube_query]), 'cron', hour=15, minute=30, second=0,
#                           day_of_week="sat-sun-mon-tue-wed")
#
#         scheduler.add_job(lambda: insert_data(cur, conn, 'weekly',
#                                               [weekly_wage_cube_query]), 'cron', hour=15, minute=30, second=0,
#                           day_of_week="fri")
#     try:
#         scheduler.start()
#     except (KeyboardInterrupt, SystemExit):
#         pass


if __name__ == "__main__":
    # schedule_insert_job()
    conns = create_conn()
    with conns.cursor() as curs:
        insert_data(curs, conns, 'daily',
                    [usable_credit_cube_query, transaction_cube_query,
                     final_credit_cube_query])
        insert_data(curs, conns, 'weekly',
                    [weekly_wage_cube_query])
