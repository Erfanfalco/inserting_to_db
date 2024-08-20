from apscheduler.schedulers.blocking import BlockingScheduler
import logging
from termcolor import colored
import datetime as dt

from database import DatabaseConnection

from commands.daily_transactions import daily_transactions_cmd, checking_date_transaction_cube, transaction_cube_query, \
    updating_amount_transaction_cube
from commands.final_credit import daily_final_credit_cmd, checking_date_final_credit_cube, final_credit_cube_query, \
    updating_amount_final_credit_cube
from commands.usable_credit import daily_usable_credit_cmd, checking_date_usable_credit_cube, usable_credit_cube_query, \
    updating_amount_usable_credit_cube
from commands.weekly_wage import weekly_wage_cmd, checking_date_weekly_wage_cube, weekly_wage_cube_query, \
    updating_amount_weekly_wage_cube

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize database connection
db_config_path = r"C:\Users\erfan\Downloads\projects\DbInfo.ini"
db_conn = DatabaseConnection(db_config_path)
db_conn.connect()


def get_data(cursor, date, period='daily'):
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
            cursor.execute(query.format(date))
            exe_list.append(cursor.fetchall())
        except Exception as e:
            logging.error(f"An error occurred: {e}")

    return exe_list


def insert_data(cur, conn, period, queries):
    date = dt.datetime.now().date()

    day_start = 0
    limit_days = 7

    while day_start < limit_days:
        query_date = date - dt.timedelta(days=day_start)

        inserted_data = get_data(cur, query_date, period)
        exist = is_exist(conn, inserted_data, period)

        for query, data, flag in zip(queries, inserted_data, exist):

            if any(any(item is not None for item in sub_data) for sub_data in data):
                if not flag:
                    try:
                        cur.executemany(query, data)
                        conn.commit()
                        logging.info(colored("Data inserted successfully.", "light_green", force_color=True))
                    except Exception as e:
                        logging.error(f"An error occurred: {e}")
                        conn.rollback()
                        continue
                else:
                    logging.info(f"Data in {query_date} is exist!")
        day_start += 1

    logging.info(
        colored(f"*** {period} data in {date} has run to insert and cube, successfully ***", color="cyan",
                force_color=True))


def is_exist(connection, data, period):
    cursor = connection.cursor()
    checking_query = {
        'daily': [
            checking_date_usable_credit_cube,
            checking_date_transaction_cube,
            checking_date_final_credit_cube
        ],
        'weekly': [
            checking_date_weekly_wage_cube
        ]
    }
    updating_query = {
        'daily': [
            updating_amount_usable_credit_cube,
            updating_amount_transaction_cube,
            updating_amount_final_credit_cube
        ],
        'weekly': [
            updating_amount_weekly_wage_cube
        ]
    }

    check = checking_query[period]
    update = updating_query[period]

    results = []
    for date_query, table, amount_query in zip(check, data, update):
        hasResponse = False
        for row in table:
            cursor.execute(date_query.format(row[2]))
            response = cursor.fetchone()

            if response is not None:
                hasResponse = True
                try:
                    cursor.execute(amount_query.format(row[1], row[2]))
                    connection.commit()
                except Exception as e:
                    logging.error(f"An error occurred: {e}")
                    continue

                break

        results.append(hasResponse)
    return results


def schedule_insert_job():
    scheduler = BlockingScheduler()
    connect = db_conn.get_connection()
    with db_conn.get_cursor() as cur:
        scheduler.add_job(lambda: insert_data(cur, connect, 'daily',
                                              [usable_credit_cube_query, transaction_cube_query,
                                               final_credit_cube_query]), 'cron', hour=15, minute=30, second=0,
                          day_of_week="sat-sun-mon-tue-wed")

        scheduler.add_job(lambda: insert_data(cur, connect, 'weekly',
                                              [weekly_wage_cube_query]), 'cron', hour=15, minute=30, second=0,
                          day_of_week="fri")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit) as e:
        logging.error(f"Scheduler interrupted: {e}")


if __name__ == "__main__":
    schedule_insert_job()

    # connect = db_conn.get_connection()
    # with db_conn.get_cursor() as curs:
    #     insert_data(curs, connect, 'daily',
    #                 [usable_credit_cube_query, transaction_cube_query,
    #                  final_credit_cube_query])
    #     insert_data(curs, connect, 'weekly',
    #                 [weekly_wage_cube_query])
