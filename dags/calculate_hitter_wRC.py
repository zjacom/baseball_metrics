from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from datetime import datetime

import pendulum
import pymysql
pymysql.install_as_MySQLdb()

import logging
import time
hook = MySqlHook(mysql_conn_id='mysql_conn')

kst = pendulum.timezone("Asia/Seoul")
from airflow.utils.dates import days_ago
start_date = kst.convert(days_ago(1))

dag = DAG(
    dag_id="calculate_hitter_wRC",
    start_date=start_date,
    schedule_interval=None,
    catchup=False,
)


def _calculate_wRC():
    get_league_wOBA_query = "SELECT AVG(wOBA) FROM hitter_metrics;"
    league_wOBA = hook.get_first(get_league_wOBA_query)[0]
    logging.info(league_wOBA)

    sql_query = "SELECT player_id, wOBA FROM hitter_metrics;"
    records = hook.get_records(sql_query)

    get_league_query = "SELECT total_pa, total_runs FROM league_info;"
    total_pa, total_runs = hook.get_first(get_league_query)[0], hook.get_first(get_league_query)[1]

    for record in records:
        player_id, wOBA = record[0], record[1]
        get_hitter_query = f"SELECT on_base_percentage, at_bat FROM hitter_info WHERE player_id = {player_id}"
        obp = hook.get_first(get_hitter_query)[0]
        pa = hook.get_first(get_hitter_query)[1]
        if wOBA == 0 or obp == 0:
            continue
        wRC = ((wOBA - league_wOBA) / (obp / wOBA) + (total_runs / total_pa)) * pa
        logging.info(wRC)
        insert_query = """
                    INSERT INTO hitter_metrics (player_id, wRC)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE
                    wRC = VALUES(wRC);
                """
        hook.run(insert_query, parameters=(player_id, wRC))
        time.sleep(0.5)


calculate_wRC = PythonOperator(
    task_id='calculate_wRC',
    python_callable=_calculate_wRC,
    dag=dag
)

calculate_wRC