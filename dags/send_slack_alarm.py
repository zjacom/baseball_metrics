import os
import sys
# Airflow가 실행되는 경로에서 plugins 폴더를 찾을 수 있도록 경로를 설정
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from my_slack import send_message_to_a_slack_channel

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime

import pendulum
import pymysql
pymysql.install_as_MySQLdb()

kst = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id="send_slack_alarm",
    start_date=datetime(2024, 6, 6, tzinfo=kst),
    schedule_interval=None,
    # schedule_interval="0 3 * * *",
    catchup=False,
)
hook = MySqlHook(mysql_conn_id='mysql_conn')


def _send_slack_alarm():
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("SELECT game_time, away, home, away_ERAp, away_wRCp, home_ERAp, home_wRCp FROM today_games")

    row = cursor.fetchone()
    while row:
        game_time, away, home, away_ERAp, away_wRCp, home_ERAp, home_wRCp = row
        if away_ERAp + away_wRCp > home_ERAp + home_wRCp:
            message = f"{game_time}분 {away} vs {home} 경기에서 {away}의 승리가 예상됩니다."
        else:
            message = f"{game_time}분 {away} vs {home} 경기에서 {home}의 승리가 예상됩니다."
        send_message_to_a_slack_channel(message, ":scream:")
        row = cursor.fetchone()


send_slack_alarm = PythonOperator(
    task_id='send_slack_alarm',
    python_callable=_send_slack_alarm,
    dag=dag,
)

send_slack_alarm