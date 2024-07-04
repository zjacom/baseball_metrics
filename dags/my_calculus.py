from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta


import pendulum
import pymysql
pymysql.install_as_MySQLdb()

kst = pendulum.timezone("Asia/Seoul")
from airflow.utils.dates import days_ago
start_date = kst.convert(days_ago(1))

dag = DAG(
    dag_id="my_calculus",
    start_date=start_date,
    schedule_interval=None,
    # schedule_interval="0 3 * * *",
    catchup=False,
)
hook = MySqlHook(mysql_conn_id='mysql_conn')

teams = ["두산", "LG", "KIA", "NC", "한화", "SSG", "삼성", "키움", "KT", "롯데"]


def _query_view(**context):
    for team in teams:
        hitter_query = f"""
            SELECT AVG(metric)
            FROM today_lineup
            WHERE team = '{team}' AND position <> '선발 투수' AND metric IS NOT NULL;
            """
        hitter_metric = hook.get_first(hitter_query)[0]

        pitcher_query = f"""
            SELECT metric
            FROM today_lineup
            WHERE team = '{team}' AND position = '선발 투수' AND metric IS NOT NULL;
            """
        pitcher_metric = hook.get_first(pitcher_query)[0]

        execution_date = context['execution_date']
        korean_time = execution_date + timedelta(hours=9)
        game_date = korean_time.strftime('%Y-%m-%d')
        if hitter_metric and pitcher_metric:
            update_sql = f"""
                UPDATE today_games
                SET 
                    away_wRCp = CASE 
                                    WHEN game_date = '{game_date}' AND away = '{team}' THEN {hitter_metric}
                                    ELSE away_wRCp
                                END,
                    away_ERAp = CASE 
                                    WHEN game_date = '{game_date}' AND away = '{team}' THEN {pitcher_metric}
                                    ELSE away_ERAp
                                END,
                    home_wRCp = CASE 
                                    WHEN game_date = '{game_date}' AND home = '{team}' THEN {hitter_metric}
                                    ELSE home_wRCp
                                END,
                    home_ERAp = CASE 
                                    WHEN game_date = '{game_date}' AND home = '{team}' THEN {pitcher_metric}
                                    ELSE home_ERAp
                                END
                WHERE 
                    game_date = '{game_date}' AND (away = '{team}' OR home = '{team}');
                """
            hook.run(update_sql)



query_view = PythonOperator(
    task_id='query_view',
    python_callable=_query_view,
    dag=dag
)

trigger_send_slack_alarm = TriggerDagRunOperator(
    task_id='trigger_send_slack_alarm',
    trigger_dag_id='send_slack_alarm',
    dag=dag
)

query_view >> trigger_send_slack_alarm