from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

import pendulum
import pymysql
pymysql.install_as_MySQLdb()

kst = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id="calculate_league_metrics",
    start_date=datetime(2024, 6, 6, tzinfo=kst),
    schedule_interval=None,
    catchup=False,
)
hook = MySqlHook(mysql_conn_id='mysql_conn')


def _insert_league_metrics():
    get_league_wRC_qeury = "SELECT SUM(wRC) FROM hitter_metrics WHERE wRC IS NOT NULL;"
    league_wRC = hook.get_first(get_league_wRC_qeury)[0]
    get_leahue_ERA_query = "SELECT AVG(ERA) FROM pitcher_info WHERE ERA IS NOT NULL;"
    league_ERA = hook.get_first(get_leahue_ERA_query)[0]
    
    # 업데이트 쿼리 실행
    update_query = f"""
    UPDATE league_info
    SET league_wRC = {league_wRC}, league_ERA = {league_ERA}
    WHERE league_wRC IS NULL AND league_ERA IS NULL;
    """
    hook.run(update_query)


insert_league_metrics = PythonOperator(
    task_id='insert_league_metrics',
    python_callable=_insert_league_metrics,
    dag=dag
)

trigger_get_today_lineup = TriggerDagRunOperator(
    task_id='trigger_get_today_lineup',
    trigger_dag_id='my_calcget_today_lineupulus',
    dag=dag
)

insert_league_metrics >> trigger_get_today_lineup
