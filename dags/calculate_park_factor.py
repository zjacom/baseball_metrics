from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago

import pendulum
import pymysql
pymysql.install_as_MySQLdb()

kst = pendulum.timezone("Asia/Seoul")

start_date = kst.convert(days_ago(1))

dag = DAG(
    dag_id="calculate_park_factor",
    start_date=start_date,
    schedule_interval="0 4 * * *",
    catchup=False,
)

hook = MySqlHook(mysql_conn_id='mysql_conn')
"""
((Decimal('1.5172'), '창원'), (Decimal('0.9810'), '수원'), (Decimal('0.8115'), '사직'), (Decimal('1.0769'), '대전'),
(Decimal('1.5283'), '잠실'), (Decimal('0.8265'), '대구'), (Decimal('0.9708'), '문학'), (Decimal('1.1765'), '광주'),
(Decimal('0.8866'), '고척'))
"""

def _calculate_park_factor(**context):
    sql_query = """
    SELECT (SUM(home_score) / SUM(away_score)) as park_factor, stadium
    FROM game_records
    GROUP BY stadium;
    """
    park_factors = hook.get_records(sql=sql_query)
    park_factors = [(float(factor), stadium) for factor, stadium in park_factors]

    context['task_instance'].xcom_push(key='park_factors', value=park_factors)



def _insert_data(**context):
    park_factors = context['task_instance'].xcom_pull(task_ids='calculate_park_factor', key='park_factors')

    for park_factor, stadium in park_factors:
        insert_query = "INSERT INTO park_factor (stadium, value) values(%s, %s)"
        hook.run(insert_query, parameters=(stadium, park_factor))


calculate_park_factor = PythonOperator(
    task_id='calculate_park_factor',
    python_callable=_calculate_park_factor,
    provide_context=True,
    dag=dag
)

drop_table = MySqlOperator(
    task_id='drop_table',
    sql="DROP TABLE IF EXISTS park_factor;",
    mysql_conn_id='mysql_conn',
    dag=dag,
)

create_table = MySqlOperator(
    task_id='create_table',
    sql="""
        CREATE TABLE IF NOT EXISTS park_factor (
            stadium VARCHAR(16),
            value FLOAT
        );
    """,
    mysql_conn_id='mysql_conn',
    dag=dag,
)

insert_data = PythonOperator(
    task_id='insert_data',
    python_callable=_insert_data,
    dag=dag
)

calculate_park_factor >> drop_table >> create_table >> insert_data