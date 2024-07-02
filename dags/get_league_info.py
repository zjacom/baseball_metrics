from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import pendulum
import pymysql
pymysql.install_as_MySQLdb()

kst = pendulum.timezone("Asia/Seoul")
from airflow.utils.dates import days_ago
start_date = kst.convert(days_ago(1))

dag = DAG(
    dag_id="get_league_info",
    start_date=start_date,
    schedule_interval="0 5 * * *",
    catchup=False,
)
hook = MySqlHook(mysql_conn_id='mysql_conn')


def _crawling(**context):
    chrome_options = Options()
    chrome_options.add_argument('--ignore-ssl-errors=yes')
    chrome_options.add_argument('--ignore-certificate-errors')

    user_agent = 'userMozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
    chrome_options.add_argument(f'user-agent={user_agent}')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--headless')

    remote_webdriver = 'remote_chromedriver'
    with webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=chrome_options) as driver:
        url = 'https://www.koreabaseball.com/Record/Team/Hitter/Basic1.aspx'
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, 'html.parser')

    league_info = soup.find("div", {"class" : "sub-content"}).find("tfoot").find_all('td')
    # 총 경기수, 총 타석수, 총 득점수
    total_games, total_at_bats, total_score = int(league_info[2].text), int(league_info[3].text), int(league_info[5].text)

    task_instance = context['task_instance']
    task_instance.xcom_push(key='total_games', value=total_games)
    task_instance.xcom_push(key='total_at_bats', value=total_at_bats)
    task_instance.xcom_push(key='total_score', value=total_score)

def _generate_sql(**context):
    task_instance = context['task_instance']
    total_games = task_instance.xcom_pull(task_ids='crawling', key='total_games')
    total_at_bats = task_instance.xcom_pull(task_ids='crawling', key='total_at_bats')
    total_score = task_instance.xcom_pull(task_ids='crawling', key='total_score')
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS league_info (
        total_pa INT,
        total_runs INT,
        total_games INT,
        league_wRC FLOAT,
        league_ERA FLOAT
    );
    """
    insert_query = f"""
    INSERT INTO league_info (total_pa, total_runs, total_games)
    VALUES ({total_at_bats}, {total_score}, {total_games});
    """
    
    task_instance.xcom_push(key='create_table_query', value=create_table_query)
    task_instance.xcom_push(key='insert_query', value=insert_query)

generate_sql = PythonOperator(
    task_id='generate_sql',
    python_callable=_generate_sql,
    provide_context=True,
    dag=dag
)

delete_table = MySqlOperator(
    task_id='delete_table',
    mysql_conn_id='mysql_conn',
    sql="DROP TABLE IF EXISTS league_info;",
    dag=dag
)

create_table = MySqlOperator(
    task_id='create_table',
    mysql_conn_id='mysql_conn',
    sql="{{ task_instance.xcom_pull(task_ids='generate_sql', key='create_table_query') }}",
    dag=dag
)

insert_data = MySqlOperator(
    task_id='insert_data',
    mysql_conn_id='mysql_conn',
    sql="{{ task_instance.xcom_pull(task_ids='generate_sql', key='insert_query') }}",
    dag=dag
)

crawling = PythonOperator(
    task_id='crawling',
    python_callable=_crawling,
    dag=dag
)

crawling >> generate_sql >> delete_table >> create_table >> insert_data
