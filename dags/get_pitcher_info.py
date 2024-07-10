from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import pendulum
import pymysql
import time
pymysql.install_as_MySQLdb()

kst = pendulum.timezone("Asia/Seoul")
from airflow.utils.dates import days_ago
start_date = kst.convert(days_ago(1))

dag = DAG(
    dag_id="get_pitcher_info",
    start_date=start_date,
    schedule_interval=None,
    catchup=False,
)

hook = MySqlHook(mysql_conn_id='mysql_conn')

sql_query = "SELECT player_id FROM pitcher_info;"
player_ids = hook.get_records(sql=sql_query)


def _crawling():
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
        for player_id in player_ids:
            player_id = player_id[0]
            url = f'https://www.koreabaseball.com/Record/Player/PitcherDetail/Basic.aspx?playerId={player_id}'
            driver.get(url)
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            pitcher_info = soup.find('tbody')
            first_line = pitcher_info.find_all("td")

            if first_line[0].text == "기록이 없습니다.":
                continue

            era = float(first_line[1].text)

            if era == "-":
                continue

            sql = """
            INSERT INTO pitcher_info (player_id, ERA) VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
                ERA = VALUES(ERA);
            """
            hook.run(sql, parameters=(player_id, era))

            time.sleep(1)


crawling = PythonOperator(
    task_id='crawling',
    python_callable=_crawling,
    dag=dag
)

trigger_calculate_league_metrics = TriggerDagRunOperator(
    task_id='trigger_calculate_league_metrics',
    trigger_dag_id='calculate_league_metrics',
    execution_date='{{ execution_date }}',
    dag=dag
)

crawling >> trigger_calculate_league_metrics