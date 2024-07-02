from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import DagRun
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta, time
from airflow.api.common.experimental.trigger_dag import trigger_dag

import pendulum
import logging
import pymysql
pymysql.install_as_MySQLdb()

kst = pendulum.timezone("Asia/Seoul")

start_date = kst.convert(days_ago(1))
# 스케줄러 DAG
scheduler_dag = DAG(
    dag_id='schedule_collect_hitter_id',
    schedule_interval='30 9 * * *',
    start_date=start_date,
)

def convert_timedelta_to_time(delta):
    """
    Convert a timedelta object to a time object.
    """
    seconds = delta.total_seconds()
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    return datetime.time(hours, minutes, seconds)


def schedule_dynamic_dag():
    # MySQL에서 시간 데이터 읽기
    hook = MySqlHook(mysql_conn_id='mysql_conn')
    query = "SELECT DISTINCT game_time FROM today_games WHERE game_date = CURDATE();"
    result = hook.get_records(query)

    if result:
        for game_time in result:
            exec_time = (datetime.combine(datetime.today(), convert_timedelta_to_time(game_time[0])) - timedelta(minutes=30))
            schedule_dag_run('collect_hitter_id', exec_time)


def schedule_dag_run(dag_id, execution_time):
    # DAG 실행이 이미 존재하는지 확인
    existing_dag_run = DagRun.find(dag_id=dag_id, execution_date=execution_time)
    if existing_dag_run:
        logging.info(f"{dag_id}가 이미 {execution_time}에 예약되어 있습니다.")
        return

    logging.info(f"{dag_id}를(을) {execution_time}에 예약했습니다.")
    trigger_dag(dag_id=dag_id, run_id=f'scheduled__{execution_time.isoformat()}', execution_date=execution_time, replace_microseconds=False)


schedule_collect_hitter_id_dag = PythonOperator(
    task_id='schedule_collect_hitter_id_dag',
    python_callable=schedule_dynamic_dag,
    provide_context=True,
    dag=scheduler_dag,
)

from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import pendulum
import pymysql
import time
pymysql.install_as_MySQLdb()

kst = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id="collect_hitter_id",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)

link_ids = ["HT", "LG", "OB", "SS", "SK", "NC", "HH", "KT", "LT", "WO"]
dic = {
    "HT" : "KIA",
    "OB" : "두산",
    "SS" : "삼성",
    "SK" : "SSG",
    "HH" : "한화",
    "LT" : "롯데",
    "WO" : "키움",
}


def _crawling(**context):
    execution_date = context['execution_date']
    korean_time = execution_date + timedelta(hours=9)
    today = korean_time.strftime('%Y-%m-%d')
    hook = MySqlHook(mysql_conn_id='mysql_conn')

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
        url = 'https://www.koreabaseball.com/Player/Register.aspx'
        driver.get(url)

        for link_id in link_ids:
            # 각 링크를 직접 클릭
            link = driver.find_element(By.CSS_SELECTOR, f'div.teams ul li[data-id="{link_id}"] a')
            link.click()

            time.sleep(1)

            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.row'))
            )

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            date_text = soup.find("span", {"class" : "date-txt"}).text
            is_today_lineup = date_text[:10].replace(".", "-")
            if today != is_today_lineup:
                continue
            table = soup.find_all("td")

            for i in table:
                hitter_tag = i.find('a', href=lambda href: href and 'HitterDetail' in href)
                if hitter_tag is None:
                    continue
                # href 속성에서 playerId 추출
                player_id = int(hitter_tag['href'].split('=')[-1])
                # 텍스트에서 이름 추출
                name = hitter_tag.get_text()
                team = dic.get(link_id, link_id)

                sql = "INSERT IGNORE INTO hitter (name, team, player_id) VALUES (%s, %s, %s)"
                hook.run(sql, parameters=(name, team, player_id))

                
create_table = MySqlOperator(
    task_id='create_table',
    sql="""
        CREATE TABLE IF NOT EXISTS hitter (
            name varchar(32),
            team varchar(8),
            player_id int,
            primary key (player_id)
        );
    """,
    mysql_conn_id='mysql_conn',
    dag=dag,
)

crawling = PythonOperator(
    task_id='crawling',
    python_callable=_crawling,
    dag=dag
)

trigger_get_hitter_info = TriggerDagRunOperator(
    task_id='trigger_get_hitter_info',
    trigger_dag_id='get_hitter_info',
    execution_date='{{ execution_date }}',
    dag=dag
)

create_table >> crawling >> trigger_get_hitter_info