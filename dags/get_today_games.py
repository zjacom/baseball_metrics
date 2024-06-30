from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
from datetime import timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import pendulum
import pymysql
pymysql.install_as_MySQLdb()

import logging

kst = pendulum.timezone("Asia/Seoul")

start_date = kst.convert(days_ago(1))

dag = DAG(
    dag_id="get_today_games",
    start_date=start_date,
    schedule_interval="0 9 * * *",
    catchup=False,
)
hook = MySqlHook(mysql_conn_id='mysql_conn')

dic = {
    "두산" : "잠실",
    "LG" : "잠실",
    "KIA" : "광주",
    "NC" : "창원",
    "한화" : "대전",
    "SSG" : "문학",
    "삼성" : "대구",
    "키움" : "고척",
    "KT" : "수원",
    "롯데" : "사직",
}


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
        url = 'https://www.koreabaseball.com/Schedule/Schedule.aspx'
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        game_schedules = soup.find_all("tr")
        execution_date = context['execution_date']
        korean_time = execution_date + timedelta(hours=9)
        today = korean_time.strftime('%m-%d')

        logging.info(today)

        flag = False

        for game_schedule in game_schedules:
            day = game_schedule.find("td", {"class" : "day"})
            if day is not None:
                if flag:
                    break
                day = day.text[:5].replace(".", "-")
                if day == today:
                    flag = True
            if flag:
                game_date = korean_time.strftime('%Y-%m-%d')
                game_time = game_schedule.find("td", {"class" : "time"}).text
                temp = game_schedule.find("td", {"class" : "play"}).text.split("vs")
                away, home = temp[0], temp[1]
                logging.info(home)
                get_park_factor_query = f"SELECT value FROM park_factor WHERE stadium = '{dic[home]}'"
                park_factor = hook.get_first(get_park_factor_query)[0]

                insert_query = "INSERT INTO today_games (game_date, game_time, away, home, park_factor) VALUES (%s, %s, %s, %s, %s)"
                hook.run(insert_query, parameters=(game_date, game_time, away, home, park_factor))

drop_table = MySqlOperator(
    task_id='drop_table',
    mysql_conn_id='mysql_conn',
    sql="DROP TABLE IF EXISTS today_games;",
    dag=dag
)

create_table = MySqlOperator(
    task_id='create_table',
    mysql_conn_id='mysql_conn',
    sql="""
        CREATE TABLE IF NOT EXISTS today_games (
            game_date DATE,
            game_time TIME,
            away VARCHAR(16),
            home VARCHAR(16),
            park_factor float,
            away_wRCp float,
            home_wRCp float,
            away_ERAp float,
            home_ERAp float
        )
    """,
    dag=dag
)

crawling = PythonOperator(
    task_id='crawling',
    python_callable=_crawling,
    dag=dag
)

drop_table >> create_table >> crawling