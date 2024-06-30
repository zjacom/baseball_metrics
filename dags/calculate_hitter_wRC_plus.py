# today_games 테이블에서 오늘 날짜 홈, 원정 정보를 가져온다.
# 선수 등록 페이지에서 collect_hitter_id를 참고해서 오늘자 등록된 선수를 파악한다.
# 파악 후 hitter_metrics에 wRC가 있으면 가져오고, 없으면 새로 계산한다.
    # get_hitter_id -> get_hitter_info -> calculate_hitter_wOBA -> calculate_hitter_wRC
# wRC와 선수 정보를 가져오고 파크 팩터를 가져와서 wRC+를 계산하고, 저장한다.

# 주전과 후보의 분류가 필요하다. 어디서 라인업을 가지고와야하나..

from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from bs4 import BeautifulSoup
from collections import defaultdict
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

import logging

kst = pendulum.timezone("Asia/Seoul")

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

dic1 = {
    "HT" : "KIA",
    "OB" : "두산",
    "SS" : "삼성",
    "SK" : "SSG",
    "HH" : "한화",
    "LT" : "롯데",
    "WO" : "키움",
}


link_ids = ["HT", "LG", "OB", "SS", "SK", "NC", "HH", "KT", "LT", "WO"]
today_park_factor = defaultdict(float)

dag = DAG(
    dag_id="calculate_hitter_wRC_plus",
    start_date=datetime(2024, 6, 6, tzinfo=kst),
    schedule_interval=None,
    # schedule_interval="20 4 * * *",
    catchup=False,
)

hook = MySqlHook(mysql_conn_id='mysql_conn')

def _get_today_park_factor(**context):
    execution_date = context['execution_date']
    korean_time = execution_date + timedelta(hours=9)
    today = korean_time.strftime('%Y-%m-%d')

    get_today_games_query = f"SELECT away, home FROM today_games WHERE game_date = '{today}';"
    today_games = hook.get_records(sql=get_today_games_query)

    for away, home in today_games:
        stadium = dic.get(home)
        get_park_factor_query = f"SELECT value FROM park_factor WHERE stadium = '{stadium}'"
        park_factor = hook.get_first(sql=get_park_factor_query)[0]
        logging.info(park_factor)
        today_park_factor[home] = park_factor
        today_park_factor[away] = park_factor


def _crawling():
    get_league_info_query = "SELECT total_pa, total_runs, league_wRC FROM league_info;"
    get_league_info = hook.get_first(sql=get_league_info_query)
    league_pa, league_r, league_wRC = get_league_info[0], get_league_info[1], get_league_info[2]

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

            time.sleep(5)

            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.row'))
            )

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            table = soup.find_all("td")

            for i in table:
                hitter_tag = i.find('a', href=lambda href: href and 'HitterDetail' in href)
                if hitter_tag is None:
                    continue
                # href 속성에서 playerId 추출
                player_id = int(hitter_tag['href'].split('=')[-1])
                # 텍스트에서 이름 추출
                name = hitter_tag.get_text()
                team = dic1.get(link_id, link_id)

                get_wRC_query = f"SELECT wRC FROM hitter_metrics WHERE player_id = {player_id};"
                wRC = hook.get_first(sql=get_wRC_query)
                if wRC is None or wRC[0] is None:
                    continue
                wRC = wRC[0]
                park_factor = today_park_factor[team]
                get_pa_query = f"SELECT at_bat FROM hitter_info WHERE player_id = {player_id};"
                pa = hook.get_first(get_pa_query)[0]
                if pa <= 58:
                    continue
                logging.info(park_factor)
                wRCp = ((wRC / pa) + (league_r / league_pa) * (1 - park_factor)) * league_pa / league_wRC * 100

                insert_query = "INSERT INTO today_wRCp (name, team, wRCp) VALUES(%s, %s, %s);"
                hook.run(insert_query, parameters=(name, team, wRCp))



create_table = MySqlOperator(
    task_id='create_table',
    mysql_conn_id='mysql_conn',
    sql="""CREATE TABLE IF NOT EXISTS today_wRCp(
        name VARCHAR(16),
        team VARCHAR(16),
        wRCp float
        );"""
)

truncate_table = MySqlOperator(
    task_id='truncate_table',
    mysql_conn_id='mysql_conn',
    sql="TRUNCATE today_wRCp;",
    dag=dag
)

get_today_park_factor = PythonOperator(
    task_id='get_today_park_factor',
    python_callable=_get_today_park_factor,
    dag=dag
)

calculate_wRCp = PythonOperator(
    task_id='calculate_wRCp',
    python_callable=_crawling,
    dag=dag
)

truncate_table >> create_table >> get_today_park_factor >> calculate_wRCp