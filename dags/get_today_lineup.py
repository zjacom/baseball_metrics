from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

import pendulum
import pymysql
import time
pymysql.install_as_MySQLdb()
import logging
kst = pendulum.timezone("Asia/Seoul")
from airflow.utils.dates import days_ago
start_date = kst.convert(days_ago(1))

dag = DAG(
    dag_id="get_today_lineup",
    start_date=start_date,
    schedule_interval=None,
    catchup=False,
)

hook = MySqlHook(mysql_conn_id='mysql_conn')


def _crawling(**context):
    execution_date = context['execution_date']
    korean_time = execution_date + timedelta(hours=9)
    today = korean_time.strftime('%Y-%m-%d')

    hook = MySqlHook(mysql_conn_id='mysql_conn')
    get_league_info_query = "SELECT total_pa, total_runs, league_wRC FROM league_info;"
    get_league_info = hook.get_first(sql=get_league_info_query)
    league_pa, league_r, league_wRC = get_league_info[0], get_league_info[1], get_league_info[2]
    get_league_ERA_query = "SELECT league_ERA FROM league_info;"
    league_ERA = hook.get_first(get_league_ERA_query)[0]

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
        url = 'https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx'
        driver.get(url)
        li_elements = driver.find_elements(By.CSS_SELECTOR, 'ul.game-list-n > li')

        for li in li_elements:
            if li.find("p", {"class" : "staus"}).text == "경기취소":
                continue
            # 게임 센터에서 5개의 경기를 하나씩 클릭
            li.click()
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            time.sleep(3)  # 추가 대기 시간을 설정하여 페이지가 완전히 로드되도록 함
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            pitchers = soup.find_all("span", {"class" : "name"})
            logging.info(f"Pitchers found: {pitchers}")
            away_pitcher, home_pitcher = pitchers[0].text, pitchers[1].text

            # 라인업 분석 버튼 클릭
            tabs = driver.find_elements(By.CSS_SELECTOR, 'ul.tab > li')
            # 만약 금일 DH1 경기 후 DH2 선발 정보가 나와서 라인업 분석 버튼이 2번 째에 있다면 continue
            if len(tabs) < 3:
                continue
            lineup_button = tabs[3]
            lineup_button.click()

            soup1 = BeautifulSoup(driver.page_source, 'html.parser')
            # 금일 라인업 등록 전이라면 continue
            register_flag = soup1.find("p", {"class" : "section-left fb f16"}).text
            if register_flag == "라인업 발표 전으로 최근 라인업 기준입니다.":
                continue
            
            # 원정팀 라인업 정보
            away = soup1.find("h6", {"id" : "txtAwayTeam"}).text
            get_park_factor_query = f"SELECT park_factor FROM today_games WHERE away='{away}' AND game_date='{today}';"
            park_factor = hook.get_first(get_park_factor_query)[0]
            get_away_pitcher_id_query = f"SELECT player_id FROM pitcher_info WHERE name = '{away_pitcher}' AND team = '{away}'"
            away_pitcher_id = hook.get_first(get_away_pitcher_id_query)[0]
            away_lineup = soup1.find("table", {"id" : "tblAwayLineUp"}).find("tbody").find_all("tr")
            for player in away_lineup:
                info = player.find_all("td")
                num, position, name = info[0].text, info[1].text, info[2].text
                get_away_hitter_id_query = f"SELECT player_id FROM hitter WHERE name = '{name}' AND team = '{away}'"
                away_hitter_id = hook.get_first(get_away_hitter_id_query)[0]
                get_wRC_query = f"SELECT wRC FROM hitter_metrics WHERE player_id = {away_hitter_id};"
                wRC = hook.get_first(sql=get_wRC_query)
                if wRC is None or wRC[0] is None:
                    continue
                wRC = wRC[0]
                get_pa_query = f"SELECT at_bat FROM hitter_info WHERE player_id = {away_hitter_id};"
                pa = hook.get_first(get_pa_query)[0]
                if pa <= 58:
                    continue
                metric = ((wRC / pa) + (league_r / league_pa) * (1 - park_factor)) * league_pa / league_wRC * 100
                insert_query = "INSERT INTO today_lineup (team, player, position, batting_order, metric) VALUES (%s, %s, %s, %s, %s)"
                hook.run(insert_query, parameters=(away, name, position, num, metric))
            get_pitcher_ERA_query = f"SELECT ERA FROM pitcher_info WHERE player_id = {away_pitcher_id};"
            era = hook.get_first(get_pitcher_ERA_query)[0]
            if not era:
                continue
            metric = league_ERA / era * park_factor * 100
            insert_pitcher_query = "INSERT INTO today_lineup (team, player, position, batting_order, metric) VALUES (%s, %s, %s, %s, %s)"
            hook.run(insert_pitcher_query, parameters=(away, away_pitcher, "선발 투수", 0, metric))


            home = soup1.find("h6", {"id" : "txtHomeTeam"}).text
            get_home_pitcher_id_query = f"SELECT player_id FROM pitcher_info WHERE name = '{home_pitcher}' AND team = '{home}'"
            home_pitcher_id = hook.get_first(get_home_pitcher_id_query)[0]
            home_lineup = soup1.find("table", {"id" : "tblHomeLineUp"}).find("tbody").find_all("tr")
            for player in home_lineup:
                info = player.find_all("td")
                num, position, name = info[0].text, info[1].text, info[2].text
                get_home_hitter_id_query = f"SELECT player_id FROM hitter WHERE name = '{name}' AND team = '{home}'"
                home_hitter_id = hook.get_first(get_home_hitter_id_query)[0]
                get_wRC_query = f"SELECT wRC FROM hitter_metrics WHERE player_id = {home_hitter_id};"
                wRC = hook.get_first(sql=get_wRC_query)
                if wRC is None or wRC[0] is None:
                    continue
                wRC = wRC[0]
                get_pa_query = f"SELECT at_bat FROM hitter_info WHERE player_id = {home_hitter_id};"
                pa = hook.get_first(get_pa_query)[0]
                if pa <= 58:
                    continue
                metric = ((wRC / pa) + (league_r / league_pa) * (1 - park_factor)) * league_pa / league_wRC * 100
                insert_query = "INSERT INTO today_lineup (team, player, position, batting_order, metric) VALUES (%s, %s, %s, %s, %s)"
                hook.run(insert_query, parameters=(home, name, position, num, metric))
            get_pitcher_ERA_query = f"SELECT ERA FROM pitcher_info WHERE player_id = '{home_pitcher_id}';"
            era = hook.get_first(get_pitcher_ERA_query)[0]
            if not era:
                continue
            metric = league_ERA / era * park_factor * 100
            insert_pitcher_query = "INSERT INTO today_lineup (team, player, position, batting_order, metric) VALUES (%s, %s, %s, %s, %s)"
            hook.run(insert_pitcher_query, parameters=(home, home_pitcher, "선발 투수", 0, metric))


drop_table = MySqlOperator(
    task_id='drop_table',
    mysql_conn_id='mysql_conn',
    sql="DROP TABLE IF EXISTS today_lineup;",
    dag=dag
)

create_table = MySqlOperator(
    task_id='create_table',
    mysql_conn_id='mysql_conn',
    sql="""
        CREATE TABLE IF NOT EXISTS today_lineup (
            team varchar(16),
            player varchar(16),
            position varchar(32),
            batting_order int,
            metric float
        );""",
    dag=dag
)

crawling = PythonOperator(
    task_id='crawling',
    python_callable=_crawling,
    dag=dag
)

trigger_my_calculus = TriggerDagRunOperator(
    task_id='trigger_my_calculus',
    trigger_dag_id='my_calculus',
    dag=dag
)

drop_table >> create_table >> crawling >> trigger_my_calculus