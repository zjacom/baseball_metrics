from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import pendulum
import pymysql
import time
pymysql.install_as_MySQLdb()

kst = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id="get_hitter_info",
    start_date= datetime(2024, 6, 6, tzinfo=kst),
    schedule_interval=None
)

hook = MySqlHook(mysql_conn_id='mysql_conn')

sql_query = "SELECT player_id FROM hitter;"
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
            url = f'https://www.koreabaseball.com/Record/Player/HitterDetail/Basic.aspx?playerId={player_id}'
            driver.get(url)
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            hitter_info = soup.find_all('tbody')
            first_line = hitter_info[0].find_all("td")
            second_line = hitter_info[1].find_all("td")

            if first_line[0].text == "기록이 없습니다.":
                continue

            team, at_bat = first_line[0].text.strip(), int(first_line[3].text) # 구단, 타석
            if at_bat == 0:
                continue
            single, double_hit, triple, homerun = int(first_line[6].text), int(first_line[7].text), int(first_line[8].text), int(first_line[9].text) # 안타, 2루타, 3루타, 홈런
            steal_base, fail_steal_base, sacrifice_bunt = int(first_line[12].text), int(first_line[13].text), int(first_line[14].text) # 도루 성공, 도루 실패, 희생 번트
            walk, intentional_walk, hbp, on_base_percentage, = int(second_line[0].text), int(second_line[1].text), int(second_line[2].text), float(second_line[6].text) # 볼넷, 고의 사구, 출루율

            sql = """
            INSERT INTO hitter_info (
                player_id, team, at_bat, single, double_hit, triple, homerun, steal_base, fail_steal_base, sacrifice_bunt, walk, intentional_walk, hbp, on_base_percentage
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                team = VALUES(team),
                at_bat = VALUES(at_bat),
                single = VALUES(single),
                double_hit = VALUES(double_hit),
                triple = VALUES(triple),
                homerun = VALUES(homerun),
                steal_base = VALUES(steal_base),
                fail_steal_base = VALUES(fail_steal_base),
                sacrifice_bunt = VALUES(sacrifice_bunt),
                walk = VALUES(walk),
                intentional_walk = VALUES(intentional_walk),
                hbp = VALUES(hbp),
                on_base_percentage = VALUES(on_base_percentage);
            """
            hook.run(sql, parameters=(player_id, team, at_bat, single, double_hit, triple, homerun, steal_base, fail_steal_base, sacrifice_bunt, walk, intentional_walk, hbp, on_base_percentage))

            time.sleep(1)


create_table = MySqlOperator(
    task_id='create_table',
    mysql_conn_id='mysql_conn',
    sql="""
        CREATE TABLE IF NOT EXISTS hitter_info (
            player_id int,
            team varchar(8),
            at_bat int,
            single int,
            double_hit int,
            triple int,
            homerun int,
            steal_base int,
            fail_steal_base int,
            sacrifice_bunt int,
            walk int,
            intentional_walk int,
            hbp int,
            on_base_percentage float,
            primary key (player_id)
        );
    """,
    dag=dag
)

crawling = PythonOperator(
    task_id='crawling',
    python_callable=_crawling,
    dag=dag
)

trigger_calculate_hitter_wOBA = TriggerDagRunOperator(
    task_id='trigger_calculate_hitter_wOBA',
    trigger_dag_id='calculate_hitter_wOBA',
    dag=dag
)

create_table >> crawling >> trigger_calculate_hitter_wOBA