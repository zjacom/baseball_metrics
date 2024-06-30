from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from bs4 import BeautifulSoup
from datetime import datetime
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

dag = DAG(
    dag_id="calculate_hitter_wOBA",
    start_date=datetime(2024, 6, 6, tzinfo=kst),
    schedule_interval=None,
    catchup=False,
)

link_ids = ["HT", "LG", "OB", "SS", "SK", "NC", "HH", "KT", "LT", "WO"]


def _crawling():
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

                sql_query = f"SELECT * FROM hitter_info WHERE player_id = {player_id}"
                player_details = hook.get_first(sql_query) # (53327, '키움', 284, 92, 21, 2, 9, 2, 2, 0, 24, 0, 0, 0.415)
                if player_details is None:
                    continue
                at_bat, single, double_hit, triple, homerun, steal_base, fail_steal_base, sacrifice_bunt, walk, intentional_walk, hbp = player_details[2], player_details[3], player_details[4], player_details[5], player_details[6], player_details[7], player_details[8], player_details[9], player_details[10], player_details[11], player_details[12]
                wOBA = (0.7 * (walk - intentional_walk - hbp) + 0.9 * (single) + 1.25 * (double_hit) + 1.6 * (triple) + 2 * (homerun) + 0.25 * (steal_base) - 0.5 * (fail_steal_base)) / (at_bat - intentional_walk - sacrifice_bunt)
                logging.info(wOBA)
                insert_query = """
                    INSERT INTO hitter_metrics (player_id, wOBA)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE
                    wOBA = VALUES(wOBA);
                """
                hook.run(insert_query, parameters=(player_id, wOBA))


create_table = MySqlOperator(
    task_id='create_table',
    mysql_conn_id='mysql_conn',
    sql="""CREATE TABLE IF NOT EXISTS hitter_metrics (
        player_id int,
        wOBA float,
        wRC float,
        primary key (player_id)
    );""",
    dag=dag
)

crawling = PythonOperator(
    task_id='crawling',
    python_callable=_crawling,
    dag=dag
)

trigger_calculate_hitter_wRC = TriggerDagRunOperator(
    task_id='trigger_calculate_hitter_wRC',
    trigger_dag_id='calculate_hitter_wRC',
    dag=dag
)

create_table >> crawling >> trigger_calculate_hitter_wRC