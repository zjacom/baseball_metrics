# from airflow import DAG
# from airflow.operators.mysql_operator import MySqlOperator
# from airflow.operators.python import PythonOperator
# from airflow.providers.mysql.hooks.mysql import MySqlHook
# from bs4 import BeautifulSoup
# from datetime import datetime, timedelta
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC

# import pendulum
# import pymysql
# import time
# pymysql.install_as_MySQLdb()

# kst = pendulum.timezone("Asia/Seoul")

# dag = DAG(
#     dag_id="get_today_register",
#     start_date=datetime(2024, 6, 6, tzinfo=kst),
#     schedule_interval=None,
#     # schedule_interval="20 3 * * *",
#     catchup=False,
# )

# link_ids = ["HT", "LG", "OB", "SS", "SK", "NC", "HH", "KT", "LT", "WO"]
# dic = {
#     "HT" : "KIA",
#     "OB" : "두산",
#     "SS" : "삼성",
#     "SK" : "SSG",
#     "HH" : "한화",
#     "LT" : "롯데",
#     "WO" : "키움",
# }


# def _crawling(**context):
#     execution_date = context['execution_date']
#     korean_time = execution_date + timedelta(hours=9)
#     today = korean_time.strftime('%Y-%m-%d')

#     hook = MySqlHook(mysql_conn_id='mysql_conn')
#     get_league_info_query = "SELECT total_pa, total_runs, league_wRC FROM league_info;"
#     get_league_info = hook.get_first(sql=get_league_info_query)
#     league_pa, league_r, league_wRC = get_league_info[0], get_league_info[1], get_league_info[2]
#     get_league_ERA_query = "SELECT league_ERA FROM league_info;"
#     league_ERA = hook.get_first(get_league_ERA_query)[0]

#     chrome_options = Options()
#     chrome_options.add_argument('--ignore-ssl-errors=yes')
#     chrome_options.add_argument('--ignore-certificate-errors')

#     user_agent = 'userMozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
#     chrome_options.add_argument(f'user-agent={user_agent}')
#     chrome_options.add_argument('--disable-dev-shm-usage')
#     chrome_options.add_argument('--no-sandbox')
#     chrome_options.add_argument('--headless')

#     remote_webdriver = 'remote_chromedriver'
#     with webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=chrome_options) as driver:
#         url = 'https://www.koreabaseball.com/Player/Register.aspx'
#         driver.get(url)

#         for link_id in link_ids:
#             # 각 링크를 직접 클릭
#             link = driver.find_element(By.CSS_SELECTOR, f'div.teams ul li[data-id="{link_id}"] a')
#             link.click()

#             time.sleep(5)

#             WebDriverWait(driver, 10).until(
#                 EC.presence_of_element_located((By.CSS_SELECTOR, 'div.row'))
#             )

#             soup = BeautifulSoup(driver.page_source, 'html.parser')
#             table = soup.find_all("td")

#             team = dic.get(link_id, link_id)

#             for i in table:
#                 hitter_tag = i.find('a', href=lambda href: href and 'HitterDetail' in href)
#                 pitcher_tag = i.find('a', href=lambda href: href and 'PitcherDetail' in href)
#                 if hitter_tag is None and pitcher_tag is None:
#                     continue
#                 get_park_factor_query = f"SELECT park_factor FROM today_games WHERE away='{team}' OR home='{team}' AND game_date='{today}';"
#                 park_factor = hook.get_first(get_park_factor_query)[0]
#                 # href 속성에서 playerId 추출
#                 if hitter_tag is not None:
#                     player_id = int(hitter_tag['href'].split('=')[-1])
#                     name = hitter_tag.get_text()
#                     position = "타자"
#                     get_wRC_query = f"SELECT wRC FROM hitter_metrics WHERE player_id = {player_id};"
#                     wRC = hook.get_first(sql=get_wRC_query)
#                     if wRC is None or wRC[0] is None:
#                         continue
#                     wRC = wRC[0]
#                     get_pa_query = f"SELECT at_bat FROM hitter_info WHERE player_id = {player_id};"
#                     pa = hook.get_first(get_pa_query)[0]
#                     if pa <= 58:
#                         continue
#                     metric = ((wRC / pa) + (league_r / league_pa) * (1 - park_factor)) * league_pa / league_wRC * 100
#                 else:
#                     player_id = int(pitcher_tag['href'].split('=')[-1])
#                     name = pitcher_tag.get_text()
#                     position = "투수"
#                     get_pitcher_ERA_query = f"SELECT ERA FROM pitcher_info WHERE player_id = '{player_id}';"
#                     era = hook.get_first(get_pitcher_ERA_query)[0]
#                     if not era:
#                         continue
#                     metric = league_ERA / era * park_factor * 100

#                 sql = "INSERT IGNORE INTO today_register (player_id, name, team, position, metric) VALUES (%s, %s, %s, %s, %s)"
#                 hook.run(sql, parameters=(player_id, name, team, position, metric))
                
# drop_table = MySqlOperator(
#     task_id='drop_table',
#     mysql_conn_id='mysql_conn',
#     sql="DROP TABLE IF EXISTS today_register;",
#     dag=dag
# )

# create_table = MySqlOperator(
#     task_id='create_table',
#     sql="""
#         CREATE TABLE IF NOT EXISTS today_register (
#             name varchar(32),
#             team varchar(8),
#             player_id int,
#             position varchar(16),
#             metric FLOAT,
#             primary key (player_id)
#         );
#     """,
#     mysql_conn_id='mysql_conn',
#     dag=dag,
# )

# crawling = PythonOperator(
#     task_id='crawling',
#     python_callable=_crawling,
#     dag=dag
# )

# drop_table >> create_table >> crawling
