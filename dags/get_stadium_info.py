from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import pendulum
import pymysql
pymysql.install_as_MySQLdb()

import logging
hook = MySqlHook(mysql_conn_id='mysql_conn')

def parse_date(input_date):
    formatted_date = f'2024-{input_date.replace("월 ", "-").replace("일 ", "")}'
    parsed_date = datetime.strptime(formatted_date, '%Y-%m-%d').date()
    return parsed_date


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

kst = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id="get_stadium_info",
    start_date=datetime(2024, 3, 6, tzinfo=kst),
    schedule_interval="50 23 * * *",
    max_active_runs=1,
    depend_on_past=True,
    catchup=True,
)

# game_records DB에서 가장 최신 데이터 가져오는 함수
def _get_recent_date(**context):
    sql_query = "SELECT MAX(game_date) FROM game_records;"
    most_recent_date = hook.get_first(sql_query)[0]
    most_recent_date_str = most_recent_date.strftime('%Y-%m-%d') if most_recent_date else "1997-08-23"
    logging.info(most_recent_date_str)
    
    context['task_instance'].xcom_push(key='get_recent_date', value=most_recent_date_str)


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
        execution_date = context['execution_date']
        date_str = execution_date.strftime('%Y-%m-%d')
        url = f'https://m.sports.naver.com/kbaseball/schedule/index?category=kbo&date={date_str}'
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "ScheduleLeagueType_match_list_container__1v4b0"))
        )
        soup = BeautifulSoup(driver.page_source, 'html.parser')

    most_recent_date = context['task_instance'].xcom_pull(task_ids='get_recent_date', key='get_recent_date')
    most_recent_date = datetime.strptime(most_recent_date, '%Y-%m-%d').date()
    
    score_boards = soup.find_all("div", {"class" : "ScheduleLeagueType_match_list_group__18ML9"})
    
    insert_queries = []

    for score_board in score_boards:
        date = parse_date(score_board.find("em", {"class" : "ScheduleLeagueType_title__2Kalm"}).text[:-3])
        if most_recent_date >= date:
            continue
        games = score_board.find_all("li", {"class" : "MatchBox_match_item__3_D0Q MatchBox_type_baseball__3VkMB type_end"})
        for game in games:
            status = game.find("em", {"class" : "MatchBox_status__2pbzi"}).text
            scores = game.find_all("strong", {"class" : "MatchBoxTeamArea_score__1_YFB"})
            away_score, home_score = int(scores[0].text), int(scores[1].text)
            stadium = game.find_all("strong", {"class" : "MatchBoxTeamArea_team__3aB4O"})[1].text
            
            if status == "종료":
                # 삽입할 데이터
                stadium_location = dic.get(stadium)
                
                insert_query = f"""
                INSERT INTO game_records (game_date, away_score, home_score, stadium)
                VALUES ('{date}', {away_score}, {home_score}, '{stadium_location}');
                """

                insert_queries.append(insert_query)
    context['task_instance'].xcom_push(key='insert_queries', value=insert_queries)


def _insert_data(**context):
    task_instance = context['task_instance']
    insert_queries = task_instance.xcom_pull(task_ids='crawling', key='insert_queries')

    connection = hook.get_conn()
    cursor = connection.cursor()

    for query in insert_queries:
        cursor.execute(query)

    connection.commit()
    cursor.close()
    connection.close()


create_table = MySqlOperator(
    task_id='create_table',
    sql="""
        CREATE TABLE IF NOT EXISTS game_records (
            game_date DATE,
            away_score INT,
            home_score INT,
            stadium VARCHAR(16)
        );
    """,
    mysql_conn_id='mysql_conn',
    dag=dag,
)

get_recent_date = PythonOperator(
    task_id='get_recent_date',
    python_callable=_get_recent_date,
    provide_context=True, 
    dag=dag,
)

crawling = PythonOperator(
    task_id="crawling", 
    python_callable=_crawling,
    provide_context=True, 
    dag=dag
)

insert_data = PythonOperator(
    task_id='insert_data',
    python_callable=_insert_data,
    provide_context=True,
    dag=dag
)

create_table >> get_recent_date >> crawling >> insert_data 