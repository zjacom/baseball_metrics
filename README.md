# 프로젝트 소개

이 프로젝트는 야구 선수의 가치를 판단하는데 도움이 되는 ***세이버메트릭스***를 사용해 KBO 경기의 결과를 예측하는 프로젝트입니다.

## 지표 선택
* 타자 지표 : wRC+
    * [wRC+ 계산법](https://library.fangraphs.com/offense/wrc/)을 참고하여 계산했습니다.
* 투수 지표 : ERA+
    * [ERA+ 계산법](https://namu.wiki/w/평균자책점)을 참고하여 계산했습니다.

## 아키텍처 구조
<img width="855" alt="스크린샷 2024-07-04 오후 1 17 31" src="https://github.com/zjacom/baseball_metrics/assets/112957047/6eb3d6d4-47c4-4dfb-bb6a-5967e0572d12">

# DAG 문서화
![untitled](https://github.com/zjacom/baseball_metrics/assets/112957047/671a83b8-6d24-4041-ac8b-78bd4a17ca63)

* ***실선***은 `Trigger` 오퍼레이터를 사용했음을 나타냅니다.
* ***점선***은 업스트림 DAG에 의해 다운스트림 DAG의 실행시간이 동적으로 정해짐을 나타냅니다.

## DAG 역할 설명
- `get_stadium_info` : 파크 팩터를 계산하기 위해 매일 구장별 득실점 기록을 수집합니다.
- `calculate_park_factor` : 파크 팩터를 계산합니다.
- `get_league_info` : KBO 리그 정보를 수집합니다.
- `get_today_games` : 금일 예정된 경기 정보를 수집합니다.
- `schedule_collect_hitter_id` : `collect_hitter_id`의 실행 시간을 스케줄링합니다.
- `schedule_collect_pitcher_id` : `collect_pitcher_id`의 실행 시간을 스케줄링합니다.
- `collect_hitter_id` : 금일 등록된 선수 명단을 바탕으로 타자의 세부 정보를 조회할 수 있는 ID 값을 수집합니다.
- `collect_pitcher_id` : 금일 등록된 선수 명단을 바탕으로 투수의 세부 정보를 조회할 수 있는 ID 값을 수집합니다.
- `get_hitter_info` : 수집한 타자의 ID 값으로 타자의 세부 정보를 수집합니다.
- `get_pitcher_info` : 수집한 투수의 ID 값으로 투수의 세부 정보를 수집합니다.
- `calculate_hitter_wOBA` : 타자의 wOBA를 계산합니다.
- `calculate_hitter_wRC` : 타자의 wRC를 계산합니다.
- `calculate_league_metrics` : 리그 평균 wRC와 ERA를 계산합니다.
- `get_today_lineup` : 금일 진행되는 경기 중 각 팀의 라인업 정보를 수집합니다.
- `my_calculus` : 지금까지 수집한 데이터를 활용해 최종 지표인 wRC+, ERA+를 계산합니다.
- `send_slack_alarm` : 도출된 wRC+와 ERA+를 참고하여 어떤 팀이 승리할 것인지 예측하고 슬랙으로 메시지를 보냅니다.