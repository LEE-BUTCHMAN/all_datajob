import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import time


def calculate_signup_ratio_and_update():
    """첫구매이벤트+회원가입 비중을 계산하고 구글시트 AF열에 업데이트"""

    # 1. 비중 계산 쿼리 (첫구매이벤트 + 회원가입 합계)
    signup_ratio_query = """
                         with charge_detail \
                                  as (select year (substr(created_at, 1, 10)) as reward_year, week(substr(created_at, 1, 10), 1) as reward_week, type, reason, sum (amount) as amount
                         from (select CASE pnt.event_type
                             WHEN 'REWARD' THEN '충전'
                             WHEN 'USED' THEN '사용'
                             WHEN 'EXPIRED' THEN '만료'
                             WHEN 'ADMIN_SUBTRACT' THEN '차감'
                             ELSE ''
                             END AS "type", CASE pnt.reason_type
                             WHEN 'ORDER' THEN '주문'
                             WHEN 'NONE' THEN '해당사항없음'
                             WHEN 'ORDER_CANCEL' THEN '주문취소'
                             WHEN 'DY' THEN '계근환급'
                             WHEN 'EVENT' THEN '이벤트'
                             WHEN 'JOINED' THEN '회원가입'
                             WHEN 'MAKE_UP' THEN '임의조정'
                             WHEN 'EXPIRED' THEN '만료'
                             WHEN 'FIRST_ORDER_EVENT' THEN '첫구매이벤트'
                             WHEN 'REFERRAL_MEMBER' THEN '친구추천(추천인)'
                             WHEN 'FIRST_ORDER_REFERRED' THEN '친구추천(신규회원)'
                             WHEN 'SHIPMENT_DATE_OPTIONS_EVENT' THEN '반짝적립'
                             ELSE ''
                             END AS "reason", pnt.amount "amount", pnt.created_at "created_at"
                             from cancun.point pnt
                             inner join cancun.base_user bu on bu.id = pnt.user_id
                             inner join cancun.user u on u.base_user_id = bu.id
                             where substr(pnt.created_at, 1, 10) between '2025-07-14' and '2025-07-20'
                             order by pnt.created_at desc
                             ) A
                         where A.type='충전'
                         group by 1, 2, 3, 4
                             ),
                             week_total as (
                         select reward_year, reward_week, sum (amount) as total_week_amount
                         from charge_detail
                         group by reward_year, reward_week
                             ),
                             signup_events as (
                         select cd.reward_year, cd.reward_week, sum (cd.amount) as signup_amount -- 첫구매이벤트 + 회원가입 합계
                         from charge_detail cd
                         where cd.reason in ('회원가입', '첫구매이벤트')
                         group by cd.reward_year, cd.reward_week
                             )
                         select se.reward_year || '년 ' || se.reward_week || '주차'          as week_info,
                                se.reward_year,
                                se.reward_week,
                                se.signup_amount,
                                wt.total_week_amount,
                                round(se.signup_amount * 100.0 / wt.total_week_amount, 2) as signup_ratio_percent
                         from signup_events se
                                  inner join week_total wt on se.reward_year = wt.reward_year
                             and se.reward_week = wt.reward_week
                         order by se.reward_year, se.reward_week; \
                         """

    print("🔍 첫구매이벤트+회원가입 비중 쿼리:")
    print(signup_ratio_query)

    # TODO: 여기서 실제 DB 쿼리 실행해서 결과를 받아야 함
    # 예시 결과 (실제로는 DB에서 가져온 데이터)
    query_results = [
        {'week_info': '2025년 29주차', 'reward_year': 2025, 'reward_week': 29,
         'signup_amount': 7010000, 'total_week_amount': 7510843, 'signup_ratio_percent': 93.33}
    ]

    print("✅ 쿼리 결과:")
    for result in query_results:
        print(f"  {result['week_info']}: {result['signup_ratio_percent']}%")

    # 2. 구글 시트 업데이트
    update_google_sheet_af_column(query_results)


def update_google_sheet_af_column(ratio_results):
    """구글 시트의 AF열에 첫구매이벤트+회원가입 비중 업데이트"""

    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    try:
        # 구글 시트 열기
        sheet_id = '1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE'
        spreadsheet = client.open_by_key(sheet_id)
        worksheet = spreadsheet.get_worksheet(0)  # 첫 번째 워크시트

        # B열의 모든 값 읽기 (주차 정보가 있는 열)
        b_column_values = worksheet.col_values(2)  # B열 = 2번째 열

        print(f"🔍 B열 주차 정보 확인:")
        for i, val in enumerate(b_column_values[:10]):  # 처음 10개만 출력
            if val.strip():
                print(f"  {i + 1}행: '{val}'")

        # 각 쿼리 결과에 대해 매칭하여 업데이트
        for result in ratio_results:
            week_info = result['week_info']  # "2025년 29주차"
            ratio_percent = result['signup_ratio_percent']  # 93.33

            # 주차 정보에서 숫자만 추출 (예: "29주차")
            if '년 ' in week_info and '주차' in week_info:
                week_part = week_info.split('년 ')[1]  # "29주차"
                week_num = week_part.replace('주차', '')  # "29"

                print(f"🔍 찾는 주차: '{week_info}' → 숫자: '{week_num}'")

                # B열에서 해당 주차를 찾기
                target_row = None
                for i, cell_value in enumerate(b_column_values):
                    if cell_value and str(week_num) in str(cell_value):
                        print(f"✅ 매칭 발견: {i + 1}행 '{cell_value}'에서 '{week_num}' 찾음")
                        target_row = i + 1  # gspread는 1부터 시작
                        break

                if target_row:
                    print(f"📍 {week_info} 비중({ratio_percent}%)을 {target_row}행 AF열에 업데이트 중...")

                    # AF열에 비중 값 업데이트
                    worksheet.update(f'AF{target_row}', [[f"{ratio_percent}%"]])
                    time.sleep(0.5)  # API 제한 방지

                    print(f"✅ AF{target_row}: {ratio_percent}% 업데이트 완료")
                else:
                    print(f"❌ {week_info}에 해당하는 행을 B열에서 찾을 수 없습니다.")

        print("🎉 AF열 업데이트 완료!")

    except Exception as e:
        print(f"❌ 구글 시트 업데이트 오류: {e}")


# 실제 DB 연결이 있는 환경에서 사용할 함수
def execute_query_and_update_sheet(db_connection):
    """실제 DB에서 쿼리 실행하고 시트 업데이트"""

    # 비중 계산 쿼리
    signup_ratio_query = """
                         with charge_detail \
                                  as (select year (substr(created_at, 1, 10)) as reward_year, week(substr(created_at, 1, 10), 1) as reward_week, type, reason, sum (amount) as amount
                         from (select CASE pnt.event_type
                             WHEN 'REWARD' THEN '충전'
                             WHEN 'USED' THEN '사용'
                             WHEN 'EXPIRED' THEN '만료'
                             WHEN 'ADMIN_SUBTRACT' THEN '차감'
                             ELSE ''
                             END AS "type", CASE pnt.reason_type
                             WHEN 'ORDER' THEN '주문'
                             WHEN 'NONE' THEN '해당사항없음'
                             WHEN 'ORDER_CANCEL' THEN '주문취소'
                             WHEN 'DY' THEN '계근환급'
                             WHEN 'EVENT' THEN '이벤트'
                             WHEN 'JOINED' THEN '회원가입'
                             WHEN 'MAKE_UP' THEN '임의조정'
                             WHEN 'EXPIRED' THEN '만료'
                             WHEN 'FIRST_ORDER_EVENT' THEN '첫구매이벤트'
                             WHEN 'REFERRAL_MEMBER' THEN '친구추천(추천인)'
                             WHEN 'FIRST_ORDER_REFERRED' THEN '친구추천(신규회원)'
                             WHEN 'SHIPMENT_DATE_OPTIONS_EVENT' THEN '반짝적립'
                             ELSE ''
                             END AS "reason", pnt.amount "amount", pnt.created_at "created_at"
                             from cancun.point pnt
                             inner join cancun.base_user bu on bu.id = pnt.user_id
                             inner join cancun.user u on u.base_user_id = bu.id
                             where substr(pnt.created_at, 1, 10) between '2025-07-14' and '2025-07-20'
                             ) A
                         where A.type='충전'
                         group by 1, 2, 3, 4
                             ),
                             week_total as (
                         select reward_year, reward_week, sum (amount) as total_week_amount
                         from charge_detail
                         group by reward_year, reward_week
                             ),
                             signup_events as (
                         select cd.reward_year, cd.reward_week, sum (cd.amount) as signup_amount
                         from charge_detail cd
                         where cd.reason in ('회원가입', '첫구매이벤트')
                         group by cd.reward_year, cd.reward_week
                             )
                         select se.reward_year || '년 ' || se.reward_week || '주차'          as week_info,
                                se.reward_year,
                                se.reward_week,
                                se.signup_amount,
                                wt.total_week_amount,
                                round(se.signup_amount * 100.0 / wt.total_week_amount, 2) as signup_ratio_percent
                         from signup_events se
                                  inner join week_total wt on se.reward_year = wt.reward_year
                             and se.reward_week = wt.reward_week
                         order by se.reward_year, se.reward_week; \
                         """

    # DB 쿼리 실행
    cursor = db_connection.cursor()
    cursor.execute(signup_ratio_query)
    results = cursor.fetchall()

    # 결과를 딕셔너리 리스트로 변환
    query_results = []
    for row in results:
        query_results.append({
            'week_info': row[0],
            'reward_year': row[1],
            'reward_week': row[2],
            'signup_amount': row[3],
            'total_week_amount': row[4],
            'signup_ratio_percent': row[5]
        })

    # 구글 시트 업데이트
    update_google_sheet_af_column(query_results)


if __name__ == "__main__":
    # 테스트 실행
    calculate_signup_ratio_and_update()

    print("\n=== 실제 사용 예시 ===")
    print("# DB 연결이 있는 경우:")
    print("# execute_query_and_update_sheet(your_db_connection)")