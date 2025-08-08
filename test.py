import pymysql
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import time


def weekly_update():
    """주차별 업데이트"""
    print("=" * 60)
    print("주차별 업데이트 시작 (2~32주차)")
    print("(1주차는 연도 변경 구간이라 제외)")
    print("=" * 60)

    # DB 연결
    conn = pymysql.connect(
        host='prod-common-db.cluster-ro-ch624l3cypvt.ap-northeast-2.rds.amazonaws.com',
        user='cancun_data',
        password='#ZXsd@~H>)2>',
        database='cancun',
        port=3306,
        charset='utf8mb4'
    )

    print("\n📊 데이터 조회 중...")

    # 1. 회원가입 데이터
    q1 = """
    SELECT 
        week(u.created_at,1) as week,
        CASE 
            WHEN u.recommender_username REGEXP '^#' THEN '영업대행'
            WHEN u.recommender_username REGEXP '^[a-zA-Z0-9]+$' THEN '친구추천'
        END as type,
        COUNT(*) as cnt
    FROM cancun.user u
    WHERE u.deleted_yn = 'n' 
    AND year(u.created_at) = 2025
    AND week(u.created_at,1) <= 32
    GROUP BY week, type
    HAVING type IS NOT NULL
    """
    df1 = pd.read_sql(q1, conn)
    print(f"회원가입: {len(df1)}행")

    # 2. 신규가입자
    q2 = """
    SELECT 
        week(substr(period_date,1,10),1) as week,
        sum(new_count) as cnt
    FROM cancun.dashboard_user
    WHERE period_type = 'DAILY'
    AND year(substr(period_date,1,10)) = 2025
    AND week(substr(period_date,1,10),1) <= 32
    GROUP BY week
    """
    df2 = pd.read_sql(q2, conn)
    print(f"신규가입: {len(df2)}행")

    # 3. 직배요청
    q3 = """
    SELECT 
        week(substr(created_at,1,10),1) as week,
        count(*) as cnt
    FROM cancun.direct_shipping
    WHERE is_deleted = 0
    AND year(substr(created_at,1,10)) = 2025
    AND week(substr(created_at,1,10),1) <= 32
    GROUP BY week
    """
    df3 = pd.read_sql(q3, conn)
    print(f"직배요청: {len(df3)}행")

    # 4. 누적 데이터 조회 (각 주차의 마지막 날)
    q4 = """
    WITH weekly_last AS (
        SELECT 
            week(substr(period_date,1,10),1) as week,
            ok_total_count,
            ok_direct_count,
            ok_parcel_count,
            ROW_NUMBER() OVER (PARTITION BY week(substr(period_date,1,10),1) 
                              ORDER BY period_date DESC) as rn
        FROM cancun.dashboard_user
        WHERE period_type = 'DAILY'
        AND year(substr(period_date,1,10)) = 2025
        AND week(substr(period_date,1,10),1) <= 32
    )
    SELECT week, ok_total_count, ok_direct_count, ok_parcel_count
    FROM weekly_last
    WHERE rn = 1
    """
    df4 = pd.read_sql(q4, conn)
    print(f"누적데이터: {len(df4)}행")

    # 5. 증감 데이터 조회
    q5 = """
    WITH weekly_last AS (
        SELECT 
            week(substr(period_date,1,10),1) as week,
            ok_total_count,
            ok_direct_count,
            ok_parcel_count,
            ROW_NUMBER() OVER (PARTITION BY week(substr(period_date,1,10),1) 
                              ORDER BY period_date DESC) as rn
        FROM cancun.dashboard_user
        WHERE period_type = 'DAILY'
        AND year(substr(period_date,1,10)) = 2025
        AND week(substr(period_date,1,10),1) <= 32
    ),
    weekly_data AS (
        SELECT week, ok_total_count as total_count, ok_direct_count as direct_count, ok_parcel_count as parcel_count
        FROM weekly_last
        WHERE rn = 1
    )
    SELECT 
        curr.week,
        curr.total_count - IFNULL(prev.total_count, 0) as total_growth,
        curr.direct_count - IFNULL(prev.direct_count, 0) as direct_growth,
        curr.parcel_count - IFNULL(prev.parcel_count, 0) as parcel_growth
    FROM weekly_data curr
    LEFT JOIN weekly_data prev ON curr.week = prev.week + 1
    WHERE curr.week >= 2
    """
    df5 = pd.read_sql(q5, conn)
    print(f"증감데이터: {len(df5)}행")

    conn.close()

    # Google Sheets 연결
    print("\n📝 Google Sheets 업데이트 중...")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key('1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE')
    ws = sheet.worksheet('automation(매출)')

    # 업데이트 (2주차부터 32주차까지)
    cnt = 0
    for w in range(2, 33):  # 2주차부터 시작
        col = w + 1  # B열부터

        # 6행: 신규가입
        val = df2[df2['week'] == w]
        if not val.empty:
            ws.update_cell(6, col, int(val['cnt'].iloc[0]))
            cnt += 1
            print(f"  {w}주차 6행 = {int(val['cnt'].iloc[0])}")
            time.sleep(2)

        # 8행: 친구추천
        val = df1[(df1['week'] == w) & (df1['type'] == '친구추천')]
        if not val.empty:
            ws.update_cell(8, col, int(val['cnt'].iloc[0]))
            cnt += 1
            print(f"  {w}주차 8행 = {int(val['cnt'].iloc[0])}")
            time.sleep(2)

        # 9행: 영업대행
        val = df1[(df1['week'] == w) & (df1['type'] == '영업대행')]
        if not val.empty:
            ws.update_cell(9, col, int(val['cnt'].iloc[0]))
            cnt += 1
            print(f"  {w}주차 9행 = {int(val['cnt'].iloc[0])}")
            time.sleep(2)

        # 11행: (정상)회원수 - 누적
        val = df4[df4['week'] == w]
        if not val.empty:
            ws.update_cell(11, col, int(val['ok_total_count'].iloc[0]))
            cnt += 1
            print(f"  {w}주차 11행 (정상)회원수 = {int(val['ok_total_count'].iloc[0])}")
            time.sleep(2)

        # 12행: 직배 - 누적
        val = df4[df4['week'] == w]
        if not val.empty:
            ws.update_cell(12, col, int(val['ok_direct_count'].iloc[0]))
            cnt += 1
            print(f"  {w}주차 12행 직배 = {int(val['ok_direct_count'].iloc[0])}")
            time.sleep(2)

        # 13행: 택배 - 누적
        val = df4[df4['week'] == w]
        if not val.empty:
            ws.update_cell(13, col, int(val['ok_parcel_count'].iloc[0]))
            cnt += 1
            print(f"  {w}주차 13행 택배 = {int(val['ok_parcel_count'].iloc[0])}")
            time.sleep(2)

        # 14행: (정상)회원 증감
        val = df5[df5['week'] == w]
        if not val.empty:
            ws.update_cell(14, col, int(val['total_growth'].iloc[0]))
            cnt += 1
            print(f"  {w}주차 14행 (정상)회원 증감 = {int(val['total_growth'].iloc[0])}")
            time.sleep(2)

        # 15행: 직배 증감
        val = df5[df5['week'] == w]
        if not val.empty:
            ws.update_cell(15, col, int(val['direct_growth'].iloc[0]))
            cnt += 1
            print(f"  {w}주차 15행 직배 증감 = {int(val['direct_growth'].iloc[0])}")
            time.sleep(2)

        # 16행: 택배 증감
        val = df5[df5['week'] == w]
        if not val.empty:
            ws.update_cell(16, col, int(val['parcel_growth'].iloc[0]))
            cnt += 1
            print(f"  {w}주차 16행 택배 증감 = {int(val['parcel_growth'].iloc[0])}")
            time.sleep(2)

        # 17행: 직배 요청
        val = df3[df3['week'] == w]
        if not val.empty:
            ws.update_cell(17, col, int(val['cnt'].iloc[0]))
            cnt += 1
            print(f"  {w}주차 17행 직배 요청 = {int(val['cnt'].iloc[0])}")
            time.sleep(2)

    print(f"\n✅ 완료! {cnt}개 셀 업데이트")


def monthly_update():
    """월별 업데이트"""
    print("=" * 60)
    print("월별 업데이트 시작 (1~8월)")
    print("=" * 60)

    # DB 연결
    conn = pymysql.connect(
        host='prod-common-db.cluster-ro-ch624l3cypvt.ap-northeast-2.rds.amazonaws.com',
        user='cancun_data',
        password='#ZXsd@~H>)2>',
        database='cancun',
        port=3306,
        charset='utf8mb4'
    )

    print("\n📊 데이터 조회 중...")

    # 1. 회원가입 데이터
    q1 = """
         SELECT
             month (u.created_at) as month, CASE
             WHEN u.recommender_username REGEXP '^#' THEN '영업대행'
             WHEN u.recommender_username REGEXP '^[a-zA-Z0-9]+' THEN '친구추천'
         END \
         as type,
            COUNT(*) as cnt
        FROM cancun.user u
        WHERE u.deleted_yn = 'n' 
        AND year(u.created_at) = 2025
        AND month(u.created_at) <= 8
        GROUP BY month, type
        HAVING type IS NOT NULL \
         """
    df1 = pd.read_sql(q1, conn)
    print(f"회원가입: {len(df1)}행")

    # 2. 신규가입자
    q2 = """
    SELECT 
        month(substr(period_date,1,10)) as month,
        sum(new_count) as cnt
    FROM cancun.dashboard_user
    WHERE period_type = 'DAILY'
    AND year(substr(period_date,1,10)) = 2025
    AND month(substr(period_date,1,10)) <= 8
    GROUP BY month
    """
    df2 = pd.read_sql(q2, conn)
    print(f"신규가입: {len(df2)}행")

    # 3. 직배요청
    q3 = """
    SELECT 
        month(substr(created_at,1,10)) as month,
        count(*) as cnt
    FROM cancun.direct_shipping
    WHERE is_deleted = 0
    AND year(substr(created_at,1,10)) = 2025
    AND month(substr(created_at,1,10)) <= 8
    GROUP BY month
    """
    df3 = pd.read_sql(q3, conn)
    print(f"직배요청: {len(df3)}행")

    # 4. 월별 실제 회원수 조회 (각 월의 마지막 날)
    q4 = """
    WITH monthly_last AS (
        SELECT 
            month(substr(period_date,1,10)) as month,
            ok_total_count,
            ok_direct_count,
            ok_parcel_count,
            ROW_NUMBER() OVER (PARTITION BY month(substr(period_date,1,10)) 
                              ORDER BY period_date DESC) as rn
        FROM cancun.dashboard_user
        WHERE period_type = 'DAILY'
        AND year(substr(period_date,1,10)) = 2025
        AND month(substr(period_date,1,10)) <= 8
    )
    SELECT month, ok_total_count, ok_direct_count, ok_parcel_count
    FROM monthly_last
    WHERE rn = 1
    """
    df4 = pd.read_sql(q4, conn)
    print(f"실제회원수: {len(df4)}행")

    # 5. 월별 증감 데이터 조회
    q5 = """
    WITH monthly_last AS (
        SELECT 
            month(substr(period_date,1,10)) as month,
            ok_total_count,
            ok_direct_count,
            ok_parcel_count,
            ROW_NUMBER() OVER (PARTITION BY month(substr(period_date,1,10)) 
                              ORDER BY period_date DESC) as rn
        FROM cancun.dashboard_user
        WHERE period_type = 'DAILY'
        AND year(substr(period_date,1,10)) = 2025
        AND month(substr(period_date,1,10)) <= 8
    ),
    monthly_data AS (
        SELECT month, ok_total_count as total_count, ok_direct_count as direct_count, ok_parcel_count as parcel_count
        FROM monthly_last
        WHERE rn = 1
    )
    SELECT 
        curr.month,
        curr.total_count - IFNULL(prev.total_count, 0) as total_growth,
        curr.direct_count - IFNULL(prev.direct_count, 0) as direct_growth,
        curr.parcel_count - IFNULL(prev.parcel_count, 0) as parcel_growth
    FROM monthly_data curr
    LEFT JOIN monthly_data prev ON curr.month = prev.month + 1
    WHERE curr.month >= 2
    """
    df5 = pd.read_sql(q5, conn)
    print(f"증감데이터: {len(df5)}행")

    conn.close()

    # Google Sheets 연결
    print("\n📝 Google Sheets 업데이트 중...")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key('1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE')
    ws = sheet.worksheet('automation(매출월기준)')

    # 업데이트
    cnt = 0
    for m in range(1, 9):
        col = m + 1  # B열부터

        # 6행: 신규가입
        val = df2[df2['month'] == m]
        if not val.empty:
            ws.update_cell(6, col, int(val['cnt'].iloc[0]))
            cnt += 1
            print(f"  {m}월 6행 = {int(val['cnt'].iloc[0])}")
            time.sleep(2)

        # 8행: 친구추천
        val = df1[(df1['month'] == m) & (df1['type'] == '친구추천')]
        if not val.empty:
            ws.update_cell(8, col, int(val['cnt'].iloc[0]))
            cnt += 1
            print(f"  {m}월 8행 = {int(val['cnt'].iloc[0])}")
            time.sleep(2)

        # 9행: 영업대행
        val = df1[(df1['month'] == m) & (df1['type'] == '영업대행')]
        if not val.empty:
            ws.update_cell(9, col, int(val['cnt'].iloc[0]))
            cnt += 1
            print(f"  {m}월 9행 = {int(val['cnt'].iloc[0])}")
            time.sleep(2)

        # 11행: (정상)회원수 - 실제값
        val = df4[df4['month'] == m]
        if not val.empty:
            ws.update_cell(11, col, int(val['ok_total_count'].iloc[0]))
            cnt += 1
            print(f"  {m}월 11행 (정상)회원수 = {int(val['ok_total_count'].iloc[0])}")
            time.sleep(2)

        # 12행: 직배 - 실제값
        val = df4[df4['month'] == m]
        if not val.empty:
            ws.update_cell(12, col, int(val['ok_direct_count'].iloc[0]))
            cnt += 1
            print(f"  {m}월 12행 직배 = {int(val['ok_direct_count'].iloc[0])}")
            time.sleep(2)

        # 13행: 택배 - 실제값
        val = df4[df4['month'] == m]
        if not val.empty:
            ws.update_cell(13, col, int(val['ok_parcel_count'].iloc[0]))
            cnt += 1
            print(f"  {m}월 13행 택배 = {int(val['ok_parcel_count'].iloc[0])}")
            time.sleep(2)

        # 14행: (정상)회원 증감
        val = df5[df5['month'] == m]
        if not val.empty:
            ws.update_cell(14, col, int(val['total_growth'].iloc[0]))
            cnt += 1
            print(f"  {m}월 14행 (정상)회원 증감 = {int(val['total_growth'].iloc[0])}")
            time.sleep(2)

        # 15행: 직배 증감
        val = df5[df5['month'] == m]
        if not val.empty:
            ws.update_cell(15, col, int(val['direct_growth'].iloc[0]))
            cnt += 1
            print(f"  {m}월 15행 직배 증감 = {int(val['direct_growth'].iloc[0])}")
            time.sleep(2)

        # 16행: 택배 증감
        val = df5[df5['month'] == m]
        if not val.empty:
            ws.update_cell(16, col, int(val['parcel_growth'].iloc[0]))
            cnt += 1
            print(f"  {m}월 16행 택배 증감 = {int(val['parcel_growth'].iloc[0])}")
            time.sleep(2)

        # 17행: 직배 요청
        val = df3[df3['month'] == m]
        if not val.empty:
            ws.update_cell(17, col, int(val['cnt'].iloc[0]))
            cnt += 1
            print(f"  {m}월 17행 직배 요청 = {int(val['cnt'].iloc[0])}")
            time.sleep(2)

    print(f"\n✅ 완료! {cnt}개 셀 업데이트")


if __name__ == "__main__":
    print("1. 주차별")
    print("2. 월별")
    print("3. 둘 다")

    c = input("선택: ").strip()

    if c == "1":
        weekly_update()
    elif c == "2":
        monthly_update()
    elif c == "3":
        weekly_update()
        print()
        monthly_update()
    else:
        print("잘못된 선택")