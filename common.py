import pymysql
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import warnings
import time

# 모든 warning 무시
warnings.filterwarnings('ignore')

# 업데이트할 월 설정 (여기만 바꾸면 모든 함수에 적용됨)
TARGET_MONTH = 8  # 월 업데이트

# 업데이트할 주차 설정 (여기만 바꾸면 모든 함수에 적용됨)
TARGET_WEEK = 31  # 주차 업데이트 (1-12)


def get_weekly_signup_data():
    """회원가입 데이터를 주차별/추천타입별로 조회"""
    connection = pymysql.connect(
        host='prod-common-db.cluster-ro-ch624l3cypvt.ap-northeast-2.rds.amazonaws.com',
        user='cancun_data',
        password='#ZXsd@~H>)2>',
        database='cancun',
        port=3306,
        charset='utf8mb4'
    )

    query = """
            SELECT
                year (u.created_at) as signup_year, week(u.created_at, 1) as signup_week, -- 월요일 시작 주차 
                CASE WHEN u.recommender_username IS NULL OR u.recommender_username = '' THEN '-'
                WHEN u.recommender_username REGEXP '^#' THEN '오프라인'
                WHEN u.recommender_username REGEXP '^[a-zA-Z0-9]+$' THEN '친구추천'
                ELSE '-'
            END
            AS recommender_type,
        COUNT(*) as signup_count
    FROM cancun.base_user bu
    INNER JOIN cancun.user u ON u.base_user_id = bu.id
    WHERE u.deleted_yn = 'n'
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3 
            """

    df = pd.read_sql(query, connection)
    connection.close()

    print(f"🔍 회원가입 데이터 조회 완료: {len(df)}행")
    if not df.empty:
        print(f"🔍 주차 범위: {df['signup_week'].min()}주차 ~ {df['signup_week'].max()}주차")
        print(f"🔍 추천타입: {df['recommender_type'].unique().tolist()}")

    return df


def get_weekly_new_users_data():
    """dashboard_user 테이블에서 주차별 신규 가입자 수 조회"""
    connection = pymysql.connect(
        host='prod-common-db.cluster-ro-ch624l3cypvt.ap-northeast-2.rds.amazonaws.com',
        user='cancun_data',
        password='#ZXsd@~H>)2>',
        database='cancun',
        port=3306,
        charset='utf8mb4'
    )

    query = """
            SELECT
                year (substr(du.period_date, 1, 10)) as signup_year, week(substr(du.period_date, 1, 10), 1) as signup_week, sum(du.new_count) as new_signups_users
            FROM cancun.dashboard_user du
            WHERE du.period_type = 'DAILY'
            GROUP BY 1, 2
            ORDER BY 1, 2
            """

    df = pd.read_sql(query, connection)
    connection.close()

    print(f"🔍 신규 가입자 데이터 조회 완료: {len(df)}행")
    if not df.empty:
        print(f"🔍 주차 범위: {df['signup_week'].min()}주차 ~ {df['signup_week'].max()}주차")

    return df


def get_weekly_comparison_data():
    """주차별 증감 데이터 조회 (현재주차 - 이전주차)"""
    try:
        connection = pymysql.connect(
            host='prod-common-db.cluster-ro-ch624l3cypvt.ap-northeast-2.rds.amazonaws.com',
            user='cancun_data',
            password='#ZXsd@~H>)2>',
            database='cancun',
            port=3306,
            charset='utf8mb4'
        )

        query = f"""
                (SELECT '{TARGET_WEEK - 1}주차' as label, period_date, ok_total_count, ok_direct_count, ok_parcel_count
                 FROM cancun.dashboard_user
                 WHERE week(substr(period_date, 1, 10), 1) = {TARGET_WEEK - 1}
                   AND period_type = 'DAILY'
                 ORDER BY period_date DESC LIMIT 1)
                UNION ALL
                (SELECT '{TARGET_WEEK}주차' as label, period_date, ok_total_count, ok_direct_count, ok_parcel_count
                 FROM cancun.dashboard_user
                 WHERE week(substr(period_date, 1, 10), 1) = {TARGET_WEEK}
                   AND period_type = 'DAILY'
                 ORDER BY period_date DESC LIMIT 1)
                """

        print(f"증감 쿼리 실행 중... TARGET_WEEK = {TARGET_WEEK}")
        df = pd.read_sql(query, connection)
        connection.close()

        print(f"증감 데이터 조회 결과: {len(df)}행")
        print(df)

        if len(df) == 2:
            df = df.sort_values('period_date')
            previous_data = df.iloc[0]
            current_data = df.iloc[1]

            growth_data = {
                'current_week': TARGET_WEEK,
                'total_growth': int(current_data['ok_total_count'] - previous_data['ok_total_count']),
                'direct_growth': int(current_data['ok_direct_count'] - previous_data['ok_direct_count']),
                'parcel_growth': int(current_data['ok_parcel_count'] - previous_data['ok_parcel_count'])
            }

            print(f"증감값 계산됨: {growth_data}")
            return pd.DataFrame([growth_data])
        else:
            print(f"데이터 부족함: {len(df)}행")
            return pd.DataFrame()

    except Exception as e:
        print(f"증감 데이터 조회 에러: {e}")
        return pd.DataFrame()


def get_weekly_direct_shipping_data():
    """주차별 직배 요청 데이터 조회"""
    connection = pymysql.connect(
        host='prod-common-db.cluster-ro-ch624l3cypvt.ap-northeast-2.rds.amazonaws.com',
        user='cancun_data',
        password='#ZXsd@~H>)2>',
        database='cancun',
        port=3306,
        charset='utf8mb4'
    )

    query = """
            SELECT     
                year(substr(ds.created_at,1,10)) as request_year,     
                week(substr(ds.created_at,1,10),1) as request_week,     
                count(u.company_name) as company_count 
            FROM cancun.direct_shipping ds 
            LEFT JOIN cancun.user u ON u.base_user_id = ds.user_id AND u.deleted_yn = 'N' 
            JOIN cancun.base_user bu ON u.base_user_id = bu.id 
            JOIN cancun.user_shipping us ON u.base_user_id = us.user_id 
            WHERE ds.is_deleted = 0 
            GROUP BY 1,2
            ORDER BY 1,2
            """

    df = pd.read_sql(query, connection)
    connection.close()

    print(f"🔍 주차별 직배 요청 데이터 조회 완료: {len(df)}행")
    if not df.empty:
        print(f"🔍 주차 범위: {df['request_week'].min()}주차 ~ {df['request_week'].max()}주차")

    return df


def get_monthly_cumulative_data():
    """월별 누적 데이터 조회 (실행일 전날까지)"""
    connection = pymysql.connect(
        host='prod-common-db.cluster-ro-ch624l3cypvt.ap-northeast-2.rds.amazonaws.com',
        user='cancun_data',
        password='#ZXsd@~H>)2>',
        database='cancun',
        port=3306,
        charset='utf8mb4'
    )

    query = f"""
            SELECT 
                {TARGET_MONTH} as target_month,
                ok_total_count,
                ok_direct_count,
                ok_parcel_count,
                period_date
            FROM cancun.dashboard_user
            WHERE month(substr(period_date, 1, 10)) = {TARGET_MONTH}
              AND year(substr(period_date, 1, 10)) = 2025
              AND period_type = 'DAILY'
              AND period_date < CURDATE()
            ORDER BY period_date DESC 
            LIMIT 1
            """

    df = pd.read_sql(query, connection)
    connection.close()

    print(f"🔍 월별 누적 데이터 조회 완료: {len(df)}행")
    if not df.empty:
        print(f"🔍 {TARGET_MONTH}월 마지막 데이터: {df['period_date'].iloc[0]}")

    return df


def get_monthly_new_users_data():
    """dashboard_user 테이블에서 월별 신규 가입자 수 조회"""
    connection = pymysql.connect(
        host='prod-common-db.cluster-ro-ch624l3cypvt.ap-northeast-2.rds.amazonaws.com',
        user='cancun_data',
        password='#ZXsd@~H>)2>',
        database='cancun',
        port=3306,
        charset='utf8mb4'
    )

    query = """
            SELECT
                year (substr(period_date, 1, 10)) as signup_year, month (substr(period_date, 1, 10)) as signup_month, sum(new_count) as new_signups_users
            FROM cancun.dashboard_user
            WHERE period_type = 'DAILY'
            GROUP BY 1, 2
            ORDER BY 1, 2
            """

    df = pd.read_sql(query, connection)
    connection.close()

    print(f"🔍 월별 신규 가입자 데이터 조회 완료: {len(df)}행")
    if not df.empty:
        print(f"🔍 월 범위: {df['signup_month'].min()}월 ~ {df['signup_month'].max()}월")

    return df


def get_monthly_signup_data():
    """회원가입 데이터를 월별/추천타입별로 조회"""
    connection = pymysql.connect(
        host='prod-common-db.cluster-ro-ch624l3cypvt.ap-northeast-2.rds.amazonaws.com',
        user='cancun_data',
        password='#ZXsd@~H>)2>',
        database='cancun',
        port=3306,
        charset='utf8mb4'
    )

    query = """
            SELECT
                year (u.created_at) as signup_year, month (u.created_at) as signup_month, CASE WHEN u.recommender_username IS NULL OR u.recommender_username = '' THEN '-'
                WHEN u.recommender_username REGEXP '^#' THEN '오프라인'
                WHEN u.recommender_username REGEXP '^[a-zA-Z0-9]+$' THEN '친구추천'
                ELSE '-'
            END
            AS recommender_type,
                COUNT(*) as signup_count
            FROM cancun.base_user bu
            INNER JOIN cancun.user u ON u.base_user_id = bu.id
            WHERE u.deleted_yn = 'n'
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3 
            """

    df = pd.read_sql(query, connection)
    connection.close()

    print(f"🔍 월별 회원가입 데이터 조회 완료: {len(df)}행")
    if not df.empty:
        print(f"🔍 월 범위: {df['signup_month'].min()}월 ~ {df['signup_month'].max()}월")
        print(f"🔍 추천타입: {df['recommender_type'].unique().tolist()}")

    return df


def get_monthly_direct_shipping_data():
    """월별 직배 요청 데이터 조회"""
    connection = pymysql.connect(
        host='prod-common-db.cluster-ro-ch624l3cypvt.ap-northeast-2.rds.amazonaws.com',
        user='cancun_data',
        password='#ZXsd@~H>)2>',
        database='cancun',
        port=3306,
        charset='utf8mb4'
    )

    query = """
            SELECT     
                year(substr(ds.created_at,1,10)) as request_year,     
                month(substr(ds.created_at,1,10)) as request_month,     
                count(u.company_name) as company_count 
            FROM cancun.direct_shipping ds 
            LEFT JOIN cancun.user u ON u.base_user_id = ds.user_id AND u.deleted_yn = 'N' 
            JOIN cancun.base_user bu ON u.base_user_id = bu.id 
            JOIN cancun.user_shipping us ON u.base_user_id = us.user_id 
            WHERE ds.is_deleted = 0 
            GROUP BY 1,2
            ORDER BY 1,2
            """

    df = pd.read_sql(query, connection)
    connection.close()

    print(f"🔍 월별 직배 요청 데이터 조회 완료: {len(df)}행")
    if not df.empty:
        print(f"🔍 월 범위: {df['request_month'].min()}월 ~ {df['request_month'].max()}월")

    return df

def update_signup_sheets(signup_df, new_users_df, comparison_df, direct_shipping_df):
    """Google Sheets에 회원가입 데이터 업데이트 - 추천타입별로 8행(친구추천), 9행(오프라인)"""
    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 시트 열기
    sheet = client.open_by_key('1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE')
    worksheet = sheet.worksheet('automation(주문)')

    # 추천타입별 행 번호
    recommender_rows = {
        '친구추천': 8,  # 8행
        '오프라인': 9  # 9행
    }

    print(f"\n=== 회원가입 추천타입별 데이터 업데이트 ===")

    # 주차별 열 매핑: 29주차=B열(2), 30주차=C열(3), 31주차=D열(4)...
    target_week = TARGET_WEEK
    target_col = 2 + (target_week - 29)  # 29주차부터 시작하여 B열부터 매핑

    print(f"회원가입 {target_week}주차를 {chr(64 + target_col)}열에 업데이트합니다.")

    # 해당 주차 데이터만 찾기
    target_week_data = signup_df[signup_df['signup_week'] == target_week]

    if target_week_data.empty:
        print(f"❌ 회원가입 {target_week}주차 데이터가 없습니다.")
        return

    print(f"📊 {target_week}주차 데이터: {len(target_week_data)}개 추천타입")

    # 추천타입별로 업데이트
    updated_count = 0
    for _, row in target_week_data.iterrows():
        recommender_type = row['recommender_type']
        signup_count = int(row['signup_count'])

        if recommender_type in recommender_rows:
            target_row = recommender_rows[recommender_type]
            worksheet.update_cell(target_row, target_col, signup_count)
            time.sleep(1.0)
            print(f"  ✅ {recommender_type}: 행{target_row}, 열{target_col} = {signup_count}명")
            updated_count += 1
        else:
            print(f"  ⚠️ {recommender_type}: 매핑되지 않은 추천타입 (값: {signup_count})")

    print(f"🎉 회원가입 {target_week}주차 업데이트 완료! ({updated_count}개 타입 업데이트)")

    # === 6행 신규 가입자 업데이트 추가 ===
    if new_users_df is not None:
        target_week_new_users = new_users_df[
            (new_users_df['signup_week'] == target_week) &
            (new_users_df['signup_year'] == 2025)
            ]

        if not target_week_new_users.empty:
            new_users_count = int(target_week_new_users['new_signups_users'].iloc[0])
            worksheet.update_cell(6, target_col, new_users_count)  # 6행에 업데이트
            time.sleep(1.0)
            print(f"  ✅ 신규 가입자: 행6, 열{target_col} = {new_users_count}명")
        else:
            print(f"  ⚠️ {target_week}주차 신규 가입자 데이터가 없습니다.")

    # === 11행, 12행, 13행 증감 데이터 업데이트 추가 ===
    if comparison_df is not None and not comparison_df.empty:
        total_growth = int(comparison_df['total_growth'].iloc[0])
        direct_growth = int(comparison_df['direct_growth'].iloc[0])
        parcel_growth = int(comparison_df['parcel_growth'].iloc[0])

        # 11행: 전체 증감
        worksheet.update_cell(11, target_col, total_growth)
        time.sleep(1.0)
        print(f"  ✅ 전체 증감: 행11, 열{target_col} = {total_growth}")

        # 12행: 직배 증감
        worksheet.update_cell(12, target_col, direct_growth)
        time.sleep(1.0)
        print(f"  ✅ 직배 증감: 행12, 열{target_col} = {direct_growth}")

        # 13행: 택배 증감
        worksheet.update_cell(13, target_col, parcel_growth)
        time.sleep(1.0)
        print(f"  ✅ 택배 증감: 행13, 열{target_col} = {parcel_growth}")
    else:
        print(f"  ⚠️ {target_week}주차 증감 데이터가 없습니다.")

    # 주차별 직배 데이터 업데이트 로직 (update_signup_sheets 함수 안에 추가)
    # === 14행 주차별 직배 요청 업데이트 추가 ===
    if direct_shipping_df is not None:
        target_week_direct = direct_shipping_df[
            (direct_shipping_df['request_week'] == target_week) &
            (direct_shipping_df['request_year'] == 2025)
            ]

        if not target_week_direct.empty:
            direct_count = int(target_week_direct['company_count'].iloc[0])
            worksheet.update_cell(14, target_col, direct_count)  # 14행에 업데이트
            time.sleep(1.0)
            print(f"  ✅ 직배 요청: 행14, 열{target_col} = {direct_count}개")
        else:
            print(f"  ⚠️ {target_week}주차 직배 요청 데이터가 없습니다.")


def update_monthly_signup_sheets(signup_df, monthly_new_users_df=None, monthly_cumulative_df=None,
                                     monthly_direct_shipping_df=None):
    """Google Sheets에 월별 회원가입 데이터 업데이트 - 추천타입별로 8행(친구추천), 9행(오프라인)"""
    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 시트 열기
    sheet = client.open_by_key('1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE')
    worksheet = sheet.worksheet('automation(매출월기준)')

    # 추천타입별 행 번호
    recommender_rows = {
        '친구추천': 8,  # 8행
        '오프라인': 9  # 9행
    }

    print(f"\n=== 월별 회원가입 추천타입별 데이터 업데이트 ===")

    # 월별 열 매핑: 7월=B열(2), 8월=C열(3), 9월=D열(4)...
    target_month = TARGET_MONTH
    target_col = 2 + (target_month - 7)  # 7월부터 시작하여 B열부터 매핑

    print(f"회원가입 {target_month}월을 {chr(64 + target_col)}열에 업데이트합니다.")

    # 해당 월 데이터만 찾기 (2025년)
    target_month_data = signup_df[
        (signup_df['signup_month'] == target_month) &
        (signup_df['signup_year'] == 2025)
        ]

    if target_month_data.empty:
        print(f"❌ 회원가입 2025년 {target_month}월 데이터가 없습니다.")
        return

    print(f"📊 2025년 {target_month}월 데이터: {len(target_month_data)}개 추천타입")

    # 추천타입별로 업데이트
    updated_count = 0
    for _, row in target_month_data.iterrows():
        recommender_type = row['recommender_type']
        signup_count = int(row['signup_count'])

        if recommender_type in recommender_rows:
            target_row = recommender_rows[recommender_type]
            worksheet.update_cell(target_row, target_col, signup_count)
            time.sleep(1.0)
            print(f"  ✅ {recommender_type}: 행{target_row}, 열{target_col} = {signup_count}명")
            updated_count += 1
        else:
            print(f"  ⚠️ {recommender_type}: 매핑되지 않은 추천타입 (값: {signup_count})")

    print(f"🎉 회원가입 2025년 {target_month}월 업데이트 완료! ({updated_count}개 타입 업데이트)")

    # === 6행 월별 신규 가입자 업데이트 추가 ===
    if monthly_new_users_df is not None:
        target_month_new_users = monthly_new_users_df[
            (monthly_new_users_df['signup_month'] == target_month) &
            (monthly_new_users_df['signup_year'] == 2025)
            ]

        if not target_month_new_users.empty:
            new_users_count = int(target_month_new_users['new_signups_users'].iloc[0])
            worksheet.update_cell(6, target_col, new_users_count)  # 6행에 업데이트
            time.sleep(1.0)
            print(f"  ✅ 월별 신규 가입자: 행6, 열{target_col} = {new_users_count}명")
        else:
            print(f"  ⚠️ 2025년 {target_month}월 신규 가입자 데이터가 없습니다.")

    # === 월별 누적 데이터 업데이트 추가 ===
    if monthly_cumulative_df is not None and not monthly_cumulative_df.empty:
        total_count = int(monthly_cumulative_df['ok_total_count'].iloc[0])
        direct_count = int(monthly_cumulative_df['ok_direct_count'].iloc[0])
        parcel_count = int(monthly_cumulative_df['ok_parcel_count'].iloc[0])

        # 11행: 전체 누적
        worksheet.update_cell(11, target_col, total_count)
        time.sleep(1.0)
        print(f"  ✅ 전체 누적: 행11, 열{target_col} = {total_count}")

        # 12행: 직배 누적
        worksheet.update_cell(12, target_col, direct_count)
        time.sleep(1.0)
        print(f"  ✅ 직배 누적: 행12, 열{target_col} = {direct_count}")

        # 13행: 택배 누적
        worksheet.update_cell(13, target_col, parcel_count)
        time.sleep(1.0)
        print(f"  ✅ 택배 누적: 행13, 열{target_col} = {parcel_count}")
    else:
        print(f"  ⚠️ 2025년 {target_month}월 누적 데이터가 없습니다.")

    # 월별 직배 데이터 업데이트 로직 (update_monthly_signup_sheets 함수 안에 추가)
    # === 14행 월별 직배 요청 업데이트 추가 ===
    if monthly_direct_shipping_df is not None:
        target_month_direct = monthly_direct_shipping_df[
            (monthly_direct_shipping_df['request_month'] == target_month) &
            (monthly_direct_shipping_df['request_year'] == 2025)
            ]

        if not target_month_direct.empty:
            direct_count = int(target_month_direct['company_count'].iloc[0])
            worksheet.update_cell(14, target_col, direct_count)  # 14행에 업데이트
            time.sleep(1.0)
            print(f"  ✅ 월별 직배 요청: 행14, 열{target_col} = {direct_count}개")
        else:
            print(f"  ⚠️ 2025년 {target_month}월 직배 요청 데이터가 없습니다.")


def main_weekly():
    """회원가입 데이터 주차별 업데이트"""
    print(f"🚀 {TARGET_WEEK}주차 회원가입 데이터 업데이트 시작...")
    print(f"📅 매핑: 29주차=B열, 30주차=C열, 31주차=D열...")
    print(f"📍 대상: 6행(신규가입자), 8행(친구추천), 9행(오프라인), 11행(전체증감), 12행(직배증감), 13행(택배증감)")

    try:
        # 1. 회원가입 데이터 조회
        signup_df = get_weekly_signup_data()

        # 1-1. 신규 가입자 데이터 조회
        new_users_df = get_weekly_new_users_data()

        comparison_df = get_weekly_comparison_data()

        # 1-3. 직배 데이터 조회 (추가!)
        direct_shipping_df = get_weekly_direct_shipping_data()

        if signup_df.empty:
            print("❌ 조회된 회원가입 데이터가 없습니다.")
            return

        # 2. Google Sheets 업데이트
        update_signup_sheets(signup_df, new_users_df, comparison_df , direct_shipping_df)

        print(f"\n🎊 {TARGET_WEEK}주차 회원가입 데이터 업데이트 완료!")
        print(f"✨ {chr(64 + 2 + (TARGET_WEEK - 29))}열에 데이터가 업데이트되었습니다.")

    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()


def main_monthly():
    """회원가입 데이터 월별 업데이트"""
    print(f"🚀 {TARGET_MONTH}월 회원가입 데이터 업데이트 시작...")
    print(f"📅 매핑: 7월=B열, 8월=C열, 9월=D열, 10월=E열...")
    print(f"📍 대상: automation(매출월기준) 시트 6행(신규가입자), 8행(친구추천), 9행(오프라인), 11행(전체누적), 12행(직배누적), 13행(택배누적)")

    try:
        # 1. 월별 회원가입 데이터 조회
        signup_df = get_monthly_signup_data()

        # 1-1. 월별 신규 가입자 데이터 조회
        monthly_new_users_df = get_monthly_new_users_data()

        # 1-2. 월별 누적 데이터 조회
        monthly_cumulative_df = get_monthly_cumulative_data()

        # 1-3. 월별 직배 데이터 조회 (추가!)
        monthly_direct_shipping_df = get_monthly_direct_shipping_data()

        if signup_df.empty:
            print("❌ 조회된 월별 회원가입 데이터가 없습니다.")
            return

        # 2. Google Sheets 업데이트
        update_monthly_signup_sheets(signup_df, monthly_new_users_df, monthly_cumulative_df, monthly_direct_shipping_df)

        print(f"\n🎊 2025년 {TARGET_MONTH}월 회원가입 데이터 업데이트 완료!")
        print(f"✨ {chr(64 + 2 + (TARGET_MONTH - 7))}열에 데이터가 업데이트되었습니다.")

    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # 주차별 업데이트를 원하면 main_weekly() 실행
    # 월별 업데이트를 원하면 main_monthly() 실행

    print("🔄 업데이트 타입을 선택하세요:")
    print("1. 주차별 업데이트 (automation(주문) 시트)")
    print("2. 월별 업데이트 (automation(매출월기준) 시트)")

    choice = input("선택 (1 또는 2): ").strip()

    if choice == "1":
        main_weekly()
    elif choice == "2":
        main_monthly()
    else:
        print("❌ 잘못된 선택입니다. 1 또는 2를 입력하세요.")