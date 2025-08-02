import pymysql
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import warnings
import time

# 모든 warning 무시
warnings.filterwarnings('ignore')

# 업데이트할 주차 설정 (여기만 바꾸면 모든 함수에 적용됨)
TARGET_WEEK = 29  # 주차 업데이트

# 업데이트할 월 설정 (여기만 바꾸면 모든 함수에 적용됨)
TARGET_MONTH = 7  # 월 업데이트

# 업데이트할 년도 설정
TARGET_YEAR = 2025


def get_point_ratio_data():
    """포인트 비중 데이터를 주차별로 조회"""
    connection = pymysql.connect(
        host='prod-common-db.cluster-ro-ch624l3cypvt.ap-northeast-2.rds.amazonaws.com',
        user='cancun_data',
        password='#ZXsd@~H>)2>',
        database='cancun',
        port=3306,
        charset='utf8mb4'
    )

    query = f"""
    select 
        case 
            when reason = '첫구매이벤트' then '첫구매이벤트'
            when reason = '회원가입' then '회원가입' 
            when reason in ('친구추천(추천인)', '친구추천(신규회원)') then '친구추천'
            else reason
        end as 사유,
        sum(round(amount  / total_amount, 2)) as 비중
    from (
        select reason,
               sum(amount) as amount,
               sum(sum(amount)) over () as total_amount
        from (
            select CASE pnt.event_type
                       WHEN 'REWARD' THEN '충전'
                       WHEN 'USED' THEN '사용'
                       WHEN 'EXPIRED' THEN '만료'
                       WHEN 'ADMIN_SUBTRACT' THEN '차감'
                       ELSE ''
                   END AS type,
                   CASE pnt.reason_type
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
                   END AS reason,
                   pnt.amount as amount,
                   substr(pnt.created_at,1,10) as recharge_date
            from cancun.point pnt
            inner join cancun.base_user bu on bu.id = pnt.user_id
            inner join cancun.user u on u.base_user_id = bu.id
        ) A
        where A.type = '충전'
          and year(recharge_date) = {TARGET_YEAR}
          and week(recharge_date, 1) = {TARGET_WEEK}
        group by reason
    ) B
    where reason in ('첫구매이벤트', '회원가입', '친구추천(추천인)', '친구추천(신규회원)')
    group by 1
    order by 
        case 
            when 사유 = '첫구매이벤트' then 1
            when 사유 = '회원가입' then 2 
            when 사유 = '친구추천' then 3
        end
    """

    df = pd.read_sql(query, connection)
    connection.close()

    print(f"🔍 포인트 비중 데이터 조회 완료: {len(df)}행")
    if not df.empty:
        print(f"🔍 {TARGET_YEAR}년 {TARGET_WEEK}주차 데이터:")
        for _, row in df.iterrows():
            print(f"   {row['사유']}: {row['비중']}")
    else:
        print(f"❌ {TARGET_YEAR}년 {TARGET_WEEK}주차 데이터가 없습니다.")

    return df

def get_monthly_point_ratio_data():
    """포인트 비중 데이터를 월별로 조회"""
    connection = pymysql.connect(
        host='prod-common-db.cluster-ro-ch624l3cypvt.ap-northeast-2.rds.amazonaws.com',
        user='cancun_data',
        password='#ZXsd@~H>)2>',
        database='cancun',
        port=3306,
        charset='utf8mb4'
    )

    query = f"""
    select 
        case 
            when reason = '첫구매이벤트' then '첫구매이벤트'
            when reason = '회원가입' then '회원가입' 
            when reason in ('친구추천(추천인)', '친구추천(신규회원)') then '친구추천'
            else reason
        end as 사유,
        sum(round(amount / total_amount, 2)) as 비중
    from (
        select reason,
               sum(amount) as amount,
               sum(sum(amount)) over () as total_amount
        from (
            select CASE pnt.event_type
                       WHEN 'REWARD' THEN '충전'
                       WHEN 'USED' THEN '사용'
                       WHEN 'EXPIRED' THEN '만료'
                       WHEN 'ADMIN_SUBTRACT' THEN '차감'
                       ELSE ''
                   END AS type,
                   CASE pnt.reason_type
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
                   END AS reason,
                   pnt.amount as amount,
                   substr(pnt.created_at,1,10) as recharge_date
            from cancun.point pnt
            inner join cancun.base_user bu on bu.id = pnt.user_id
            inner join cancun.user u on u.base_user_id = bu.id
        ) A
        where A.type = '충전'
          and year(recharge_date) = {TARGET_YEAR}
          and month(recharge_date) = {TARGET_MONTH}  # 여기가 월별 핵심!
        group by reason
    ) B
    where reason in ('첫구매이벤트', '회원가입', '친구추천(추천인)', '친구추천(신규회원)')
    group by 1
    order by 
        case 
            when 사유 = '첫구매이벤트' then 1
            when 사유 = '회원가입' then 2 
            when 사유 = '친구추천' then 3
        end
    """

    df = pd.read_sql(query, connection)
    connection.close()

    print(f"🔍 월별 포인트 비중 데이터 조회 완료: {len(df)}행")
    if not df.empty:
        print(f"🔍 {TARGET_YEAR}년 {TARGET_MONTH}월 데이터:")
        for _, row in df.iterrows():
            print(f"   {row['사유']}: {row['비중']}")
    else:
        print(f"❌ {TARGET_YEAR}년 {TARGET_MONTH}월 데이터가 없습니다.")

    return df

def update_point_ratio_sheets(point_ratio_df):
    """Google Sheets에 포인트 비중 데이터 업데이트 - automation(포인트비중) 시트"""
    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 시트 열기
    sheet = client.open_by_key('1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE')
    worksheet = sheet.worksheet('automation(포인트비중)')

    # 사유별 행 번호
    ratio_rows = {
        '첫구매이벤트': 3,  # 3행
        '회원가입': 4,  # 4행
        '친구추천': 5  # 5행
    }

    print(f"\n=== 포인트 비중 데이터 업데이트 ===")

    # 주차별 열 매핑: 29주차=B열(2), 30주차=C열(3), 31주차=D열(4)...
    target_week = TARGET_WEEK
    target_col = 2 + (target_week - 29)  # 29주차부터 시작하여 B열부터 매핑

    print(f"포인트 비중 {target_week}주차를 {chr(64 + target_col)}열에 업데이트합니다.")

    if point_ratio_df.empty:
        print(f"❌ 포인트 비중 {target_week}주차 데이터가 없습니다.")
        return

    print(f"📊 {target_week}주차 데이터: {len(point_ratio_df)}개 사유")

    # 기본값 설정 (모든 사유를 0으로 초기화)
    update_data = {
        '첫구매이벤트': 0,
        '회원가입': 0,
        '친구추천': 0
    }

    # 조회된 데이터로 업데이트
    for _, row in point_ratio_df.iterrows():
        사유 = row['사유']
        비중 = float(row['비중'])
        if 사유 in update_data:
            update_data[사유] = 비중

    print(f"📋 업데이트할 데이터: {update_data}")

    # 사유별로 업데이트
    updated_count = 0
    for 사유, 비중 in update_data.items():
        if 사유 in ratio_rows:
            target_row = ratio_rows[사유]
            worksheet.update_cell(target_row, target_col, 비중)
            time.sleep(1.0)
            print(f"  ✅ {사유}: 행{target_row}, 열{target_col} = {비중}")
            updated_count += 1

    print(f"🎉 포인트 비중 {target_week}주차 업데이트 완료! ({updated_count}개 사유 업데이트)")


def verify_point_ratio_update():
    """업데이트된 데이터 검증"""
    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 시트 열기
    sheet = client.open_by_key('1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE')
    worksheet = sheet.worksheet('automation(포인트비중)')

    # 업데이트된 컬럼 계산
    target_week = TARGET_WEEK
    target_col = 2 + (target_week - 29)
    col_letter = chr(64 + target_col)

    print(f"\n=== {target_week}주차 ({col_letter}열) 업데이트 검증 ===")

    # 3행~5행 데이터 읽기
    values = worksheet.get(f'{col_letter}3:{col_letter}5')

    if values:
        print(f"✅ 첫구매이벤트 (3행): {values[0][0] if len(values) > 0 and len(values[0]) > 0 else '값 없음'}")
        print(f"✅ 회원가입 (4행): {values[1][0] if len(values) > 1 and len(values[1]) > 0 else '값 없음'}")
        print(f"✅ 친구추천 (5행): {values[2][0] if len(values) > 2 and len(values[2]) > 0 else '값 없음'}")

        # 신규회원 포인트 Cost 계산용 합계
        try:
            첫구매 = float(values[0][0]) if len(values) > 0 and len(values[0]) > 0 else 0
            회원가입 = float(values[1][0]) if len(values) > 1 and len(values[1]) > 0 else 0
            합계 = 첫구매 + 회원가입
            print(f"💰 신규회원 포인트 Cost 계산용 합계: {합계} (첫구매 {첫구매} + 회원가입 {회원가입})")
        except:
            print("⚠️ 합계 계산 실패")
    else:
        print("❌ 업데이트된 값을 읽을 수 없습니다.")


def update_monthly_point_ratio_sheets(point_ratio_df):
    """Google Sheets에 월별 포인트 비중 데이터 업데이트 - automation(포인트월비중) 시트"""
    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 시트 열기 - 여기가 월별 시트!
    sheet = client.open_by_key('1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE')
    worksheet = sheet.worksheet('automation(포인트월비중)')

    # 사유별 행 번호 (주차별과 동일)
    ratio_rows = {
        '첫구매이벤트': 3,  # 3행
        '회원가입': 4,  # 4행
        '친구추천': 5  # 5행
    }

    print(f"\n=== 월별 포인트 비중 데이터 업데이트 ===")

    # 월별 열 매핑: 7월=B열(2), 8월=C열(3), 9월=D열(4)...
    target_month = TARGET_MONTH
    target_col = 2 + (target_month - 7)  # 7월부터 시작하여 B열부터 매핑

    print(f"포인트 비중 {target_month}월을 {chr(64 + target_col)}열에 업데이트합니다.")

    if point_ratio_df.empty:
        print(f"❌ 포인트 비중 {target_month}월 데이터가 없습니다.")
        return

    print(f"📊 {target_month}월 데이터: {len(point_ratio_df)}개 사유")

    # 기본값 설정 (모든 사유를 0으로 초기화)
    update_data = {
        '첫구매이벤트': 0,
        '회원가입': 0,
        '친구추천': 0
    }

    # 조회된 데이터로 업데이트
    for _, row in point_ratio_df.iterrows():
        사유 = row['사유']
        비중 = float(row['비중'])
        if 사유 in update_data:
            update_data[사유] = 비중

    print(f"📋 업데이트할 데이터: {update_data}")

    # 사유별로 업데이트
    updated_count = 0
    for 사유, 비중 in update_data.items():
        if 사유 in ratio_rows:
            target_row = ratio_rows[사유]
            worksheet.update_cell(target_row, target_col, 비중)  # 실제 업데이트!
            time.sleep(1.0)
            print(f"  ✅ {사유}: 행{target_row}, 열{target_col} = {비중}")
            updated_count += 1

    print(f"🎉 포인트 비중 {target_month}월 업데이트 완료! ({updated_count}개 사유 업데이트)")


def verify_monthly_point_ratio_update():
    """월별 업데이트된 데이터 검증"""
    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 시트 열기
    sheet = client.open_by_key('1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE')
    worksheet = sheet.worksheet('automation(포인트월비중)')

    # 업데이트된 컬럼 계산
    target_month = TARGET_MONTH
    target_col = 2 + (target_month - 7)
    col_letter = chr(64 + target_col)

    print(f"\n=== {target_month}월 ({col_letter}열) 업데이트 검증 ===")

    # 3행~5행 데이터 읽기
    values = worksheet.get(f'{col_letter}3:{col_letter}5')

    if values:
        print(f"✅ 첫구매이벤트 (3행): {values[0][0] if len(values) > 0 and len(values[0]) > 0 else '값 없음'}")
        print(f"✅ 회원가입 (4행): {values[1][0] if len(values) > 1 and len(values[1]) > 0 else '값 없음'}")
        print(f"✅ 친구추천 (5행): {values[2][0] if len(values) > 2 and len(values[2]) > 0 else '값 없음'}")

        # 신규회원 포인트 Cost 계산용 합계
        try:
            첫구매 = float(values[0][0]) if len(values) > 0 and len(values[0]) > 0 else 0
            회원가입 = float(values[1][0]) if len(values) > 1 and len(values[1]) > 0 else 0
            합계 = 첫구매 + 회원가입
            print(f"💰 신규회원 포인트 Cost 계산용 합계: {합계} (첫구매 {첫구매} + 회원가입 {회원가입})")
        except:
            print("⚠️ 합계 계산 실패")
    else:
        print("❌ 업데이트된 값을 읽을 수 없습니다.")

def main_point_ratio():
    """포인트 비중 데이터 주차별 업데이트"""
    print(f"🚀 {TARGET_WEEK}주차 포인트 비중 데이터 업데이트 시작...")
    print(f"📅 매핑: 29주차=B열, 30주차=C열, 31주차=D열...")
    print(f"📍 대상: automation(포인트비중) 시트 3행(첫구매이벤트), 4행(회원가입), 5행(친구추천)")
    print(f"🎯 타겟: {TARGET_YEAR}년 {TARGET_WEEK}주차")

    try:
        # 1. 포인트 비중 데이터 조회
        point_ratio_df = get_point_ratio_data()

        # 2. Google Sheets 업데이트
        update_point_ratio_sheets(point_ratio_df)

        # 3. 업데이트 검증
        verify_point_ratio_update()

        print(f"\n🎊 {TARGET_WEEK}주차 포인트 비중 데이터 업데이트 완료!")
        print(f"✨ {chr(64 + 2 + (TARGET_WEEK - 29))}열에 데이터가 업데이트되었습니다.")

    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()


def update_multiple_weeks():
    """여러 주차 일괄 업데이트"""
    global TARGET_WEEK

    # 사용자에게 주차들 입력받기
    print("여러 주차를 일괄 업데이트합니다.")
    weeks_input = input("업데이트할 주차들을 쉼표로 구분해서 입력하세요 (예: 29,30,31): ").strip()

    try:
        target_weeks = [int(week.strip()) for week in weeks_input.split(',')]
        target_weeks.sort()  # 순서대로 정렬
    except:
        print("❌ 유효한 주차 번호들을 입력하세요. (예: 29,30,31)")
        return

    print(f"📅 여러 주차 일괄 업데이트 시작: {target_weeks}")

    for week in target_weeks:
        print(f"\n{'=' * 50}")
        TARGET_WEEK = week  # 전역 변수 업데이트
        print(f"🎯 {week}주차 업데이트 중...")

        try:
            # 포인트 비중 데이터 조회
            point_ratio_df = get_point_ratio_data()

            # Google Sheets 업데이트
            update_point_ratio_sheets(point_ratio_df)

            print(f"✅ {week}주차 완료!")

        except Exception as e:
            print(f"❌ {week}주차 실패: {str(e)}")

        # 다음 주차 처리 전 잠시 대기
        if week != target_weeks[-1]:
            time.sleep(2.0)

    print(f"\n🎉 모든 주차 업데이트 완료!")


def main_monthly_point_ratio():
    """포인트 비중 데이터 월별 업데이트"""
    print(f"🚀 {TARGET_MONTH}월 포인트 비중 데이터 업데이트 시작...")
    print(f"📅 매핑: 7월=B열, 8월=C열, 9월=D열...")
    print(f"📍 대상: automation(포인트월비중) 시트 3행(첫구매이벤트), 4행(회원가입), 5행(친구추천)")
    print(f"🎯 타겟: {TARGET_YEAR}년 {TARGET_MONTH}월")
    print(f"📊 비중 형태: 소수점 (예: 0.2117, 0.7216)")

    try:
        # 1. 포인트 비중 데이터 조회
        point_ratio_df = get_monthly_point_ratio_data()

        # 2. Google Sheets 업데이트
        update_monthly_point_ratio_sheets(point_ratio_df)

        # 3. 업데이트 검증
        verify_monthly_point_ratio_update()

        print(f"\n🎊 {TARGET_MONTH}월 포인트 비중 데이터 업데이트 완료!")
        print(f"✨ {chr(64 + 2 + (TARGET_MONTH - 7))}열에 데이터가 업데이트되었습니다.")

    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()


def update_multiple_months():
    """여러 월 일괄 업데이트"""
    global TARGET_MONTH

    # 사용자에게 월들 입력받기
    print("여러 월을 일괄 업데이트합니다.")
    months_input = input("업데이트할 월들을 쉼표로 구분해서 입력하세요 (예: 7,8,9): ").strip()

    try:
        target_months = [int(month.strip()) for month in months_input.split(',')]
        target_months.sort()  # 순서대로 정렬
    except:
        print("❌ 유효한 월 번호들을 입력하세요. (예: 7,8,9)")
        return

    print(f"📅 여러 월 일괄 업데이트 시작: {target_months}")

    for month in target_months:
        print(f"\n{'=' * 50}")
        TARGET_MONTH = month  # 전역 변수 업데이트
        print(f"🎯 {month}월 업데이트 중...")

        try:
            # 포인트 비중 데이터 조회
            point_ratio_df = get_monthly_point_ratio_data()

            # Google Sheets 업데이트
            update_monthly_point_ratio_sheets(point_ratio_df)

            print(f"✅ {month}월 완료!")

        except Exception as e:
            print(f"❌ {month}월 실패: {str(e)}")

        # 다음 월 처리 전 잠시 대기
        if month != target_months[-1]:
            time.sleep(2.0)

    print(f"\n🎉 모든 월 업데이트 완료!")

if __name__ == "__main__":
    print("🔄 업데이트 타입을 선택하세요:")
    print("="*50)
    print("📅 주차별 포인트 비중:")
    print("1. 단일 주차 업데이트 (현재 설정: {}주차)".format(TARGET_WEEK))
    print("2. 여러 주차 일괄 업데이트")
    print("3. 주차 변경 후 단일 업데이트")
    print("="*50)
    print("📅 월별 포인트 비중:")
    print("4. 단일 월 업데이트 (현재 설정: {}월)".format(TARGET_MONTH))
    print("5. 여러 월 일괄 업데이트")
    print("6. 월 변경 후 단일 업데이트")

    choice = input("선택 (1-6): ").strip()

    if choice == "1":
        main_point_ratio()
    elif choice == "2":
        update_multiple_weeks()
    elif choice == "3":
        new_week = input(f"새로운 주차를 입력하세요 (현재: {TARGET_WEEK}): ").strip()
        if new_week.isdigit():
            TARGET_WEEK = int(new_week)
            print(f"✅ 주차가 {TARGET_WEEK}주차로 변경되었습니다.")
            main_point_ratio()
        else:
            print("❌ 유효한 주차 번호를 입력하세요.")
    elif choice == "4":
        main_monthly_point_ratio()
    elif choice == "5":
        update_multiple_months()
    elif choice == "6":
        new_month = input(f"새로운 월을 입력하세요 (현재: {TARGET_MONTH}): ").strip()
        if new_month.isdigit():
            TARGET_MONTH = int(new_month)
            print(f"✅ 월이 {TARGET_MONTH}월로 변경되었습니다.")
            main_monthly_point_ratio()
        else:
            print("❌ 유효한 월 번호를 입력하세요.")
    else:
        print("❌ 잘못된 선택입니다. 1-6 중에서 선택하세요.")

