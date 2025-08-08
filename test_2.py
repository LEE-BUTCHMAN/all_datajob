import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import time
from datetime import datetime, timedelta

# 업데이트할 주차 범위 설정
START_WEEK = 2  # 시작 주차
END_WEEK = 32  # 종료 주차

# 업데이트할 월 설정
TARGET_MONTH = 8  # 8월 데이터 업데이트


def get_all_week_dates():
    """2주차부터 32주차까지 모든 주차의 날짜 범위 반환"""
    week_dates = {}

    # 2025년 1월 6일 월요일이 2주차 시작
    start_date = datetime(2025, 1, 6)

    for week in range(2, 33):  # 2주차부터 32주차까지
        week_start = start_date + timedelta(weeks=(week - 2))
        week_end = week_start + timedelta(days=6)
        week_dates[week] = (
            week_start.strftime('%Y-%m-%d'),
            week_end.strftime('%Y-%m-%d')
        )

    return week_dates


def get_new_headers():
    """새로운 헤더 정의 (date 컬럼 + 카테고리별 지표) - 영어 버전"""
    headers = ['date']  # 첫 번째 컬럼

    # 카테고리 정의 - 영어로 변경
    categories = [
        ('total', ['impressions', 'clicks', 'ctr', 'cost', 'cpc', 'conversions', 'revenue', 'signups', 'cpa', 'roas']),
        ('powerlink', ['impressions', 'clicks', 'ctr', 'cost', 'cpc', 'conversions', 'revenue', 'signups', 'roas']),
        ('brandsearch', ['impressions', 'clicks', 'ctr', 'cost', 'cpc', 'conversions', 'revenue', 'signups', 'roas']),
        ('gfa', ['impressions', 'clicks', 'ctr', 'cost', 'cpc', 'conversions', 'revenue', 'signups', 'roas']),
        ('meta', ['impressions', 'clicks', 'ctr', 'cost', 'cpc', 'conversions', 'revenue', 'signups', 'roas']),
        ('google_keyword',
         ['impressions', 'clicks', 'ctr', 'cost', 'cpc', 'conversions', 'revenue', 'signups', 'roas']),
        ('google_da', ['impressions', 'clicks', 'ctr', 'cost', 'cpc', 'conversions', 'revenue', 'signups', 'roas']),
        ('criteo', ['impressions', 'clicks', 'ctr', 'cost', 'cpc', 'conversions', 'revenue', 'signups', 'roas']),
        ('daangn', ['impressions', 'clicks', 'ctr', 'cost', 'cpc', 'conversions', 'revenue', 'signups', 'roas']),
        ('mobion', ['impressions', 'clicks', 'ctr', 'cost', 'cpc', 'conversions', 'revenue', 'signups', 'roas'])
    ]

    # 각 카테고리별로 컬럼 생성
    for category, metrics in categories:
        for metric in metrics:
            headers.append(f'{category}_{metric}')

    return headers


def copy_raw_to_ad_sheet():
    """RAW 시트 데이터를 광고 시트에 그대로 복사"""

    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 시트 ID
    source_sheet_id = '1JdJ6GR71mXYAmS9gBHM28xsCWAvLrxYEdU97yK1-VIk'  # 원본
    target_sheet_id = '1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE'  # 타겟

    try:
        print("=== RAW → 광고 시트 복사 시작 ===")

        # 1. 원본 시트 열기
        print("원본 시트 연결 중...")
        source_spreadsheet = client.open_by_key(source_sheet_id)

        # 워크시트 목록 확인
        worksheets = source_spreadsheet.worksheets()
        print(f"사용 가능한 워크시트: {[ws.title for ws in worksheets]}")

        # RAW 워크시트 찾기
        source_worksheet = None
        for ws in worksheets:
            if 'RAW' in ws.title.upper():
                source_worksheet = ws
                break

        if source_worksheet is None:
            print("❌ RAW 워크시트를 찾을 수 없습니다.")
            return False

        print(f"✅ '{source_worksheet.title}' 워크시트 연결 성공")

        # 2. 원본 데이터 읽기
        print("원본 데이터 읽는 중...")
        source_data = source_worksheet.get_all_values()
        print(f"읽은 데이터: {len(source_data)}행")

        if not source_data:
            print("❌ 원본 시트에 데이터가 없습니다.")
            return False

        # 3. 타겟 시트 열기
        print("타겟 시트 연결 중...")
        target_spreadsheet = client.open_by_key(target_sheet_id)
        target_worksheet = target_spreadsheet.worksheet('광고')
        print("✅ 광고 시트 연결 성공")

        # 4. 타겟 시트 클리어
        print("기존 데이터 삭제 중...")
        target_worksheet.clear()

        # 5. 새 헤더 생성
        new_headers = get_new_headers()
        print(f"새 헤더 생성: {len(new_headers)}개 컬럼")

        # 6. 데이터 복사 준비
        print("데이터 처리 중...")

        # 새 헤더 + 원본 시트의 4행부터 끝까지 데이터
        all_data = [new_headers]

        # 원본 데이터에서 4행부터 끝까지 가져오기 (인덱스 3부터)
        data_rows = source_data[3:] if len(source_data) > 3 else []

        for row in data_rows:
            # 빈 행 건너뛰기
            if not row or all(cell.strip() == '' for cell in row if cell):
                continue

            # #VALUE! 값들을 0으로 바꾸기
            processed_row = []
            for cell in row:
                if cell == '#VALUE!' or str(cell).upper() == '#VALUE!':
                    processed_row.append('0')
                else:
                    processed_row.append(cell)

            # 새 헤더 길이에 맞춰 데이터 조정
            if len(processed_row) < len(new_headers):
                # 부족한 컬럼은 빈 문자열로 채움
                processed_row.extend([''] * (len(new_headers) - len(processed_row)))
            elif len(processed_row) > len(new_headers):
                # 초과하는 컬럼은 잘라냄
                processed_row = processed_row[:len(new_headers)]

            all_data.append(processed_row)

        print(f"처리된 데이터: {len(all_data) - 1}행 (헤더 제외)")
        print(f"새 헤더: {new_headers[:5]}... (총 {len(new_headers)}개)")

        # 7. 타겟 시트 클리어 후 데이터 업로드
        print("기존 데이터 삭제 후 새 데이터 업로드 중...")
        if all_data:
            # 기존 데이터 완전 삭제
            target_worksheet.clear()
            time.sleep(1)  # API 제한 방지

            # 새 데이터 업로드
            target_worksheet.update(values=all_data, range_name='A1', value_input_option='RAW')
            print("✅ 데이터 업로드 완료!")

        return True

    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def load_ad_data_to_dataframe():
    """광고 시트 데이터를 DataFrame으로 로드"""

    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 타겟 시트 ID
    target_sheet_id = '1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE'

    try:
        print("=== 광고 시트에서 DataFrame 로드 ===")

        # 광고 시트 열기
        spreadsheet = client.open_by_key(target_sheet_id)
        worksheet = spreadsheet.worksheet('광고')

        # 모든 데이터 읽기
        all_values = worksheet.get_all_values()

        if len(all_values) < 2:
            print("❌ 광고 시트에 충분한 데이터가 없습니다.")
            return None

        # DataFrame 생성 (첫 번째 행을 헤더로 사용)
        headers = all_values[0]
        data = all_values[1:]

        df = pd.DataFrame(data, columns=headers)

        # #VALUE! 값들을 0으로 변환
        df = df.replace('#VALUE!', '0')
        df = df.replace('#VALUE', '0')

        # 빈 문자열을 NaN으로 변환 후 0으로 채우기 (숫자 컬럼의 경우)
        for col in df.columns:
            if col != 'date':  # date 컬럼 제외
                df[col] = df[col].replace('', '0')
                # 쉼표 제거 후 숫자로 변환
                df[col] = df[col].astype(str).str.replace(',', '', regex=False)
                df[col] = df[col].str.replace('%', '', regex=False)
                # 숫자로 변환 시도
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                except:
                    pass

        print(f"✅ DataFrame 로드 완료: {len(df)}행 × {len(df.columns)}열")
        print(f"컬럼명: {list(df.columns[:5])}... (총 {len(df.columns)}개)")

        return df

    except Exception as e:
        print(f"❌ DataFrame 로드 오류: {str(e)}")
        return None


def update_ad_data_to_sheets_all_weeks(df):
    """광고 데이터를 automation(주문) 시트에 2~32주차 일괄 업데이트"""
    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 시트 열기
    sheet = client.open_by_key('1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE')
    worksheet = sheet.worksheet('automation(주문)')

    print(f"\n=== 광고 데이터 일괄 업데이트 (2주차~32주차) ===")

    # 모든 주차 날짜 가져오기
    all_week_dates = get_all_week_dates()

    # 광고 데이터 항목별 행 번호 (수정됨!)
    ad_data_rows = {
        'total_cost': 19,  # 광고비
        'total_conversions': 25,  # 매출전환수
        'avg_roas': 27  # ROAS
    }

    update_summary = []

    # 2주차부터 32주차까지 순회
    for target_week in range(START_WEEK, END_WEEK + 1):
        # 주차별 열 매핑: 2주차=C열(3), 3주차=D열(4), ... 32주차=AG열(33)
        target_col = target_week + 1  # 2주차부터 시작하여 C열부터 매핑

        col_name = chr(64 + target_col) if target_col <= 26 else 'A' + chr(64 + target_col - 26)
        print(f"\n📊 {target_week}주차를 {col_name}열에 업데이트 중...")

        week_start, week_end = all_week_dates.get(target_week, (None, None))
        if week_start is None:
            print(f"  ⚠️ {target_week}주차 날짜 정보가 없습니다.")
            continue

        # 해당 주차 데이터 필터링
        target_week_data = df[(df['date'] >= week_start) & (df['date'] <= week_end)]

        if target_week_data.empty:
            print(f"  ⚠️ {target_week}주차 ({week_start} ~ {week_end}) 데이터가 없습니다.")
            continue

        print(f"  📊 {target_week}주차 데이터 {len(target_week_data)}개 발견")

        # 각 지표별 합계 계산
        total_cost_sum = int(target_week_data['total_cost'].sum())
        total_conversions_sum = int(target_week_data['total_conversions'].sum())
        avg_roas = int(target_week_data['total_roas'].mean()) if len(target_week_data) > 0 else 0

        # 각 항목별로 해당 행에 데이터 입력
        try:
            update_count = 0

            # total_cost → 19행
            worksheet.update_cell(ad_data_rows['total_cost'], target_col, total_cost_sum)
            time.sleep(0.5)
            print(f"  ✅ 광고비: 행{ad_data_rows['total_cost']} = {total_cost_sum:,}")
            update_count += 1

            # total_conversions → 25행
            worksheet.update_cell(ad_data_rows['total_conversions'], target_col, total_conversions_sum)
            time.sleep(0.5)
            print(f"  ✅ 매출전환수: 행{ad_data_rows['total_conversions']} = {total_conversions_sum:,}")
            update_count += 1

            # avg_roas → 27행
            worksheet.update_cell(ad_data_rows['avg_roas'], target_col, avg_roas)
            time.sleep(0.5)
            print(f"  ✅ ROAS: 행{ad_data_rows['avg_roas']} = {avg_roas:,}")
            update_count += 1

            update_summary.append(f"{target_week}주차: {update_count}개 항목")

        except Exception as e:
            print(f"  ❌ {target_week}주차 업데이트 오류: {str(e)}")

    print(f"\n🎉 광고 데이터 전체 주차 업데이트 완료!")
    print(f"📊 업데이트 요약: {', '.join(update_summary)}")


def update_ad_data_to_sheets_monthly(df):
    """광고 데이터를 automation(매출월기준) 시트에 월별 업데이트"""
    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 시트 열기
    sheet = client.open_by_key('1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE')
    worksheet = sheet.worksheet('automation(매출월기준)')

    print(f"\n=== 광고 데이터 월별 업데이트 ===")

    # 월별 열 매핑: 7월=B열(2), 8월=C열(3), 9월=D열(4)...
    target_month = TARGET_MONTH
    target_col = 2 + (target_month - 7)  # 7월부터 시작하여 B열부터 매핑

    print(f"광고 데이터 {target_month}월을 {chr(64 + target_col)}열에 업데이트합니다.")

    # 해당 월 데이터 필터링 (2025년 기준)
    month_start = f'2025-{target_month:02d}-01'
    if target_month == 12:
        month_end = f'2025-12-31'
    else:
        # 다음 달 1일에서 하루 빼기
        next_month = datetime(2025, target_month + 1, 1) - timedelta(days=1)
        month_end = next_month.strftime('%Y-%m-%d')

    # 해당 월 데이터 필터링
    target_month_data = df[(df['date'] >= month_start) & (df['date'] <= month_end)]

    if target_month_data.empty:
        print(f"광고 {target_month}월 ({month_start} ~ {month_end}) 데이터가 없습니다.")
        return

    print(f"광고 {target_month}월 데이터 {len(target_month_data)}개 발견")

    # 각 지표별 합계 및 평균 계산
    total_cost_sum = int(target_month_data['total_cost'].sum())
    total_signups_sum = int(target_month_data['total_signups'].sum())
    total_conversions_sum = int(target_month_data['total_conversions'].sum())
    avg_roas = int(target_month_data['total_roas'].mean())

    print(f"월별 합계 계산 완료:")
    print(f"  total_cost: {total_cost_sum:,}")
    print(f"  total_signups: {total_signups_sum:,}")
    print(f"  total_conversions: {total_conversions_sum:,}")
    print(f"  avg_roas: {avg_roas:,}")

    # 광고 데이터 항목별 행 번호 (수정됨!)
    ad_data_rows = {
        'total_cost': 19,  # 광고비
        'total_conversions': 25,  # 매출전환수
        'avg_roas': 27  # ROAS
    }

    # 각 항목별로 해당 행에 데이터 입력
    try:
        # total_cost → 19행
        worksheet.update_cell(ad_data_rows['total_cost'], target_col, total_cost_sum)
        time.sleep(1.0)
        print(f"  total_cost: 행{ad_data_rows['total_cost']}, 열{target_col} = {total_cost_sum:,}")

        # total_signups → 18행
        worksheet.update_cell(ad_data_rows['total_signups'], target_col, total_signups_sum)
        time.sleep(1.0)
        print(f"  total_signups: 행{ad_data_rows['total_signups']}, 열{target_col} = {total_signups_sum:,}")

        # total_conversions → 25행
        worksheet.update_cell(ad_data_rows['total_conversions'], target_col, total_conversions_sum)
        time.sleep(1.0)
        print(f"  total_conversions: 행{ad_data_rows['total_conversions']}, 열{target_col} = {total_conversions_sum:,}")

        # avg_roas → 27행
        worksheet.update_cell(ad_data_rows['avg_roas'], target_col, avg_roas)
        time.sleep(1.0)
        print(f"  avg_roas: 행{ad_data_rows['avg_roas']}, 열{target_col} = {avg_roas:,}")

        print(f"광고 데이터 {target_month}월 완료!")

    except Exception as e:
        print(f"❌ 월별 시트 업데이트 오류: {str(e)}")


if __name__ == "__main__":
    print("🔄 업데이트 타입을 선택하세요:")
    print("1. 주차별 일괄 업데이트 (2~32주차)")
    print("2. 월별 업데이트")
    print("3. 모두 업데이트")

    choice = input("선택 (1, 2, 또는 3): ").strip()

    # RAW → 광고 시트 복사
    success = copy_raw_to_ad_sheet()

    if success:
        # DataFrame으로 로드
        ad_df = load_ad_data_to_dataframe()

        if ad_df is not None:
            if choice == "1":
                # 주차별 일괄 업데이트만
                update_ad_data_to_sheets_all_weeks(ad_df)
            elif choice == "2":
                # 월별 업데이트만
                update_ad_data_to_sheets_monthly(ad_df)
            elif choice == "3":
                # 둘 다 업데이트
                update_ad_data_to_sheets_all_weeks(ad_df)
                update_ad_data_to_sheets_monthly(ad_df)
            else:
                print("❌ 잘못된 선택입니다.")
        else:
            print("❌ DataFrame 로드 실패!")
    else:
        print("❌ 복사 실패!")