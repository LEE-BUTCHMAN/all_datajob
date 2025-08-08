import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import time
from datetime import datetime, timedelta

# 업데이트할 주차 범위 설정
START_WEEK = 2  # 시작 주차 (1주차는 연도 변경 구간이라 제외)
END_WEEK = 32  # 종료 주차

# 업데이트할 월 범위 설정
START_MONTH = 1  # 시작 월 (1월)
END_MONTH = 8  # 종료 월 (8월)


def get_all_week_dates():
    """1주차부터 32주차까지 모든 주차의 날짜 범위 반환"""
    week_dates = {}

    # 2024년 12월 30일 월요일이 1주차 시작
    start_date = datetime(2024, 12, 30)

    for week in range(1, 33):  # 1주차부터 32주차까지
        week_start = start_date + timedelta(weeks=(week - 1))
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
    """광고 데이터를 automation(주문) 시트에 2~32주차 일괄 업데이트 (배치 업데이트)"""
    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 시트 열기
    sheet = client.open_by_key('1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE')
    worksheet = sheet.worksheet('automation(매출)')

    print(f"\n=== 광고 데이터 일괄 업데이트 (2주차~32주차) ===")
    print("(1주차는 연도 변경 구간이라 제외)")

    # 모든 주차 날짜 가져오기
    all_week_dates = get_all_week_dates()

    # 광고 데이터 항목별 행 번호
    ad_data_rows = {
        'total_signups': 7,  # 회원가입수
        'total_cost': 19,  # 광고비
        'total_conversions': 25,  # 매출전환수
        'avg_roas': 27  # ROAS
    }

    # 배치 업데이트를 위한 데이터 준비
    batch_update_data = []

    # 2주차부터 32주차까지 순회 (1주차 제외)
    for target_week in range(START_WEEK, END_WEEK + 1):
        # 주차별 열 매핑: 1주차=B열(2), 2주차=C열(3), ... 32주차=AG열(33)
        target_col = target_week + 1  # 1주차는 B열(2)

        col_letter = chr(64 + target_col) if target_col <= 26 else 'A' + chr(64 + target_col - 26)

        week_start, week_end = all_week_dates.get(target_week, (None, None))
        if week_start is None:
            continue

        # 해당 주차 데이터 필터링
        target_week_data = df[(df['date'] >= week_start) & (df['date'] <= week_end)]

        if target_week_data.empty:
            continue

        # 각 지표별 합계 계산
        total_signups_sum = int(target_week_data['total_signups'].sum())
        total_cost_sum = int(target_week_data['total_cost'].sum())
        total_conversions_sum = int(target_week_data['total_conversions'].sum())
        avg_roas = int(target_week_data['total_roas'].mean()) if len(target_week_data) > 0 else 0

        # 배치 업데이트 데이터 추가
        batch_update_data.append({
            'range': f"{col_letter}{ad_data_rows['total_signups']}",
            'values': [[total_signups_sum]]
        })
        batch_update_data.append({
            'range': f"{col_letter}{ad_data_rows['total_cost']}",
            'values': [[total_cost_sum]]
        })
        batch_update_data.append({
            'range': f"{col_letter}{ad_data_rows['total_conversions']}",
            'values': [[total_conversions_sum]]
        })
        batch_update_data.append({
            'range': f"{col_letter}{ad_data_rows['avg_roas']}",
            'values': [[avg_roas]]
        })

    # 배치 업데이트 실행
    if batch_update_data:
        print(f"📊 총 {len(batch_update_data)}개 광고 데이터 셀 업데이트 준비 완료")
        print("⏳ 배치 업데이트 실행 중...")

        try:
            # batch_update를 사용하여 한 번에 업데이트
            worksheet.batch_update(batch_update_data)
            print(f"✅ 광고 데이터 배치 업데이트 성공! {len(batch_update_data)}개 셀 업데이트 완료")
        except Exception as e:
            print(f"❌ 배치 업데이트 실패: {str(e)}")
            print("대체 방법으로 개별 업데이트 시도...")

            # 실패 시 개별 업데이트 (속도 조절)
            for i, data in enumerate(batch_update_data):
                try:
                    worksheet.update(data['range'], data['values'])
                    time.sleep(2)  # API 제한 회피를 위해 2초 대기
                    if (i + 1) % 10 == 0:
                        print(f"  진행: {i + 1}/{len(batch_update_data)} 완료")
                except Exception as update_error:
                    print(f"  ❌ {data['range']} 업데이트 실패: {str(update_error)}")

    print(f"\n🎉 광고 데이터 전체 주차 업데이트 완료!")


def update_ad_data_to_sheets_monthly(ad_df):
    """광고 데이터 월별 업데이트"""
    print("=== 광고 데이터 월별 업데이트 ===")

    # date 컬럼을 datetime으로 변환하고 month 추출
    ad_df['date'] = pd.to_datetime(ad_df['date'])
    ad_df['month'] = ad_df['date'].dt.month
    ad_df['year'] = ad_df['date'].dt.year

    # 2025년 데이터만 필터링
    ad_df = ad_df[ad_df['year'] == 2025]

    # Google Sheets 연결
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key('1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE')
    ws = sheet.worksheet('automation(매출월기준)')

    # 업데이트할 데이터 준비
    batch_updates = []

    # 1월부터 8월까지 반복
    for month in range(1, 9):
        target_col = month + 1  # B열(2)부터 시작

        # 해당 월의 데이터 필터링
        monthly_data = ad_df[ad_df['month'] == month]

        if not monthly_data.empty:
            # 7행: total_signups (AD/회원가입수)
            signups = monthly_data['total_signups'].sum()
            batch_updates.append({'range': f'{chr(64 + target_col)}7', 'values': [[int(signups)]]})

            # 19행: total_cost (광고비)
            cost = monthly_data['total_cost'].sum()
            batch_updates.append({'range': f'{chr(64 + target_col)}19', 'values': [[int(cost)]]})

            # 25행: total_conversions (매출전환수)
            conversions = monthly_data['total_conversions'].sum()
            batch_updates.append({'range': f'{chr(64 + target_col)}25', 'values': [[int(conversions)]]})

            # 27행: avg_roas (ROAS)
            avg_roas = int(monthly_data['total_roas'].mean()) if len(monthly_data) > 0 else 0
            batch_updates.append({'range': f'{chr(64 + target_col)}27', 'values': [[avg_roas]]})

    # 배치 업데이트 실행
    if batch_updates:
        print(f"📊 총 {len(batch_updates)}개 광고 데이터 셀 업데이트 준비 완료")
        print("⏳ 배치 업데이트 실행 중...")

        ws.batch_update(batch_updates)
        print(f"✅ 광고 데이터 배치 업데이트 성공! {len(batch_updates)}개 셀 업데이트 완료")
    else:
        print("❌ 업데이트할 광고 데이터가 없습니다.")

    print("🎉 광고 데이터 월별 업데이트 완료!")


if __name__ == "__main__":
    print("🔄 업데이트 타입을 선택하세요:")
    print("1. 주차별 일괄 업데이트 (2~32주차)")
    print("2. 월별 일괄 업데이트 (1~8월)")
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
                # 월별 일괄 업데이트만
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