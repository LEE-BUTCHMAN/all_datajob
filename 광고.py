import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import time

# pandasql 라이브러리 로드
try:
    import pandasql as ps
except ImportError:
    print("pandasql이 설치되지 않았습니다. pip install pandasql 실행하세요.")
    ps = None


def get_new_headers():
    """새로운 헤더 정의 (date 컬럼 + 카테고리별 지표)"""
    headers = ['date']  # 첫 번째 컬럼

    # 카테고리 정의
    categories = [
        ('합계', ['노출', '클릭', '클릭률', '광고비', 'CPC', '전환', '매출', '회원가입', 'ROAS']),
        ('파워링크', ['노출', '클릭', '클릭률', '광고비', 'CPC', '전환', '매출', '회원가입', 'ROAS']),
        ('브랜드검색', ['노출', '클릭', '클릭률', '광고비', 'CPC', '전환', '매출', '회원가입', 'ROAS']),
        ('GFA', ['노출', '클릭', '클릭률', '광고비', 'CPC', '전환', '매출', '회원가입', 'ROAS']),
        ('Meta', ['노출', '클릭', '클릭률', '광고비', 'CPC', '전환', '매출', '회원가입', 'ROAS']),
        ('구글키워드', ['노출', '클릭', '클릭률', '광고비', 'CPC', '전환', '매출', '회원가입', 'ROAS']),
        ('구글da', ['노출', '클릭', '클릭률', '광고비', 'CPC', '전환', '매출', '회원가입', 'ROAS']),
        ('크리테오', ['노출', '클릭', '클릭률', '광고비', 'CPC', '전환', '매출', '회원가입', 'ROAS']),
        ('당근', ['노출', '클릭', '클릭률', '광고비', 'CPC', '전환', '매출', '회원가입', '전환비용(1인)', 'ROAS']),
        ('모비온', ['노출', '클릭', '클릭률', '광고비', 'CPC', '전환', '매출', '회원가입', 'ROAS'])
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
    source_sheet_id = '1FHHdO4u2zgfH5lNZmf9Uzy8RsLYppwAP0h0Hg6YgtG0'  # 원본
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


def query_ad_data(df, sql_query):
    """DataFrame에 SQL 쿼리 실행"""

    if df is None:
        print("❌ DataFrame이 없습니다. 먼저 load_ad_data_to_dataframe()를 실행하세요.")
        return None

    if ps is None:
        print("❌ pandasql이 설치되지 않았습니다. pip install pandasql 실행하세요.")
        return None

    try:
        # SQL 쿼리 실행 (ad_df로 테이블명 지정)
        result = ps.sqldf(sql_query, {'ad_df': df})
        return result
    except Exception as e:
        print(f"❌ SQL 쿼리 오류: {str(e)}")
        return None


def example_queries(df):
    """예시 쿼리들 실행"""

    print("=== 예시 SQL 쿼리 실행 ===")

    # 예시 1: 전체 데이터 10개 조회
    print("\n1. 전체 데이터 10개 조회:")
    result1 = query_ad_data(df, "SELECT * FROM ad_df LIMIT 10")
    if result1 is not None:
        print(result1)

    # 예시 2: 날짜별 합계 광고비 조회
    print("\n2. 날짜별 합계 광고비 상위 5개:")
    result2 = query_ad_data(df, """
                                SELECT date, 합계_광고비
                                FROM ad_df
                                WHERE 합계_광고비 > 0
                                ORDER BY 합계_광고비 DESC
                                    LIMIT 5
                                """)
    if result2 is not None:
        print(result2)

    # 예시 3: 특정 날짜 범위 조회
    print("\n3. 2023-07-01 ~ 2023-07-05 데이터:")
    result3 = query_ad_data(df, """
                                SELECT date, 합계_노출, 합계_클릭, 합계_광고비, 합계_ROAS
                                FROM ad_df
                                WHERE date BETWEEN '2023-07-01' AND '2023-07-05'
                                ORDER BY date
                                """)
    if result3 is not None:
        print(result3)

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
            target_worksheet.update('A1', all_data)
            print("✅ 데이터 업로드 완료!")

        return True

    except Exception as e:
    print(f"❌ 오류 발생: {str(e)}")
    import traceback
    traceback.print_exc()
    return False


if __name__ == "__main__":
    # 1단계: RAW → 광고 시트 복사
    print("=== 1단계: RAW → 광고 시트 복사 ===")
    success = copy_raw_to_ad_sheet()

    if success:
        print("🎉 복사 완료!")

        # 2단계: DataFrame으로 로드
        print("\n=== 2단계: DataFrame 로드 ===")
        ad_df = load_ad_data_to_dataframe()

        if ad_df is not None:
            # 3단계: 예시 쿼리 실행
            print("\n=== 3단계: 예시 SQL 쿼리 ===")
            example_queries(ad_df)

            # 사용자 정의 쿼리 예시
            print("\n=== 사용자 정의 쿼리 예시 ===")
            custom_query = "SELECT * FROM ad_df LIMIT 10"
            print(f"실행 쿼리: {custom_query}")
            result = query_ad_data(ad_df, custom_query)
            if result is not None:
                print(result)
        else:
            print("❌ DataFrame 로드 실패!")
    else:
        print("❌ 복사 실패!")