import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np
from datetime import datetime
import warnings
import time

# 경고 메시지 숨기기
warnings.filterwarnings('ignore')

# pandas 출력 옵션 설정 (모든 컬럼 표시, 오른쪽 정렬)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.colheader_justify', 'right')  # 컬럼명 오른쪽 정렬

# pandasql 라이브러리 로드
try:
    import pandasql as ps
except ImportError:
    print("pandasql이 설치되지 않았습니다. pip install pandasql 실행하세요.")
    ps = None

# 전역 변수 초기화
df = None


# === 시트간 복사 기능 (새로 추가) ===
def copy_raw_sheet_data():
    """25-7월리포트 시트의 특정 범위 데이터를 대상 시트의 RAW로 복사"""

    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 시트 ID 정의
    source_sheet_id = '13xym1cjOer4txV82qjq4PKKT1FX2hvkFGE2gfraJM6k'  # 원본 시트
    target_sheet_id = '1gQElxBoy8Kxxbn4Klgu4qJLoQNaKPB426h5EKES_m8k'  # 중간 복사 대상 시트 (raw에 복사)

    try:
        print("25-7월리포트 시트에서 데이터 복사 시작...")

        # 1. 원본 시트 열기
        source_spreadsheet = client.open_by_key(source_sheet_id)
        source_worksheet = source_spreadsheet.worksheet('25-7월리포트')

        # 2. 원본 데이터 읽기 (B4:DB34 범위만 - A열 제외!)
        source_data = source_worksheet.get('B4:DB34')
        print(f"원본 데이터 읽기 완료: {len(source_data)}행")

        # 3. 대상 시트 열기
        target_spreadsheet = client.open_by_key(target_sheet_id)
        target_worksheet = target_spreadsheet.worksheet('raw')

        # 4. 대상 시트 기존 데이터 클리어
        target_worksheet.clear()

        # 5. 새 헤더 추가
        new_headers = get_new_headers()

        # 6. 헤더와 데이터 결합
        all_data = [new_headers] + source_data

        # 7. 새 데이터 일괄 업데이트
        if all_data:
            # 데이터가 많으면 배치로 나누어서 업데이트
            batch_size = 1000  # 한 번에 1000행씩

            for i in range(0, len(all_data), batch_size):
                batch_data = all_data[i:i + batch_size]
                start_row = i + 1
                end_row = i + len(batch_data)

                # 범위 계산 (A1:DB까지)
                end_col_letter = get_column_letter(len(batch_data[0]))
                range_name = f'A{start_row}:{end_col_letter}{end_row}'

                target_worksheet.update(range_name, batch_data)

                # API 제한 방지를 위한 지연
                time.sleep(1)

        # 8. 특정 열에 합계 데이터 추가 기록
        print("특정 열에 합계 데이터 기록 중...")
        update_summary_columns(target_worksheet, all_data)

        print("데이터 복사 완료!")
        return True

    except gspread.exceptions.APIError as e:
        if "operation is not supported" in str(e).lower():
            print("❌ 원본 시트가 Excel 파일이거나 접근 권한이 없습니다.")
            print("해결방법:")
            print("1. 원본 시트를 Google Drive에서 열기")
            print("2. 파일 → Google Sheets로 저장")
            print("3. 변환된 시트에 서비스 계정 권한 부여")
        else:
            print(f"API 오류: {e}")
        return False
    except Exception as e:
        print(f"복사 오류 발생: {e}")
        return False


def update_weekly_data_to_sheet(weekly_result):
    """주차별 집계 결과를 구글 시트의 해당 주차 행에 업데이트"""

    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    try:
        # 최종 업데이트 시트 열기
        sheet_id = '1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE'  # 최종 업데이트 시트
        spreadsheet = client.open_by_key(sheet_id)
        worksheet = spreadsheet.get_worksheet(0)  # 첫 번째 워크시트

        # B열의 모든 값 읽기 (주차 정보가 있는 열)
        b_column_values = worksheet.col_values(2)  # B열 = 2번째 열

        # 디버깅: B열 값들 출력
        print(f"🔍 B열 값들 확인:")
        for i, val in enumerate(b_column_values):
            if val.strip():  # 빈 값이 아닌 경우만
                print(f"  {i + 1}행: '{val}'")

        # 주차별 집계 결과의 각 행 처리
        for idx, row in weekly_result.iterrows():
            year_week = row['year_week']  # 예: "2025년 29주차"

            # 주차 정보에서 숫자만 추출 (예: "29주차")
            if '주차' in year_week:
                week_part = year_week.split('년 ')[1]  # "29주차"
                week_num = week_part.replace('주차', '')  # "29"

                print(f"🔍 찾는 주차: '{year_week}' → 숫자: '{week_num}'")

                # B열에서 해당 주차를 찾기 (첫 번째 매칭만)
                target_row = None
                for i, cell_value in enumerate(b_column_values):
                    if cell_value and str(week_num) in str(cell_value):
                        print(f"✅ 매칭 발견: {i + 1}행 '{cell_value}'에서 '{week_num}' 찾음")
                        target_row = i + 1  # gspread는 1부터 시작
                        break  # 첫 번째 매칭만 사용

                if target_row:
                    print(f"📍 {year_week} 데이터를 {target_row}행에 업데이트 중...")

                    # 특정 4개 지표만 추출
                    광고비_값 = ''
                    회원가입_값 = ''
                    전환_값 = ''
                    roas_값 = ''

                    for col in weekly_result.columns:
                        if col == '합계_광고비':
                            광고비_값 = str(row[col]) if not pd.isna(row[col]) else ''
                        elif col == '합계_회원가입':
                            회원가입_값 = str(row[col]) if not pd.isna(row[col]) else ''
                        elif col == '합계_전환':
                            전환_값 = str(row[col]) if not pd.isna(row[col]) else ''
                        elif col == '합계_ROAS':
                            roas_값 = str(row[col]) if not pd.isna(row[col]) else ''

                    # 각 열에 개별 업데이트
                    if 광고비_값:
                        worksheet.update(f'W{target_row}', [[광고비_값]])
                        time.sleep(0.3)

                    if 회원가입_값:
                        worksheet.update(f'X{target_row}', [[회원가입_값]])
                        time.sleep(0.3)

                    if 전환_값:
                        worksheet.update(f'AA{target_row}', [[전환_값]])
                        time.sleep(0.3)

                    if roas_값:
                        worksheet.update(f'AC{target_row}', [[roas_값]])
                        time.sleep(0.3)

                    print(f"✅ {target_row}행 W/X/AA/AC열 업데이트 완료")
                else:
                    print(f"❌ {year_week}에 해당하는 행을 B열에서 찾을 수 없습니다.")

        print("🎉 모든 주차별 데이터 업데이트 완료!")

    except Exception as e:
        print(f"❌ 주차별 데이터 업데이트 오류: {e}")


def update_summary_columns(worksheet, all_data):
    """특정 열에 합계 데이터 기록 (W열: 합계광고비, X열: 합계회원가입, AA열: 합계전환, AC열: 합계ROAS)"""

    try:
        # 헤더에서 각 지표의 인덱스 찾기
        headers = all_data[0]

        # 각 지표의 인덱스 찾기
        광고비_idx = None
        회원가입_idx = None
        전환_idx = None
        roas_idx = None

        for i, header in enumerate(headers):
            if header == '합계_광고비':
                광고비_idx = i
            elif header == '합계_회원가입':
                회원가입_idx = i
            elif header == '합계_전환':
                전환_idx = i
            elif header == '합계_ROAS':
                roas_idx = i

        # 데이터 행들 (헤더 제외)
        data_rows = all_data[1:]

        # 각 열에 기록할 데이터 준비
        w_column_data = []  # W열: 합계 광고비
        x_column_data = []  # X열: 합계 회원가입
        aa_column_data = []  # AA열: 합계 전환
        ac_column_data = []  # AC열: 합계 ROAS

        # 헤더 행 추가
        w_column_data.append(['합계_광고비'])
        x_column_data.append(['합계_회원가입'])
        aa_column_data.append(['합계_전환'])
        ac_column_data.append(['합계_ROAS'])

        # 데이터 행들 처리
        for row in data_rows:
            # 각 지표 값 추출 (인덱스가 유효한 경우만)
            광고비_값 = row[광고비_idx] if 광고비_idx is not None and 광고비_idx < len(row) else ''
            회원가입_값 = row[회원가입_idx] if 회원가입_idx is not None and 회원가입_idx < len(row) else ''
            전환_값 = row[전환_idx] if 전환_idx is not None and 전환_idx < len(row) else ''
            roas_값 = row[roas_idx] if roas_idx is not None and roas_idx < len(row) else ''

            w_column_data.append([광고비_값])
            x_column_data.append([회원가입_값])
            aa_column_data.append([전환_값])
            ac_column_data.append([roas_값])

        # 각 열에 데이터 업데이트
        total_rows = len(w_column_data)

        # W열 업데이트 (합계 광고비)
        worksheet.update(f'W1:W{total_rows}', w_column_data)
        time.sleep(1)

        # X열 업데이트 (합계 회원가입)
        worksheet.update(f'X1:X{total_rows}', x_column_data)
        time.sleep(1)

        # AA열 업데이트 (합계 전환)
        worksheet.update(f'AA1:AA{total_rows}', aa_column_data)
        time.sleep(1)

        # AC열 업데이트 (합계 ROAS)
        worksheet.update(f'AC1:AC{total_rows}', ac_column_data)
        time.sleep(1)

        print("✅ W열(합계광고비), X열(합계회원가입), AA열(합계전환), AC열(합계ROAS) 업데이트 완료")

    except Exception as e:
        print(f"❌ 특정 열 업데이트 오류: {e}")


def add_week_column(source_data):
    """날짜 데이터를 기반으로 주차 정보를 B열에 교체"""
    processed_data = []

    for row in source_data:
        if len(row) > 1:
            date_str = row[0]  # A열의 날짜

            # 날짜에서 주차 계산
            week_info = calculate_week_from_date(date_str)

            # 새로운 행 생성: [날짜, 주차, C열부터...] - B열을 주차로 교체
            new_row = [date_str, week_info] + row[2:] if len(row) > 2 else [date_str, week_info]
            processed_data.append(new_row)
        else:
            processed_data.append(row)

    return processed_data


def calculate_week_from_date(date_str):
    """날짜 문자열에서 주차 정보 계산"""
    try:
        if not date_str or date_str.strip() == '':
            return ''

        # 날짜 파싱 시도
        from datetime import datetime

        # 여러 날짜 형식 시도
        date_formats = ['%Y-%m-%d', '%Y.%m.%d', '%m/%d/%Y', '%Y/%m/%d']

        parsed_date = None
        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(date_str.strip(), fmt)
                break
            except:
                continue

        if parsed_date:
            # ISO 주차 계산 (월요일 시작)
            year = parsed_date.year
            week_num = parsed_date.isocalendar()[1]
            return f"{week_num}"  # 예: "29"
        else:
            return ''

    except Exception as e:
        print(f"날짜 파싱 오류: {date_str} → {e}")
        return ''


def get_new_headers():
    """새로운 헤더 정의 (B열=날짜, C열부터 데이터)"""
    return [
        # B열부터 시작
        '날짜',
        # 합계 (C열부터)
        '합계_노출', '합계_클릭', '합계_클릭률', '합계_광고비', '합계_CPC', '합계_전환', '합계_매출', '합계_회원가입', '합계_6월회원가입', '합계_회원가입CPA',
        '합계_ROAS',
        # 네이버 파워링크
        '파워링크_노출', '파워링크_클릭', '파워링크_클릭률', '파워링크_광고비', '파워링크_CPC', '파워링크_전환', '파워링크_매출', '파워링크_회원가입', '파워링크_ROAS',
        # 네이버 브랜드검색
        '브랜드검색_노출', '브랜드검색_클릭', '브랜드검색_클릭률', '브랜드검색_광고비', '브랜드검색_CPC', '브랜드검색_전환', '브랜드검색_매출', '브랜드검색_회원가입',
        '브랜드검색_ROAS',
        # GFA
        'GFA_노출', 'GFA_클릭', 'GFA_클릭률', 'GFA_광고비', 'GFA_CPC', 'GFA_전환', 'GFA_매출', 'GFA_회원가입', 'GFA_ROAS',
        # Meta
        'Meta_노출', 'Meta_클릭', 'Meta_클릭률', 'Meta_광고비', 'Meta_CPC', 'Meta_전환', 'Meta_매출', 'Meta_회원가입', 'Meta_6월회원가입',
        'Meta_ROAS',
        # GDN
        'GDN_노출', 'GDN_클릭', 'GDN_클릭률', 'GDN_광고비', 'GDN_CPC', 'GDN_전환', 'GDN_매출', 'GDN_회원가입', 'GDN_ROAS',
        # 구글SA
        '구글SA_노출', '구글SA_클릭', '구글SA_클릭률', '구글SA_광고비', '구글SA_CPC', '구글SA_전환', '구글SA_매출', '구글SA_회원가입', '구글SA_ROAS',
        # 구글DA
        '구글DA_노출', '구글DA_클릭', '구글DA_클릭률', '구글DA_광고비', '구글DA_CPC', '구글DA_전환', '구글DA_매출', '구글DA_회원가입', '구글DA_ROAS',
        # 크리테오
        '크리테오_노출', '크리테오_클릭', '크리테오_클릭률', '크리테오_광고비', '크리테오_CPC', '크리테오_전환', '크리테오_매출', '크리테오_회원가입', '크리테오_ROAS',
        # 당근
        '당근_노출', '당근_클릭', '당근_클릭률', '당근_광고비', '당근_CPC', '당근_전환', '당근_매출', '당근_회원가입', '당근_회원가입CPA', '당근_ROAS',
        # 모비온
        '모비온_노출', '모비온_클릭', '모비온_클릭률', '모비온_광고비', '모비온_CPC', '모비온_전환', '모비온_매출', '모비온_회원가입', '모비온_회원가입CPA', '모비온_ROAS'
    ]


def get_column_letter(col_num):
    """숫자를 엑셀 컬럼 문자로 변환 (1->A, 26->Z, 27->AA)"""
    result = ""
    while col_num > 0:
        col_num -= 1
        result = chr(col_num % 26 + ord('A')) + result
        col_num //= 26
    return result


# === 시트간 복사 기능 끝 ===


def read_raw_sheet_data():
    """Google Sheets의 RAW 시트에서 데이터 읽어와서 SQL 쿼리 가능한 DataFrame으로 변환"""

    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 시트 ID에서 스프레드시트 열기 (중간 시트 - raw에서 데이터 읽기)
    sheet_id = '1gQElxBoy8Kxxbn4Klgu4qJLoQNaKPB426h5EKES_m8k'
    spreadsheet = client.open_by_key(sheet_id)

    try:
        # RAW 시트 선택
        worksheet = spreadsheet.worksheet('raw')

        # 데이터 읽기 - 전체 데이터
        all_values = worksheet.get_all_values()

        # DataFrame으로 변환 (첫 번째 행을 헤더로 사용)
        if len(all_values) > 0:
            df = pd.DataFrame(all_values[1:], columns=all_values[0])

            # 데이터 전처리 및 타입 변환
            df = preprocess_dataframe(df)

            return df
        else:
            print("RAW 시트에 데이터가 없습니다.")
            return None

    except gspread.exceptions.WorksheetNotFound:
        print("'raw' 시트를 찾을 수 없습니다.")
        worksheets = spreadsheet.worksheets()
        for i, ws in enumerate(worksheets):
            print(f"{i}: {ws.title}")
        return None

    except Exception as e:
        print(f"오류 발생: {e}")
        return None


def preprocess_dataframe(df):
    """DataFrame 전처리 및 데이터 타입 변환"""

    # 0. 새로운 컬럼명 설정 (B열=날짜, C열부터 데이터)
    proper_columns = get_new_headers()

    # 실제 컬럼 수에 맞게 조정
    if len(df.columns) <= len(proper_columns):
        df.columns = proper_columns[:len(df.columns)]
    else:
        # 정의된 컬럼 수만큼만 사용하고 나머지는 제거
        df = df.iloc[:, :len(proper_columns)]
        df.columns = proper_columns

    # 1. 빈 문자열을 NaN으로 변환
    df = df.replace('', np.nan)

    # 2. 완전히 빈 행 제거
    df = df.dropna(how='all')

    # 3. 각 컬럼별 데이터 타입 자동 감지 및 변환 (간소화)
    for col in df.columns:
        # 빈 값이 아닌 샘플 데이터 확인
        non_null_values = df[col].dropna()
        if len(non_null_values) == 0:
            continue

        # 숫자로 변환 시도
        if is_numeric_column(non_null_values):
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            # 문자열로 유지
            df[col] = df[col].astype(str)
            df[col] = df[col].replace('nan', np.nan)  # 'nan' 문자열을 다시 NaN으로

    return df


def is_numeric_column(series):
    """컬럼이 숫자 타입인지 확인"""
    try:
        # 샘플 몇 개를 숫자로 변환해보기
        sample = series.head(10)
        pd.to_numeric(sample, errors='raise')
        return True
    except:
        return False


def is_date_column(series):
    """컬럼이 날짜 타입인지 확인"""
    try:
        # 샘플 몇 개를 날짜로 변환해보기
        sample = series.head(10)
        pd.to_datetime(sample, errors='raise')
        return True
    except:
        return False


def query(sql_query):
    """SQL 쿼리를 실행하는 함수"""
    global df
    if df is None:
        print("데이터가 로드되지 않았습니다. 먼저 read_raw_sheet_data()를 실행하세요.")
        return None

    if ps is None:
        print("pandasql이 설치되지 않았습니다. pip install pandasql 실행 후 재시도하세요.")
        return None

    try:
        # pandasql에 df를 직접 전달
        result = ps.sqldf(sql_query, {'df': df})
        return result
    except Exception as e:
        print(f"SQL 쿼리 오류: {e}")
        return None


def generate_complete_weekly_query():
    """모든 컬럼을 포함한 완전한 주차별 집계 쿼리 생성 (월요일~일요일)"""

    # 날짜 제외한 모든 컬럼 리스트
    all_columns = [col for col in df.columns if col not in ['날짜']]

    # 평균을 구해야 하는 컬럼들 (비율/효율성 지표)
    avg_columns = ['클릭률', 'CPC', 'ROAS', '회원가입CPA']

    # 모든 컬럼에 대해 SUM 또는 AVG 절 생성
    sum_clauses = []
    for col in all_columns:
        # 컬럼명 안전하게 처리 (따옴표로 감싸기)
        safe_col = f'"{col}"'

        # 평균을 구해야 하는 컬럼인지 확인
        is_avg_column = any(avg_keyword in col for avg_keyword in avg_columns)

        if is_avg_column:
            # 평균 계산 (0으로 나누기 오류 방지, NaN을 0으로 변환)
            sum_clause = f'COALESCE(ROUND(AVG(CASE WHEN CAST(REPLACE(REPLACE(REPLACE({safe_col}, ",", ""), "#VALUE!", "0"), "-", "0") AS FLOAT) > 0 THEN CAST(REPLACE(REPLACE(REPLACE({safe_col}, ",", ""), "#VALUE!", "0"), "-", "0") AS FLOAT) ELSE NULL END), 2), 0) as "{col}"'
        else:
            # 합계 계산 (NaN을 0으로 변환)
            sum_clause = f'COALESCE(SUM(CAST(REPLACE(REPLACE(REPLACE({safe_col}, ",", ""), "#VALUE!", "0"), "-", "0") AS FLOAT)), 0) as "{col}"'

        sum_clauses.append(sum_clause)

    # 완전한 쿼리 생성 (월요일~일요일 주차)
    complete_query = f"""
SELECT 
    substr("날짜", 1, 4) || '년 ' || 
    CAST(strftime('%W', "날짜") + 1 AS INTEGER) || '주차' as year_week,
    {',\n    '.join(sum_clauses)}
FROM df 
WHERE "날짜" IS NOT NULL 
  AND "날짜" != 'None' 
  AND "날짜" NOT LIKE '%노출%'
  AND "날짜" NOT LIKE '%합계%'
  AND length("날짜") = 10
  AND "날짜" LIKE '20%'
  AND "날짜" >= '2025-07-14'
  AND "날짜" <= '2025-07-20'
GROUP BY 
    substr("날짜", 1, 4), 
    strftime('%W', "날짜")
ORDER BY 
    substr("날짜", 1, 4), 
    strftime('%W', "날짜")
"""

    return complete_query


if __name__ == "__main__":
    # 시트간 복사 실행
    print("=== 25-7월리포트 데이터 복사 ===")
    copy_success = copy_raw_sheet_data()

    if copy_success:
        print("✅ 복사 완료!\n")
    else:
        print("❌ 복사 실패\n")

    # 데이터 로드
    df = read_raw_sheet_data()

    if df is not None:
        print(f"데이터 로드 완료: {len(df)}행 × {len(df.columns)}열")

        # 디버깅: 첫 5행 데이터 확인
        print("\n🔍 첫 5행 데이터 확인:")
        print(df.head())
        print(f"\n🔍 날짜 컬럼 샘플 값들:")
        if '날짜' in df.columns:
            print(df['날짜'].head(10).tolist())
        else:
            print("❌ '날짜' 컬럼이 없습니다!")
            print(f"사용 가능한 컬럼들: {df.columns.tolist()}")

        # 주차별 집계 결과만 출력
        print("\n=== 주차별 집계 결과 ===")
        weekly_query = generate_complete_weekly_query()
        print(f"🔍 실행할 SQL 쿼리:\n{weekly_query}")
        weekly_result = query(weekly_query)
        if weekly_result is not None:
            # NaN을 0으로 변환하고 출력
            weekly_result = weekly_result.fillna(0)
            print(weekly_result)

            # 주차별 집계 결과를 구글 시트에 업데이트
            print("\n=== 주차별 데이터를 구글 시트에 업데이트 ===")
            update_weekly_data_to_sheet(weekly_result)
        else:
            print("주차별 집계 결과를 생성할 수 없습니다.")