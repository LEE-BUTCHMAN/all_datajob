import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np
from datetime import datetime
import warnings

# 경고 메시지 숨기기
warnings.filterwarnings('ignore')

# pandas 출력 옵션 설정 (모든 컬럼 표시)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# pandasql 라이브러리 로드
try:
    import pandasql as ps
except ImportError:
    print("pandasql이 설치되지 않았습니다. pip install pandasql 실행하세요.")
    ps = None

# 전역 변수 초기화
df = None


def read_raw_sheet_data():
    """Google Sheets의 RAW 시트에서 데이터 읽어와서 SQL 쿼리 가능한 DataFrame으로 변환"""

    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 시트 ID에서 스프레드시트 열기
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
        print("'RAW' 시트를 찾을 수 없습니다.")
        worksheets = spreadsheet.worksheets()
        for i, ws in enumerate(worksheets):
            print(f"{i}: {ws.title}")
        return None

    except Exception as e:
        print(f"오류 발생: {e}")
        return None


def preprocess_dataframe(df):
    """DataFrame 전처리 및 데이터 타입 변환"""

    # 0. 빈 컬럼명 처리
    new_columns = []
    for i, col in enumerate(df.columns):
        if col == '' or pd.isna(col) or str(col).strip() == '':
            new_columns.append(f'column_{i}')
        else:
            new_columns.append(str(col).strip())
    df.columns = new_columns

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


if __name__ == "__main__":
    # RAW 시트 데이터 읽기
    df = read_raw_sheet_data()

    if df is not None:
        print(f"데이터 로드 완료: {len(df)}행 × {len(df.columns)}열")

        # 첫 5행 출력 (모든 컬럼 표시)
        result = query('SELECT * FROM df LIMIT 5')
        if result is not None:
            print(result)