import gspread
from google.oauth2.service_account import Credentials
import time

# 모든 warning 무시
warnings.filterwarnings('ignore')

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

        # 첫 번째 행을 헤더로 교체하고 나머지 데이터는 그대로
        all_data = [new_headers]

        # 원본 데이터에서 헤더가 아닌 실제 데이터만 가져오기 (첫 행 제외)
        data_rows = source_data[1:] if len(source_data) > 1 else []

        for row in data_rows:
            # 빈 행 건너뛰기
            if not row or all(cell == '' for cell in row):
                continue

            # 새 헤더 길이에 맞춰 조정
            if len(row) < len(new_headers):
                # 부족한 컬럼은 빈 문자열로 채움
                row.extend([''] * (len(new_headers) - len(row)))
            elif len(row) > len(new_headers):
                # 초과하는 컬럼은 잘라냄
                row = row[:len(new_headers)]

            all_data.append(row)

        print(f"처리된 데이터: {len(all_data) - 1}행 (헤더 제외)")

        # 7. 데이터 업로드
        print("데이터 업로드 중...")
        if all_data:
            # 한 번에 업로드
            target_worksheet.update('A1', all_data)
            print("✅ 데이터 업로드 완료!")

        return True

    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = copy_raw_to_ad_sheet()
    if success:
        print("🎉 복사 완료!")
    else:
        print("❌ 복사 실패!")