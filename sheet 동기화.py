import os
import gspread
from google.oauth2.service_account import Credentials
from openpyxl import load_workbook

# OneDrive 경로 찾기
possible_paths = [
    '/Users/sfn/Library/CloudStorage/OneDrive-Personal/차별화상회모든수치완전자동화.xlsx',
    '/Users/sfn/Library/CloudStorage/OneDrive-SmartFoodnet/차별화상회모든수치완전자동화.xlsx',
    '/Users/sfn/OneDrive - Smart Foodnet/차별화상회모든수치완전자동화.xlsx',
    '/Users/sfn/OneDrive/차별화상회모든수치완전자동화.xlsx'
]

# 실제 파일 경로 찾기
EXCEL_PATH = None
for path in possible_paths:
    if os.path.exists(path):
        EXCEL_PATH = path
        print(f"✅ Excel 파일 찾음: {path}")
        break

if not EXCEL_PATH:
    print("❌ Excel 파일을 찾을 수 없습니다.")
    print("터미널에서 이 명령어 실행해보세요:")
    print("find ~ -name '차별화상회모든수치완전자동화.xlsx' 2>/dev/null")
    exit()

# Google Sheets 인증
scope = ['https://spreadsheets.google.com/feeds',
          'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file(
    '/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
    scopes=scope
)
gc = gspread.authorize(creds)

# Google Sheets 데이터 읽기
SHEET_KEY = '1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE'
sheet = gc.open_by_key(SHEET_KEY)
worksheet = sheet.worksheet('automation(포인트비중)')
all_data = worksheet.get_all_values()

# 31주차 열 찾기
week_row = all_data[1]  # 2번째 행
target_col = None

for idx, cell in enumerate(week_row):
    if '31' in str(cell):
        target_col = idx
        break

if target_col is None:
    print("31주차를 찾을 수 없습니다.")
    exit()

# 31주차 데이터만 추출
sync_data = []
for row in all_data:
    if len(row) > target_col:
        sync_data.append([row[0], row[target_col]])

# Excel 파일 열기
wb = load_workbook(EXCEL_PATH)

# test 시트가 없으면 생성
if 'test' not in wb.sheetnames:
    wb.create_sheet('test')

ws = wb['test']

# 기존 데이터 클리어
ws.delete_rows(1, ws.max_row)

# 데이터 쓰기
for row_idx, row_data in enumerate(sync_data, 1):
    for col_idx, value in enumerate(row_data, 1):
        ws.cell(row=row_idx, column=col_idx, value=value)

# 저장
wb.save(EXCEL_PATH)
wb.close()

print("✅ Google Sheets → OneDrive Excel 동기화 완료!")