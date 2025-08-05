import pandas as pd
import pymysql
from pandasql import sqldf

# 엑셀 파일 읽기
df = pd.read_excel('/Users/sfn/Downloads/August_target.xlsx')

# DB에서 base_user 데이터 가져오기
connection = pymysql.connect(
    host='prod-common-db.cluster-ro-ch624l3cypvt.ap-northeast-2.rds.amazonaws.com',
    user='cancun_data',
    password='#ZXsd@~H>)2>',
    database='cancun',
    port=3306,
    charset='utf8mb4'
)

base_user = pd.read_sql("SELECT * FROM cancun.base_user", connection)
user = pd.read_sql("SELECT *,CAST(marketing_alimtalk_agreement AS UNSIGNED) as marketing_agreement_int FROM cancun.user", connection)
connection.close()

# SQL로 DataFrame 조인
query = """
SELECT 
    A.mobile_phone_no,
    A.username,
    C.company_name,
    C.marketing_agreement_int,
    B.*
FROM df B 
INNER JOIN base_user A ON A.id = B.user_id 
INNER JOIN user C ON C.base_user_id = A.id    
"""


result = sqldf(query, locals())

# 엑셀 파일로 저장
result.to_excel('/Users/sfn/Downloads/result_august_target_v2.xlsx', index=False)

print(f"✅ 저장 완료!")
print(f"📁 파일 위치: /Users/sfn/Downloads/result_august_target_v2.xlsx")
print(f"📊 결과: {len(result)}행 × {len(result.columns)}열")
print("\n🔍 결과 미리보기:")
print(result.head())