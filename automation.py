import pymysql
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import warnings

warnings.filterwarnings('ignore', category=UserWarning)


def connect_and_query():
    """MySQL 데이터 조회"""
    try:
        connection = pymysql.connect(
            host='prod-common-db.cluster-ro-ch624l3cypvt.ap-northeast-2.rds.amazonaws.com',
            user='cancun_data',
            password='#ZXsd@~H>)2>',
            database='cancun',
            port=3306,
            charset='utf8mb4'
        )

        query = """SELECT year (substr(s.order_dated_at, 1, 10)) as order_year, week(substr(s.order_dated_at, 1, 10), 1) as order_week, CASE s.status
                       WHEN 'PENDING' THEN '입금대기'
                       WHEN 'PAYMENT' THEN '결제완료'
                       WHEN 'READY_SHIPMENT' THEN '배송준비'
                       WHEN 'SHIPPING' THEN '배송중'
                       WHEN 'SHIPPING_COMPLETE' THEN '배송완료'
                       WHEN 'CANCEL_REQUEST' THEN '취소요청'
                       WHEN 'CANCEL' THEN '취소완료'
                       ELSE '기타'
        END \
        AS delivery_status,
       CASE si.item_status
                 WHEN 'PENDING' THEN '입금대기'
                 WHEN 'ORDER' THEN '주문'
                 WHEN 'CANCEL_PARTIAL' THEN '부분취소'
                 WHEN 'CANCEL_REQUEST' THEN '취소요청'
                 WHEN 'CANCEL' THEN '취소완료'
                 ELSE 'UNKNOWN'
        END \
        AS item_status,
       CASE WHEN s.courier = 'SFN' THEN '직배'
            ELSE '택배'
        END \
        as delivery_type,
       sum(CASE si.tax_type
       WHEN 'TAX' THEN CAST(ROUND(si.price * si.quantity / 1.1, 0) AS SIGNED)
       ELSE CAST(ROUND(si.price * si.quantity, 0) AS SIGNED)
       END) AS supply_price,
       sum(CASE si.tax_type
                 WHEN 'TAX' THEN CAST(ROUND((si.list_price - si.price) * si.quantity / 1.1, 0) AS SIGNED)
                 ELSE CAST(ROUND((si.list_price - si.price) * si.quantity, 0) AS SIGNED)
                 END) AS discount_price,
      count(distinct s.order_number) as orders,
      count(s.order_number) as orders_sku,
      count(bu.id) as orders_burial,
      sum(s.delivery_price - CAST(ROUND(s.delivery_price / 1.1, 0) AS SIGNED)) as delivery_price_vat
from cancun.shipment_item si
inner join cancun.shipment s on s.id = si.shipment_id and s.status != 'DELETE'
inner join cancun.`user` u on u.base_user_id = s.user_id
inner join cancun.base_user bu on bu.id = u.base_user_id
where si.is_deleted = 0
and substr(s.order_dated_at,1,10) >= '2025-07-14'
and substr(s.order_dated_at,1,10) <= '2025-07-20'
group by 1,2,3,4,5"""

        df = pd.read_sql(query, connection)
        return df

    except Exception as e:
        print(f"MySQL 오류: {e}")
        return None
    finally:
        if 'connection' in locals():
            connection.close()


def process_data(df):
    """데이터 필터링 및 집계"""
    # 아이템상태='주문' AND 배송상태가 결제완료/배송중/배송완료/배송준비만 필터링
    valid_statuses = ['결제완료', '배송중', '배송완료', '배송준비']
    filtered_df = df[
        (df['item_status'] == '주문') &
        (df['delivery_status'].isin(valid_statuses))
        ]

    # order_week별로 집계
    summary = filtered_df.groupby('order_week').agg({
        'supply_price': 'sum',
        'discount_price': 'sum',
        'delivery_price_vat': 'sum',
        'orders': 'sum',
        'orders_sku': 'sum',
        'orders_burial': 'sum'
    }).reset_index()

    return summary


def update_google_sheets(summary_df):
    """Google Sheets 업데이트"""
    try:
        # 구글 서비스 계정 인증 (JSON 키 파일 필요)
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]

        # 서비스 계정 키 파일 경로 (미리 생성 필요)
        creds = Credentials.from_service_account_file('path/to/service-account-key.json', scopes=scope)
        client = gspread.authorize(creds)

        # 스프레드시트 열기
        sheet_id = '1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE'
        sheet = client.open_by_key(sheet_id)
        worksheet = sheet.worksheet('automation')

        # 기존 데이터 읽기 (헤더 확인)
        existing_data = worksheet.get_all_records()

        # 각 주차별로 업데이트
        for _, row in summary_df.iterrows():
            week = row['order_week']

            # 해당 주차 행 찾기 (컬럼 매칭 필요)
            target_row = None
            for idx, existing_row in enumerate(existing_data):
                if existing_row.get('order_week') == week:  # 컬럼명 확인 필요
                    target_row = idx + 2  # 헤더 고려
                    break

            if target_row:
                # 데이터 업데이트 (컬럼 위치는 실제 시트 구조에 맞게 조정)
                updates = [
                    ['C' + str(target_row), row['supply_price']],  # 공급가
                    ['D' + str(target_row), row['discount_price']],  # 할인금액
                    ['E' + str(target_row), row['delivery_price_vat']],  # 배송비
                    ['F' + str(target_row), row['orders']],  # 주문수
                    ['G' + str(target_row), row['orders_sku']],  # 총품목수
                    ['H' + str(target_row), row['orders_burial']]  # 주문매칭수
                ]

                for cell, value in updates:
                    worksheet.update(cell, value)

        print("Google Sheets 업데이트 완료!")

    except Exception as e:
        print(f"Google Sheets 오류: {e}")


def main():
    """메인 실행 함수"""
    # 1. MySQL에서 데이터 조회
    df = connect_and_query()
    if df is None:
        return

    # 2. 데이터 처리
    summary = process_data(df)
    print("집계 데이터:")
    print(summary)

    # 3. Google Sheets 업데이트
    update_google_sheets(summary)


if __name__ == "__main__":
    # 필요한 라이브러리: pip install gspread google-auth google-auth-oauthlib
    main()