import pymysql
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import warnings

# 모든 warning 무시
warnings.filterwarnings('ignore')


def get_weekly_data():
    """직배와 택배 데이터를 각각 조회"""
    connection = pymysql.connect(
        host='prod-common-db.cluster-ro-ch624l3cypvt.ap-northeast-2.rds.amazonaws.com',
        user='cancun_data',
        password='#ZXsd@~H>)2>',
        database='cancun',
        port=3306,
        charset='utf8mb4'
    )

    # 사용자 제공 쿼리 (정확히 이 쿼리 사용!)
    query = """select year (substr(s.order_dated_at, 1, 10)) as order_year, week(substr(s.order_dated_at, 1, 10), 1) as order_week, CASE s.status
                   WHEN 'PENDING' THEN '입금대기'
                   WHEN 'PAYMENT' THEN '결제완료'
                   WHEN 'READY_SHIPMENT' THEN '배송준비'
                   WHEN 'SHIPPING' THEN '배송중'
                   WHEN 'SHIPPING_COMPLETE' THEN '배송완료'
                   WHEN 'CANCEL_REQUEST' THEN '취소요청'
                   WHEN 'CANCEL' THEN '취소완료'
                   WHEN 'DELETE' THEN '삭제'
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
     sum(IF(si.tax_type = 'TAX',
       CAST(ROUND(si.coupon_discount_amount / 1.1, 0) AS SIGNED),
       si.coupon_discount_amount)) as coupon,
      sum(IF(si.tax_type = 'TAX', CAST(ROUND(si.use_point / 1.1, 0) AS SIGNED), si.use_point)) as point,
      count(distinct s.order_number) as orders,
      count(s.order_number) as  orders_sku,
      count(distinct bu.id) as  orders_burial,
      count(DISTINCT CASE
    WHEN IF((SELECT COUNT(*)
             FROM cancun.shipment spmnt
             WHERE spmnt.user_id = s.user_id
               AND spmnt.status IN ('PENDING', 'PAYMENT', 'READY_SHIPMENT', 'SHIPPING', 'SHIPPING_COMPLETE')
               AND spmnt.order_dated_at < s.order_dated_at) = 0, 'O', 'X') = 'O'
    THEN s.user_id
    ELSE NULL
END) AS first_order_users,
    SUM(CAST(ROUND(s.delivery_price / 1.1, 0) AS SIGNED)) as delivery_price
from cancun.shipment_item si
inner join cancun.shipment s on s.id = si.shipment_id and s.status != 'DELETE'
inner join cancun.`user` u on u.base_user_id = s.user_id
inner join cancun.base_user bu on bu.id = u.base_user_id
where si.is_deleted = 0
and substr(s.order_dated_at,1,10) >= '2025-07-14'
and substr(s.order_dated_at,1,10) <= '2025-07-20'
group by 1,2,3,4,5"""

    df = pd.read_sql(query, connection)
    connection.close()

    # 기본 필터링 조건
    base_filter = (df['item_status'] == '주문') & (df['delivery_status'].isin(['결제완료', '배송중', '배송완료', '배송준비']))

    # 직배 필터링
    direct_df = df[base_filter & (df['delivery_type'] == '직배')]
    direct_summary = direct_df.groupby('order_week').agg({
        'supply_price': 'sum',
        'discount_price': 'sum',
        'delivery_price': 'sum',
        'coupon': 'sum',
        'point': 'sum',
        'orders': 'sum',
        'orders_sku': 'sum',
        'orders_burial': 'sum',
        'first_order_users': 'sum'
    }).reset_index()

    # 택배 필터링
    parcel_df = df[base_filter & (df['delivery_type'] == '택배')]
    parcel_summary = parcel_df.groupby('order_week').agg({
        'supply_price': 'sum',
        'discount_price': 'sum',
        'delivery_price': 'sum',
        'coupon': 'sum',
        'point': 'sum',
        'orders': 'sum',
        'orders_sku': 'sum',
        'orders_burial': 'sum',
        'first_order_users': 'sum'
    }).reset_index()

    return direct_summary, parcel_summary


def update_sheets(direct_df, parcel_df):
    """Google Sheets automation 시트에 직배/택배 각각 업데이트"""
    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 시트 열기
    sheet = client.open_by_key('1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE')
    worksheet = sheet.worksheet('automation(주문)')

    # B열에서 모든 주차 데이터 읽기
    week_data = worksheet.col_values(2)

    def update_delivery_data(df, delivery_type):
        """배송 유형별 데이터 업데이트"""
        for _, row in df.iterrows():
            week = int(row['order_week'])

            # 해당 주차와 배송 유형에 맞는 행 찾기
            target_rows = []
            for idx, sheet_week in enumerate(week_data):
                try:
                    if sheet_week and int(sheet_week) == week:
                        target_rows.append(idx + 1)
                except ValueError:
                    continue

            # 직배는 첫 번째 매칭, 택배는 두 번째 매칭 (보통 아래쪽)
            if delivery_type == '직배' and target_rows:
                target_row = target_rows[0]  # 첫 번째 매칭 (위쪽)
            elif delivery_type == '택배' and len(target_rows) > 1:
                target_row = target_rows[1]  # 두 번째 매칭 (아래쪽)
            elif delivery_type == '택배' and len(target_rows) == 1:
                # 택배만 있는 경우 첫 번째 사용
                target_row = target_rows[0]
            else:
                print(f"{delivery_type} {week}주차 업데이트할 행을 찾을 수 없습니다.")
                continue

            print(f"{delivery_type} {week}주차 업데이트 중... (행: {target_row})")

            # C열: 공급가
            worksheet.update_cell(target_row, 3, int(row['supply_price']))

            # D열: 할인금액
            worksheet.update_cell(target_row, 4, int(row['discount_price']))

            # E열: 포인트
            worksheet.update_cell(target_row, 5, int(row['point']))

            # F열: 쿠폰
            worksheet.update_cell(target_row, 6, int(row['coupon']))

            # G열: 배송비
            worksheet.update_cell(target_row, 7, int(row['delivery_price']))

            # H열: 주문수
            worksheet.update_cell(target_row, 8, int(row['orders']))

            # I열: 주문매칭수
            worksheet.update_cell(target_row, 9, int(row['orders_burial']))

            # J열: 신규주문매칭수 (첫구매자)
            worksheet.update_cell(target_row, 10, int(row['first_order_users']))

            # K열: 총품목수량
            worksheet.update_cell(target_row, 11, int(row['orders_sku']))

            print(f"{delivery_type} {week}주차 완료!")

    # 직배 데이터 업데이트
    if not direct_df.empty:
        print("\n=== 직배 데이터 업데이트 ===")
        update_delivery_data(direct_df, '직배')

    # 택배 데이터 업데이트
    if not parcel_df.empty:
        print("\n=== 택배 데이터 업데이트 ===")
        update_delivery_data(parcel_df, '택배')


def main():
    print("주차별 직배/택배 데이터 업데이트 시작...")

    # 데이터 조회 (직배, 택배 각각)
    direct_df, parcel_df = get_weekly_data()
    print(f"직배: {len(direct_df)}개 주차, 택배: {len(parcel_df)}개 주차 데이터 조회 완료")

    # 시트 업데이트
    update_sheets(direct_df, parcel_df)
    print("업데이트 완료!")


if __name__ == "__main__":
    main()