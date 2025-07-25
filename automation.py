import pymysql
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials


def get_weekly_data():
    """MySQL에서 주차별 기본 데이터 조회"""
    connection = pymysql.connect(
        host='prod-common-db.cluster-ro-ch624l3cypvt.ap-northeast-2.rds.amazonaws.com',
        user='cancun_data',
        password='#ZXsd@~H>)2>',
        database='cancun',
        port=3306,
        charset='utf8mb4'
    )

    query = """SELECT week(substr(s.order_dated_at, 1, 10), 1)                                 as order_week, \
                      sum(CASE si.tax_type \
                              WHEN 'TAX' THEN CAST(ROUND(si.price * si.quantity / 1.1, 0) AS SIGNED) \
                              ELSE CAST(ROUND(si.price * si.quantity, 0) AS SIGNED) \
                          END)                                                                 AS supply_price, \
                      sum(CASE si.tax_type \
                              WHEN 'TAX' THEN CAST(ROUND((si.list_price - si.price) * si.quantity / 1.1, 0) AS SIGNED) \
                              ELSE CAST(ROUND((si.list_price - si.price) * si.quantity, 0) AS SIGNED) \
                          END)                                                                 AS discount_price, \
                      sum(s.delivery_price - CAST(ROUND(s.delivery_price / 1.1, 0) AS SIGNED)) as delivery_price_vat, \
                      count(distinct s.order_number)                                           as orders, \
                      count(bu.id)                                                             as orders_burial, \
                      count(s.order_number)                                                    as orders_sku
               FROM cancun.shipment_item si
                        INNER JOIN cancun.shipment s ON s.id = si.shipment_id AND s.status != 'DELETE'
    INNER JOIN cancun.`user` u \
               ON u.base_user_id = s.user_id
                   INNER JOIN cancun.base_user bu ON bu.id = u.base_user_id
               WHERE si.is_deleted = 0
                 AND si.item_status = 'ORDER'
                 AND s.status IN ('PAYMENT' \
                   , 'READY_SHIPMENT' \
                   , 'SHIPPING' \
                   , 'SHIPPING_COMPLETE')
                 AND substr(s.order_dated_at \
                   , 1 \
                   , 10) >= '2025-07-01'
               GROUP BY 1
               ORDER BY 1"""

    df = pd.read_sql(query, connection)
    connection.close()
    return df


def update_sheets(df):
    """Google Sheets 업데이트"""
    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('service-account-key.json', scopes=scope)
    client = gspread.authorize(creds)

    # 시트 열기
    sheet = client.open_by_key('1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE')
    worksheet = sheet.worksheet('automation')

    # B열에서 주차 데이터 읽기
    week_data = worksheet.col_values(2)[1:]  # 헤더 제외

    # 각 주차별 업데이트
    for _, row in df.iterrows():
        week = int(row['order_week'])

        # 해당 주차 행 찾기
        target_row = None
        for idx, sheet_week in enumerate(week_data):
            if sheet_week and int(sheet_week) == week:
                target_row = idx + 2
                break

        if target_row:
            print(f"{week}주차 업데이트 중...")

            # C열: 공급가
            worksheet.update(f'C{target_row}', int(row['supply_price']))

            # D열: 할인금액
            worksheet.update(f'D{target_row}', int(row['discount_price']))

            # E열: 포인트 (나중에 추가)
            # worksheet.update(f'E{target_row}', 0)

            # F열: 쿠폰 (나중에 추가)
            # worksheet.update(f'F{target_row}', 0)

            # G열: 배송비
            worksheet.update(f'G{target_row}', int(row['delivery_price_vat']))

            # H열: 주문수
            worksheet.update(f'H{target_row}', int(row['orders']))

            # I열: 주문매장수
            worksheet.update(f'I{target_row}', int(row['orders_burial']))

            # J열: 신규주문매장수 (나중에 추가)
            # worksheet.update(f'J{target_row}', 0)

            # K열: 총품목수량
            worksheet.update(f'K{target_row}', int(row['orders_sku']))

            print(f"{week}주차 완료!")


def main():
    print("주차별 데이터 업데이트 시작...")

    # 데이터 조회
    df = get_weekly_data()
    print(f"{len(df)}개 주차 데이터 조회 완료")

    # 시트 업데이트
    update_sheets(df)
    print("업데이트 완료!")


if __name__ == "__main__":
    main()