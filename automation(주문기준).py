import pymysql
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import warnings
import time

# 모든 warning 무시
warnings.filterwarnings('ignore')

# 업종 순서 정의
BUSINESS_ORDER = [
    '서양식·피자·파스타·햄버거',
    '카페·샐러드·샌드위치',
    '한식·분식',
    '베이커리',
    '주점',
    '중식',
    '일식',
    '치킨',
    '뷔페·급식·구내식당',
    '아시안'
]


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
and substr(s.order_dated_at,1,10) >= '2025-06-01'
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


def get_bs_segment_data():
    """직배와 택배별 금액 구간 비중 데이터 조회"""
    connection = pymysql.connect(
        host='prod-common-db.cluster-ro-ch624l3cypvt.ap-northeast-2.rds.amazonaws.com',
        user='cancun_data',
        password='#ZXsd@~H>)2>',
        database='cancun',
        port=3306,
        charset='utf8mb4'
    )

    query = """
            select order_year,
           order_week,
           delivery_type,
           max(case when bs_seg = '15만_under' then percentage else null end) as '15_under_percentage',
           max(case when bs_seg = '15만_up' then percentage else null end) as '15_up_percentage',
           max(case when bs_seg = '20만_up' then percentage else null end) as '20_up_percentage',
           max(case when bs_seg = '25만_up' then percentage else null end) as '25_up_percentage',
           max(case when bs_seg = '30만_up' then percentage else null end) as '30_up_percentage'
    from (
        select order_year,
               order_week,
               delivery_type,
               bs_seg,
               cnt,
               ROUND(cnt * 100.0 / SUM(cnt) OVER (PARTITION BY order_year, order_week), 2) as percentage,
               SUM(cnt) OVER (PARTITION BY order_year, order_week) as total_orders
        from (
            select order_year,
                   order_week,
                   delivery_type,
                   case
                       when total >= 100000 and total < 150000 then '15만_under'
                       when total >= 150000 and total < 200000 then '15만_up'
                       when total >= 200000 and total < 250000 then '20만_up'
                       when total >= 250000 and total < 300000 then '25만_up'
                       when total >= 300000 then '30만_up'
                       else '10만_under' end as bs_seg,
                   count(distinct order_number) as cnt
            from (
                select year(substr(s.order_dated_at, 1, 10)) as order_year,
                       week(substr(s.order_dated_at, 1, 10), 1) as order_week,
                       s.order_number,
                       CASE WHEN s.courier = 'SFN' THEN '직배' ELSE '택배' END as delivery_type,
                       sum(IF(si.tax_type = 'TAX',
                              CAST(ROUND(si.price * si.quantity / 1.1, 0) AS SIGNED) + si.price * si.quantity -
                              CAST(ROUND(si.price * si.quantity / 1.1, 0) AS SIGNED),
                              CAST(ROUND(si.price * si.quantity, 0) AS SIGNED))) as total
                from cancun.shipment_item si
                inner join cancun.shipment s on s.id = si.shipment_id and s.status != 'DELETE'
                where si.is_deleted = 0
                  and si.item_status in ('ORDER')
                  and s.status in ('PAYMENT', 'SHIPPING', 'SHIPPING_COMPLETE', 'READY_SHIPMENT')
                  and substr(s.order_dated_at, 1, 10) >= '2025-06-01'
                  and substr(s.order_dated_at, 1, 10) <= '2025-07-20'
                group by 1, 2, 3, 4
            ) A
            group by 1, 2, 3, 4
        ) B
    ) C
    group by 1, 2, 3
    """

    df = pd.read_sql(query, connection)
    connection.close()

    # 직배와 택배 데이터 분리
    direct_df = df[df['delivery_type'] == '직배'].copy()
    parcel_df = df[df['delivery_type'] == '택배'].copy()

    # delivery_type 컬럼 제거
    direct_df = direct_df.drop('delivery_type', axis=1)
    parcel_df = parcel_df.drop('delivery_type', axis=1)

    return direct_df, parcel_df


def get_weekly_data_business():
    """업종별 데이터 조회"""
    connection = pymysql.connect(
        host='prod-common-db.cluster-ro-ch624l3cypvt.ap-northeast-2.rds.amazonaws.com',
        user='cancun_data',
        password='#ZXsd@~H>)2>',
        database='cancun',
        port=3306,
        charset='utf8mb4'
    )

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
                      bt.name as business_category,
                      sum(CASE si.tax_type
                      WHEN 'TAX' THEN CAST(ROUND(si.price * si.quantity / 1.1, 0) AS SIGNED)
                      ELSE CAST(ROUND(si.price * si.quantity, 0) AS SIGNED)
                      END) AS supply_price,
                      sum(si.supply_price) as pg_supply_price,
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
               inner join cancun.user_business_type_code ubtc on ubtc.base_user_id = bu.id
               inner join cancun.business_type bt on bt.id = ubtc.business_type_codes
               where si.is_deleted = 0
               and substr(s.order_dated_at,1,10) >= '2025-06-01'
               and substr(s.order_dated_at,1,10) <= '2025-07-20'
               group by 1,2,3,4,5,6"""

    df = pd.read_sql(query, connection)
    connection.close()

    # 기본 필터링 조건
    base_filter = (df['item_status'] == '주문') & (df['delivery_status'].isin(['결제완료', '배송중', '배송완료', '배송준비']))

    # 업종별 데이터 분리
    business_data = {}
    for business in BUSINESS_ORDER:
        business_df = df[base_filter & (df['business_category'] == business)]
        if not business_df.empty:
            business_summary = business_df.groupby('order_week').agg({
                'pg_supply_price': 'sum',  # 순매출 사용
                'discount_price': 'sum',
                'delivery_price': 'sum',
                'coupon': 'sum',
                'point': 'sum',
                'orders': 'sum',
                'orders_sku': 'sum',
                'orders_burial': 'sum',
                'first_order_users': 'sum'
            }).reset_index()
            business_data[business] = business_summary
        else:
            business_data[business] = pd.DataFrame()

    return business_data

def update_delivery_data_by_row(df, delivery_type, item_rows, worksheet):
    """배송 유형별 데이터 업데이트 - 29주차 데이터만 B열에 입력"""
    print(f"\n=== {delivery_type} 데이터 업데이트 ===")

    # 29주차 고정, B열(2번째 열)에 업데이트
    target_week = 29
    target_col = 2  # B열

    print(f"{delivery_type} {target_week}주차를 B열에 업데이트합니다.")

    # 29주차 데이터만 찾기
    target_week_data = df[df['order_week'] == target_week]

    if target_week_data.empty:
        print(f"{delivery_type} {target_week}주차 데이터가 없습니다.")
        return

    for _, row in target_week_data.iterrows():
        print(f"{delivery_type} {target_week}주차 업데이트 중... (열: {target_col})")

        # 각 항목별로 해당 행에 데이터 입력
        for item_key, item_row in item_rows.items():
            if item_key in row:
                value = int(row[item_key])
                worksheet.update_cell(item_row, target_col, value)
                time.sleep(1.0)
                print(f"  {item_key}: 행{item_row}, 열{target_col} = {value}")

        print(f"{delivery_type} {target_week}주차 완료!")
        break  # 29주차는 하나만 있으므로 break


def update_sheets(direct_df, parcel_df):
    """Google Sheets에 직배/택배 데이터 업데이트 - 29주차 데이터만 B열에 업데이트"""
    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 시트 열기
    sheet = client.open_by_key('1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE')
    worksheet = sheet.worksheet('automation(주문)')

    # 직배 항목별 행 번호 (실제 시트 기준)
    direct_rows = {
        'supply_price': 33,  # 공급가
        'discount_price': 34,  # 할인금액
        'point': 36,  # 포인트
        'coupon': 38,  # 쿠폰
        'delivery_price': 40,  # 배송비
        'orders': 41,  # 주문수
        'orders_burial': 42,  # 주문매칭수
        'first_order_users': 43,  # 신규주문매칭수
        'orders_sku': 44  # 총품목수량
    }

    # 택배 항목별 행 번호 (택배 섹션 시작 행 필요)
    parcel_rows = {
        'supply_price': 59,  # 공급가 (추정)
        'discount_price': 60,  # 할인금액 (추정)
        'point': 62,  # 포인트 (추정)
        'coupon': 64,  # 쿠폰 (추정)
        'delivery_price': 66,  # 배송비 (추정)
        'orders': 67,  # 주문수 (추정)
        'orders_burial': 68,  # 주문매칭수 (추정)
        'first_order_users': 69,  # 신규주문매칭수 (추정)
        'orders_sku': 70  # 총품목수량 (추정)
    }

    # 직배 데이터 업데이트
    if not direct_df.empty:
        update_delivery_data_by_row(direct_df, '직배', direct_rows, worksheet)

    # 택배 데이터 업데이트
    if not parcel_df.empty:
        update_delivery_data_by_row(parcel_df, '택배', parcel_rows, worksheet)


def update_bs_segment_sheets(direct_df, parcel_df):
    """Google Sheets에 금액 구간별 비중 데이터 업데이트 - 29주차 데이터만 B열에 업데이트"""
    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 시트 열기
    sheet = client.open_by_key('1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE')
    worksheet = sheet.worksheet('automation(주문)')

    # 직배 BS구간별 행 번호 (실제 시트 기준)
    direct_bs_rows = {
        '15_under_percentage': 46,  # 15만 미만(%)
        '15_up_percentage': 47,  # 15만 이상(%)
        '20_up_percentage': 48,  # 20만 이상(%)
        '25_up_percentage': 49,  # 25만 이상(%)
        '30_up_percentage': 50  # 30만 이상(%)
    }

    # 택배 BS구간별 행 번호 (실제 시트 기준)
    parcel_bs_rows = {
        '15_under_percentage': 72,  # 15만 미만(%)
        '15_up_percentage': 73,  # 15만 이상(%)
        '20_up_percentage': 74,  # 20만 이상(%)
        '25_up_percentage': 75,  # 25만 이상(%)
        '30_up_percentage': 76  # 30만 이상(%)
    }

    def update_bs_data_by_row(df, delivery_type, bs_rows):
        """배송 유형별 금액 구간 비중 데이터 업데이트 - 29주차 데이터만 B열에 입력"""
        print(f"\n=== {delivery_type} 금액구간 비중 업데이트 ===")

        # 29주차 고정, B열(2번째 열)에 업데이트
        target_week = 29
        target_col = 2  # B열

        print(f"{delivery_type} {target_week}주차를 B열에 업데이트합니다.")

        # 29주차 데이터만 찾기
        target_week_data = df[df['order_week'] == target_week]

        if target_week_data.empty:
            print(f"{delivery_type} {target_week}주차 금액구간 데이터가 없습니다.")
            return

        for _, row in target_week_data.iterrows():
            print(f"{delivery_type} {target_week}주차 금액구간 비중 업데이트 중... (열: {target_col})")

            # 각 BS구간별로 해당 행에 데이터 입력
            for bs_key, bs_row in bs_rows.items():
                if bs_key in row:
                    percentage = row[bs_key] if pd.notna(row[bs_key]) else 0
                    worksheet.update_cell(bs_row, target_col, float(percentage))
                    time.sleep(1.0)
                    print(f"  {bs_key}: 행{bs_row}, 열{target_col} = {percentage}%")

            print(f"{delivery_type} {target_week}주차 금액구간 비중 완료!")
            break  # 29주차는 하나만 있으므로 break

    # 직배 금액구간 비중 업데이트
    if not direct_df.empty:
        update_bs_data_by_row(direct_df, '직배', direct_bs_rows)

    # 택배 금액구간 비중 업데이트
    if not parcel_df.empty:
        update_bs_data_by_row(parcel_df, '택배', parcel_bs_rows)


def update_sheets_business(business_data):
    """Google Sheets에 업종별 데이터 업데이트 - 29주차 데이터만 B열에 업데이트"""
    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 시트 열기
    sheet = client.open_by_key('1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE')
    worksheet = sheet.worksheet('automation(주문)')

    # 업종별 시작 행 계산 (실제 시트 기준 - 서양식이 114행부터 시작)
    business_start_rows = [
        114,  # 서양식·피자·파스타·햄버거 (순매출 행)
        124,  # 카페·샐러드·샌드위치 (순매출 행)
        134,  # 한식·분식 (순매출 행)
        144,  # 베이커리 (순매출 행)
        154,  # 주점 (순매출 행)
        164,  # 중식 (순매출 행)
        174,  # 일식 (순매출 행)
        184,  # 치킨 (순매출 행)
        194,  # 뷔페·급식·구내식당 (순매출 행)
        204  # 아시안 (순매출 행)
    ]

    def update_business_data_by_row(df, business_name, start_row):
        """업종별 데이터 업데이트 - 29주차 데이터만 B열에 입력"""
        print(f"\n=== {business_name} 데이터 업데이트 (시작행: {start_row}) ===")

        # 29주차 고정, B열(2번째 열)에 업데이트
        target_week = 29
        target_col = 2  # B열

        print(f"{business_name} {target_week}주차를 B열에 업데이트합니다.")

        # 업종별 항목 행 오프셋 (순매출 행 기준)
        item_offsets = {
            'pg_supply_price': 0,  # 순매출 (시작행)
            'orders': 1,  # 주문수 (+1행)
            'orders_burial': 2,  # 주문매칭수 (+2행)
            'first_order_users': 3,  # 신규주문매칭수 (+3행)
            'orders_sku': 4  # 총품목수량 (+4행)
        }

        # 29주차 데이터만 찾기
        target_week_data = df[df['order_week'] == target_week]

        if target_week_data.empty:
            print(f"{business_name} {target_week}주차 데이터가 없습니다.")
            return

        for _, row in target_week_data.iterrows():
            print(f"{business_name} {target_week}주차 업데이트 중... (열: {target_col})")

            # 5개 항목만 업데이트
            for item_key, offset in item_offsets.items():
                if item_key in row:
                    item_row = start_row + offset
                    value = int(row[item_key])
                    worksheet.update_cell(item_row, target_col, value)
                    time.sleep(1.0)
                    print(f"  {item_key}: 행{item_row}, 열{target_col} = {value}")

            print(f"{business_name} {target_week}주차 완료!")
            break  # 29주차는 하나만 있으므로 break

    # 업종별 데이터 업데이트
    for business_index, business_name in enumerate(BUSINESS_ORDER):
        if business_name in business_data and not business_data[business_name].empty:
            start_row = business_start_rows[business_index]
            update_business_data_by_row(business_data[business_name], business_name, start_row)


def main():
    """모든 데이터 업데이트 - 29주차 데이터만 B열에 업데이트"""
    print("29주차 직배/택배 + 업종별 데이터 B열 업데이트 시작...")

    # 1. 기존 데이터 조회 및 업데이트
    direct_df, parcel_df = get_weekly_data()
    print(f"기존 데이터 - 직배: {len(direct_df)}개 주차, 택배: {len(parcel_df)}개 주차 조회 완료")
    update_sheets(direct_df, parcel_df)

    # 2. 금액구간 비중 데이터 조회 및 업데이트
    bs_direct_df, bs_parcel_df = get_bs_segment_data()
    print(f"금액구간 데이터 - 직배: {len(bs_direct_df)}개 주차, 택배: {len(bs_parcel_df)}개 주차 조회 완료")
    update_bs_segment_sheets(bs_direct_df, bs_parcel_df)

    # 3. 업종별 데이터 조회 및 업데이트
    business_data = get_weekly_data_business()
    total_businesses = sum(1 for df in business_data.values() if not df.empty)
    print(f"업종별 데이터 - {total_businesses}개 업종 데이터 조회 완료")
    update_sheets_business(business_data)

    print("29주차 B열 업데이트 완료!")


if __name__ == "__main__":
    main()