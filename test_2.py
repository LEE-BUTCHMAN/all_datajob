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

    # 사용자 제공 쿼리 (정확히 이 쿼리 사용!)
    query = """select year (substr(si.entering_dated_at, 1, 10)) as order_year, week(substr(si.entering_dated_at, 1, 10), 1) as order_week, CASE s.status
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
and substr(si.entering_dated_at,1,10) >= '2025-07-14'
and substr(si.entering_dated_at,1,10) <= '2025-07-20'
group by 1,2,3,4,5"""

    df = pd.read_sql(query, connection)
    connection.close()

    # 기본 필터링 조건
    base_filter = (df['item_status'] == '주문') & (df['delivery_status'].isin(['배송중', '배송완료', '배송준비']))

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

    # 직배/택배별 금액 구간 비중 쿼리
    query = """
            select entering_year,
           entering_week,
           delivery_type,
           max(case when bs_seg = '15만_under' then percentage else null end) as '15_under_percentage',
           max(case when bs_seg = '15만_up' then percentage else null end) as '15_up_percentage',
           max(case when bs_seg = '20만_up' then percentage else null end) as '20_up_percentage',
           max(case when bs_seg = '25만_up' then percentage else null end) as '25_up_percentage',
           max(case when bs_seg = '30만_up' then percentage else null end) as '30_up_percentage'
    from (
        select entering_year,
               entering_week,
               delivery_type,
               bs_seg,
               cnt,
               ROUND(cnt * 100.0 / SUM(cnt) OVER (PARTITION BY entering_year, entering_week), 2) as percentage,
               SUM(cnt) OVER (PARTITION BY entering_year, entering_week) as total_orders
        from (
            select entering_year,
                   entering_week,
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
                select year(substr(si.entering_dated_at, 1, 10)) as entering_year,
                       week(substr(si.entering_dated_at, 1, 10), 1) as entering_week,
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
                  and substr(si.entering_dated_at, 1, 10) >= '2025-07-14'
                  and substr(si.entering_dated_at, 1, 10) <= '2025-07-20'
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

    # delivery_type 컬럼 제거 (불필요)
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

    # 업종별 쿼리
    query = """select year (substr(si.entering_dated_at, 1, 10)) as entering_year, week(substr(si.entering_dated_at, 1, 10), 1) as entering_week, CASE s.status
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
               and substr(si.entering_dated_at,1,10) >= '2025-07-14'
               and substr(si.entering_dated_at,1,10) <= '2025-07-20'
               group by 1,2,3,4,5,6"""

    df = pd.read_sql(query, connection)
    connection.close()

    # 기본 필터링 조건
    base_filter = (df['item_status'] == '주문') & (df['delivery_status'].isin(['배송중', '배송완료', '배송준비']))

    # 업종별 데이터 분리
    business_data = {}
    for business in BUSINESS_ORDER:
        business_df = df[base_filter & (df['business_category'] == business)]
        if not business_df.empty:
            business_summary = business_df.groupby('entering_week').agg({
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


def get_weekly_data_business():
    """업종별 데이터 조회 (두 번째 코드에서 가져옴)"""
    connection = pymysql.connect(
        host='prod-common-db.cluster-ro-ch624l3cypvt.ap-northeast-2.rds.amazonaws.com',
        user='cancun_data',
        password='#ZXsd@~H>)2>',
        database='cancun',
        port=3306,
        charset='utf8mb4'
    )

    # 업종별 쿼리
    query = """select year (substr(si.entering_dated_at, 1, 10)) as order_year, week(substr(si.entering_dated_at, 1, 10), 1) as order_week, CASE s.status
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
               and substr(si.entering_dated_at,1,10) >= '2025-07-14'
               and substr(si.entering_dated_at,1,10) <= '2025-07-20'
               group by 1,2,3,4,5,6"""

    df = pd.read_sql(query, connection)
    connection.close()

    # 기본 필터링 조건
    base_filter = (df['item_status'] == '주문') & (df['delivery_status'].isin(['배송중', '배송완료', '배송준비']))

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


def update_sheets(direct_df, parcel_df):
    """Google Sheets automation 시트에 직배/택배 각각 업데이트"""
    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 시트 열기
    sheet = client.open_by_key('1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE')
    worksheet = sheet.worksheet('automation(매출)')

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
                    if sheet_week and int(float(sheet_week)) == week:
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

            try:
                # C열: 공급가
                worksheet.update_cell(target_row, 3, int(row['supply_price']))
                time.sleep(1.0)  # API 제한 방지

                # D열: 할인금액
                worksheet.update_cell(target_row, 4, int(row['discount_price']))
                time.sleep(1.0)

                # E열: 포인트
                worksheet.update_cell(target_row, 5, int(row['point']))
                time.sleep(1.0)

                # F열: 쿠폰
                worksheet.update_cell(target_row, 6, int(row['coupon']))
                time.sleep(1.0)

                # G열: 배송비
                worksheet.update_cell(target_row, 7, int(row['delivery_price']))
                time.sleep(1.0)

                # H열: 주문수
                worksheet.update_cell(target_row, 8, int(row['orders']))
                time.sleep(1.0)

                # I열: 주문매칭수
                worksheet.update_cell(target_row, 9, int(row['orders_burial']))
                time.sleep(1.0)

                # J열: 신규주문매칭수 (첫구매자)
                worksheet.update_cell(target_row, 10, int(row['first_order_users']))
                time.sleep(1.0)

                # K열: 총품목수량
                worksheet.update_cell(target_row, 11, int(row['orders_sku']))
                time.sleep(1.0)

                print(f"{delivery_type} {week}주차 완료!")

                # 실제 업데이트 확인
                check_value = worksheet.cell(target_row, 3).value
                print(f"확인: {target_row}행 3열 값 = {check_value}")

            except Exception as e:
                print(f"ERROR - {delivery_type} {week}주차 업데이트 실패: {e}")

    # 직배 데이터 업데이트
    if not direct_df.empty:
        print("\n=== 직배 데이터 업데이트 ===")
        update_delivery_data(direct_df, '직배')

    # 택배 데이터 업데이트
    if not parcel_df.empty:
        print("\n=== 택배 데이터 업데이트 ===")
        update_delivery_data(parcel_df, '택배')


def update_bs_segment_sheets(direct_df, parcel_df):
    """Google Sheets에 금액 구간별 비중 데이터 업데이트 (Q,R,S,T,U 열)"""
    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 시트 열기
    sheet = client.open_by_key('1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE')
    worksheet = sheet.worksheet('automation(매출)')

    # B열에서 모든 주차 데이터 읽기
    week_data = worksheet.col_values(2)

    def update_bs_data(df, delivery_type):
        """배송 유형별 금액 구간 비중 데이터 업데이트"""
        for _, row in df.iterrows():
            week = int(row['entering_week'])

            # 해당 주차와 배송 유형에 맞는 행 찾기
            target_rows = []
            for idx, sheet_week in enumerate(week_data):
                try:
                    if sheet_week and int(float(sheet_week)) == week:
                        target_rows.append(idx + 1)
                except ValueError:
                    continue

            # 직배는 첫 번째 매칭, 택배는 두 번째 매칭
            if delivery_type == '직배' and target_rows:
                target_row = target_rows[0]
            elif delivery_type == '택배' and len(target_rows) > 1:
                target_row = target_rows[1]
            elif delivery_type == '택배' and len(target_rows) == 1:
                target_row = target_rows[0]
            else:
                print(f"{delivery_type} {week}주차 금액구간 업데이트할 행을 찾을 수 없습니다.")
                continue

            print(f"{delivery_type} {week}주차 금액구간 비중 업데이트 중... (행: {target_row})")

            # Q열(17): 15만원미만 비중
            percentage_15_under = row['15_under_percentage'] if pd.notna(row['15_under_percentage']) else 0
            worksheet.update_cell(target_row, 17, float(percentage_15_under))
            time.sleep(1.0)  # API 제한 방지

            # R열(18): 15만원이상 비중
            percentage_15_up = row['15_up_percentage'] if pd.notna(row['15_up_percentage']) else 0
            worksheet.update_cell(target_row, 18, float(percentage_15_up))
            time.sleep(1.0)

            # S열(19): 20만원이상 비중
            percentage_20_up = row['20_up_percentage'] if pd.notna(row['20_up_percentage']) else 0
            worksheet.update_cell(target_row, 19, float(percentage_20_up))
            time.sleep(1.0)

            # T열(20): 25만원이상 비중
            percentage_25_up = row['25_up_percentage'] if pd.notna(row['25_up_percentage']) else 0
            worksheet.update_cell(target_row, 20, float(percentage_25_up))
            time.sleep(1.0)

            # U열(21): 30만원이상 비중
            percentage_30_up = row['30_up_percentage'] if pd.notna(row['30_up_percentage']) else 0
            worksheet.update_cell(target_row, 21, float(percentage_30_up))
            time.sleep(1.0)

            print(f"{delivery_type} {week}주차 금액구간 비중 완료!")

    # 직배 데이터 업데이트
    if not direct_df.empty:
        print("\n=== 직배 금액구간 비중 업데이트 ===")
        update_bs_data(direct_df, '직배')

    # 택배 데이터 업데이트
    if not parcel_df.empty:
        print("\n=== 택배 금액구간 비중 업데이트 ===")
        update_bs_data(parcel_df, '택배')


def update_sheets_business(business_data):
    """Google Sheets에 업종별 데이터 업데이트"""
    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 시트 열기
    sheet = client.open_by_key('1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE')
    worksheet = sheet.worksheet('automation(매출)')
    print(f"현재 시트 이름: {worksheet.title}")
    print(f"사용 가능한 시트들: {[ws.title for ws in sheet.worksheets()]}")

    def update_business_data(df, business_name, business_index):
        """업종별 데이터 업데이트"""
        for _, row in df.iterrows():
            week = int(row['entering_week'])

            # 업종별 시작 행 계산 (55행부터 시작, 각 업종마다 26행씩)
            business_start_row = 55 + (business_index * 26) + 1  # 헤더 다음 행부터

            # 주차에 맞는 행 계산 (29주차가 첫 번째 데이터 행)
            target_row = business_start_row + (week - 29)

            if target_row < business_start_row or target_row >= business_start_row + 24:
                print(f"{business_name} {week}주차는 범위를 벗어났습니다.")
                continue

            print(f"{business_name} {week}주차 업데이트 중... (행: {target_row})")

            # C열: 순매출 (pg_supply_price)
            worksheet.update_cell(target_row, 3, int(row['pg_supply_price']))
            time.sleep(1.0)  # API 제한 방지

            # D열: 할인금액
            worksheet.update_cell(target_row, 4, int(row['discount_price']))
            time.sleep(1.0)

            # E열: 포인트
            worksheet.update_cell(target_row, 5, int(row['point']))
            time.sleep(1.0)

            # F열: 쿠폰
            worksheet.update_cell(target_row, 6, int(row['coupon']))
            time.sleep(1.0)

            # G열: 배송비
            worksheet.update_cell(target_row, 7, int(row['delivery_price']))
            time.sleep(1.0)

            # H열: 주문수
            worksheet.update_cell(target_row, 8, int(row['orders']))
            time.sleep(1.0)

            # I열: 주문매칭수
            worksheet.update_cell(target_row, 9, int(row['orders_burial']))
            time.sleep(1.0)

            # J열: 신규주문매칭수 (첫구매자)
            worksheet.update_cell(target_row, 10, int(row['first_order_users']))
            time.sleep(1.0)

            # K열: 총품목수량
            worksheet.update_cell(target_row, 11, int(row['orders_sku']))
            time.sleep(1.0)

            print(f"{business_name} {week}주차 완료!")

    # 업종별 데이터 업데이트 (순서대로)
    for business_index, business_name in enumerate(BUSINESS_ORDER):
        if business_name in business_data and not business_data[business_name].empty:
            print(f"\n=== {business_name} 데이터 업데이트 ===")
            update_business_data(business_data[business_name], business_name, business_index)


def update_sheets_business(business_data):
    """Google Sheets에 업종별 데이터 업데이트 (두 번째 코드에서 가져옴)"""
    # 인증
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('/Users/sfn/Downloads/automation-data-467003-6310e37f0e5c.json',
                                                  scopes=scope)
    client = gspread.authorize(creds)

    # 시트 열기
    sheet = client.open_by_key('1zmujGEM6C51LxrljTlIsKAwxgXAj82K9YfkQxpg7OjE')
    worksheet = sheet.worksheet('automation(매출)')

    def update_business_data(df, business_name, business_index):
        """업종별 데이터 업데이트"""
        for _, row in df.iterrows():
            week = int(row['order_week'])

            # 업종별 시작 행 계산 (55행부터 시작, 각 업종마다 26행씩)
            business_start_row = 55 + (business_index * 26) + 1  # 헤더 다음 행부터

            # 주차에 맞는 행 계산 (29주차가 첫 번째 데이터 행)
            target_row = business_start_row + (week - 29)

            if target_row < business_start_row or target_row >= business_start_row + 24:
                print(f"{business_name} {week}주차는 범위를 벗어났습니다.")
                continue

            print(f"{business_name} {week}주차 업데이트 중... (행: {target_row})")

            # C열: 순매출 (pg_supply_price)
            worksheet.update_cell(target_row, 3, int(row['pg_supply_price']))
            time.sleep(1.0)  # API 제한 방지

            # D열: 할인금액
            worksheet.update_cell(target_row, 4, int(row['discount_price']))
            time.sleep(1.0)

            # E열: 포인트
            worksheet.update_cell(target_row, 5, int(row['point']))
            time.sleep(1.0)

            # F열: 쿠폰
            worksheet.update_cell(target_row, 6, int(row['coupon']))
            time.sleep(1.0)

            # G열: 배송비
            worksheet.update_cell(target_row, 7, int(row['delivery_price']))
            time.sleep(1.0)

            # H열: 주문수
            worksheet.update_cell(target_row, 8, int(row['orders']))
            time.sleep(1.0)

            # I열: 주문매칭수
            worksheet.update_cell(target_row, 9, int(row['orders_burial']))
            time.sleep(1.0)

            # J열: 신규주문매칭수 (첫구매자)
            worksheet.update_cell(target_row, 10, int(row['first_order_users']))
            time.sleep(1.0)

            # K열: 총품목수량
            worksheet.update_cell(target_row, 11, int(row['orders_sku']))
            time.sleep(1.0)

            print(f"{business_name} {week}주차 완료!")

    # 업종별 데이터 업데이트 (순서대로)
    for business_index, business_name in enumerate(BUSINESS_ORDER):
        if business_name in business_data and not business_data[business_name].empty:
            print(f"\n=== {business_name} 데이터 업데이트 ===")
            update_business_data(business_data[business_name], business_name, business_index)


def main():
    """기존 데이터 + 금액구간 비중 데이터 + 업종별 데이터 모두 업데이트"""
    print("주차별 직배/택배 + 업종별 데이터 업데이트 시작...")

    # 1. 기존 데이터 조회 및 업데이트
    direct_df, parcel_df = get_weekly_data()
    print(f"기존 데이터 - 직배: {len(direct_df)}개 주차, 택배: {len(parcel_df)}개 주차 조회 완료")
    update_sheets(direct_df, parcel_df)

    # 2. 금액구간 비중 데이터 조회 및 업데이트
    bs_direct_df, bs_parcel_df = get_bs_segment_data()
    print(f"금액구간 데이터 - 직배: {len(bs_direct_df)}개 주차, 택배: {len(bs_parcel_df)}개 주차 조회 완료")
    update_bs_segment_sheets(bs_direct_df, bs_parcel_df)

    # 3. 업종별 데이터 조회 및 업데이트 (두 번째 코드에서 가져온 부분)
    business_data = get_weekly_data_business()
    total_businesses = sum(1 for df in business_data.values() if not df.empty)
    print(f"업종별 데이터 - {total_businesses}개 업종 데이터 조회 완료")
    update_sheets_business(business_data)

    print("모든 업데이트 완료!")


if __name__ == "__main__":
    main()