import pymysql
import pandas as pd
import warnings

# pandas warning 무시
warnings.filterwarnings('ignore', category=UserWarning)


def connect_and_query():
    """PyMySQL을 사용 하여 AWS RDS MySQL에 접속 하고 쿼리 실행"""

    try:
        # 데이터베이스 연결
        print("MySQL 데이터베이스에 연결 중...")
        connection = pymysql.connect(
            host='prod-common-db.cluster-ro-ch624l3cypvt.ap-northeast-2.rds.amazonaws.com',
            user='cancun_data',
            password='#ZXsd@~H>)2>',
            database='cancun',
            port=3306,
            charset='utf8mb4'
        )

        print("데이터베이스 연결 성공!")

        # pandas 출력 옵션 설정 (모든 컬럼 보이게)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 50)

        # 쿼리 실행하고 pandas DataFrame으로 바로 변환
        query = "SELECT * FROM cancun.`order` LIMIT 100"
        df = pd.read_sql(query, connection)

        print(f"조회 완료! 총 {len(df)}개 레코드")
        print("\n" + "=" * 100)
        print(df)
        print("=" * 100)

        return df

    except Exception as e:
        print(f"오류 발생: {e}")
        return None

    finally:
        if 'connection' in locals():
            connection.close()
            print("\nMySQL 연결 종료")


if __name__ == "__main__":
    # 필요한 라이브러리 설치: pip install PyMySQL pandas

    df = connect_and_query()

    if df is not None:
        print("\n쿼리 실행 완료!")
        print(f"데이터 shape: {df.shape}")
    else:
        print("\n쿼리 실행 실패!")