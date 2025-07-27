import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
import sys
import subprocess
from matplotlib.colors import LinearSegmentedColormap
import warnings

warnings.filterwarnings('ignore')

# 필수 라이브러리 확인 및 자동 설치
try:
    import openpyxl

    print("✅ openpyxl 라이브러리 로드 완료")
except ImportError:
    print("❌ openpyxl이 없습니다. 자동 설치 중...")
    import subprocess
    import sys

    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl"])
        import openpyxl

        print("✅ openpyxl 자동 설치 완료!")
    except Exception as e:
        print(f"❌ 자동 설치 실패: {e}")
        print("수동으로 설치하세요:")
        print("/Users/sfn/PyCharmMiscProject/.venv/bin/pip install openpyxl")
        exit(1)

try:
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.offline import plot

    plotly_available = True
    print("✅ plotly 라이브러리 로드 완료")
except ImportError:
    print("❌ plotly가 없습니다. 자동 설치 시도 중...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "plotly"])
        import plotly.express as px
        import plotly.graph_objects as go
        from plotly.offline import plot

        plotly_available = True
        print("✅ plotly 자동 설치 완료!")
    except Exception as e:
        print(f"⚠️ plotly 자동 설치 실패: {e}")
        print("기본 차트만 생성합니다.")
        plotly_available = False

# 한글 폰트 설정
try:
    plt.rcParams['font.family'] = ['Malgun Gothic', 'AppleGothic', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False
except:
    print("⚠️ 한글 폰트 설정에 실패했습니다. 기본 폰트를 사용합니다.")


def create_beautiful_scatter_plot(file_path='/Users/sfn/Downloads/시각화.xlsx'):
    """
    시각화.xlsx 파일을 바로 읽어와서 ratio1, ratio2, member_id로 멋진 산점도를 생성
    """

    print("=" * 60)
    print("🎯 시각화.xlsx 파일에서 멋진 산점도 생성!")
    print("=" * 60)

    # 1. 파일 존재 확인
    if not os.path.exists(file_path):
        print(f"❌ 파일을 찾을 수 없습니다: {file_path}")
        return None

    print(f"📊 파일 로드 중: {file_path}")

    # 2. 데이터 로드
    try:
        # 모든 시트 확인
        excel_file = pd.ExcelFile(file_path)
        print(f"📋 시트 목록: {excel_file.sheet_names}")

        # 첫 번째 시트 로드
        df = pd.read_excel(file_path, sheet_name=0)
        print(f"✅ 데이터 로드 완료: {len(df)}개 행, {len(df.columns)}개 컬럼")

        # 컬럼 정보 출력
        print(f"\n📝 전체 컬럼명:")
        for i, col in enumerate(df.columns, 1):
            print(f"   {i}. {col}")

        # 데이터 미리보기
        print(f"\n👀 데이터 미리보기 (첫 5행):")
        print(df.head())

    except Exception as e:
        print(f"❌ 파일 읽기 실패: {e}")
        return None

    # 4. 필요한 컬럼 찾기 (유연한 매칭)
    ratio1_col = None
    ratio2_col = None
    member_id_col = None

    # 컬럼명에서 ratio1, ratio2, member_id와 유사한 컬럼 찾기
    for col in df.columns:
        col_lower = str(col).lower()
        if 'ratio1' in col_lower or 'ratio_1' in col_lower:
            ratio1_col = col
        elif 'ratio2' in col_lower or 'ratio_2' in col_lower:
            ratio2_col = col
        elif 'member' in col_lower and 'id' in col_lower:
            member_id_col = col
        elif 'memberid' in col_lower:
            member_id_col = col

    print(f"\n🎯 매칭된 컬럼:")
    print(f"   X축 (ratio1): {ratio1_col}")
    print(f"   Y축 (ratio2): {ratio2_col}")
    print(f"   멤버 ID: {member_id_col}")

    # 필수 컬럼이 없으면 사용자에게 선택 요청
    if not all([ratio1_col, ratio2_col, member_id_col]):
        print(f"\n⚠️ 일부 컬럼을 자동으로 찾지 못했습니다.")
        print(f"아래 코드를 수정하여 올바른 컬럼명을 지정해주세요:")
        print(f"ratio1_col = '{df.columns[0]}'  # X축에 사용할 컬럼")
        print(f"ratio2_col = '{df.columns[1]}'  # Y축에 사용할 컬럼")
        print(f"member_id_col = '{df.columns[2]}'  # 멤버 ID 컬럼")

        # 임시로 처음 3개 컬럼 사용
        ratio1_col = df.columns[0] if len(df.columns) > 0 else None
        ratio2_col = df.columns[1] if len(df.columns) > 1 else None
        member_id_col = df.columns[2] if len(df.columns) > 2 else df.columns[0]

    if not all([ratio1_col, ratio2_col]):
        print("❌ X축, Y축 데이터를 찾을 수 없습니다.")
        return None

    # 5. 데이터 전처리
    print(f"\n🧹 데이터 전처리 중...")

    # 결측값 확인
    missing_data = df[[ratio1_col, ratio2_col]].isnull().sum()
    print(f"결측값: {ratio1_col}={missing_data[ratio1_col]}, {ratio2_col}={missing_data[ratio2_col]}")

    # 결측값 제거
    df_clean = df.dropna(subset=[ratio1_col, ratio2_col]).copy()
    print(f"전처리 후 데이터: {len(df_clean)}개 행")

    # 6. 🎨 스타일 1: 종합 분석 차트 (2x2 서브플롯)
    print(f"\n🎨 종합 분석 차트 생성 중...")

    plt.style.use('default')
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle(f'Member ID별 {ratio1_col} vs {ratio2_col} 산점도 분석',
                 fontsize=20, fontweight='bold', y=0.98)

    # 서브플롯 1: 기본 산점도 with 색상 그라데이션
    scatter1 = ax1.scatter(df_clean[ratio1_col], df_clean[ratio2_col],
                           c=range(len(df_clean)), cmap='viridis',
                           alpha=0.7, s=60, edgecolors='white', linewidth=0.5)
    ax1.set_xlabel(ratio1_col, fontsize=12, fontweight='bold')
    ax1.set_ylabel(ratio2_col, fontsize=12, fontweight='bold')
    ax1.set_title('기본 산점도 (색상: 데이터 순서)', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    plt.colorbar(scatter1, ax=ax1, label='데이터 순서')

    # 서브플롯 2: 밀도 기반 히트맵
    ax2.hexbin(df_clean[ratio1_col], df_clean[ratio2_col],
               gridsize=20, cmap='Blues', alpha=0.8)
    ax2.set_xlabel(ratio1_col, fontsize=12, fontweight='bold')
    ax2.set_ylabel(ratio2_col, fontsize=12, fontweight='bold')
    ax2.set_title('밀도 기반 히트맵', fontsize=14, fontweight='bold')

    # 서브플롯 3: 크기 변화가 있는 산점도
    # ratio1 값에 따라 점 크기 조정
    normalized_sizes = ((df_clean[ratio1_col] - df_clean[ratio1_col].min()) /
                        (df_clean[ratio1_col].max() - df_clean[ratio1_col].min())) * 150 + 30

    scatter3 = ax3.scatter(df_clean[ratio1_col], df_clean[ratio2_col],
                           s=normalized_sizes, alpha=0.6,
                           c=df_clean[ratio1_col], cmap='plasma',
                           edgecolors='black', linewidth=0.5)
    ax3.set_xlabel(ratio1_col, fontsize=12, fontweight='bold')
    ax3.set_ylabel(ratio2_col, fontsize=12, fontweight='bold')
    ax3.set_title('크기 변화 산점도 (크기 ∝ ratio1)', fontsize=14, fontweight='bold')
    ax3.grid(True, alpha=0.3)
    plt.colorbar(scatter3, ax=ax3, label=ratio1_col)

    # 서브플롯 4: 회귀선이 포함된 산점도
    ax4.scatter(df_clean[ratio1_col], df_clean[ratio2_col],
                alpha=0.7, s=80, color='steelblue', edgecolors='white')

    # 회귀선 계산 및 그리기
    z = np.polyfit(df_clean[ratio1_col], df_clean[ratio2_col], 1)
    p = np.poly1d(z)
    ax4.plot(df_clean[ratio1_col], p(df_clean[ratio1_col]),
             "r--", alpha=0.8, linewidth=2, label=f'회귀선 (기울기: {z[0]:.3f})')

    ax4.set_xlabel(ratio1_col, fontsize=12, fontweight='bold')
    ax4.set_ylabel(ratio2_col, fontsize=12, fontweight='bold')
    ax4.set_title('회귀선 포함 산점도', fontsize=14, fontweight='bold')
    ax4.grid(True, alpha=0.3)
    ax4.legend()

    plt.tight_layout()
    plt.savefig('scatter_plot_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()

    # 7. 🌟 스타일 2: 인터랙티브 Plotly 차트
    if plotly_available:
        print(f"🌟 인터랙티브 Plotly 차트 생성 중...")

        # 호버 데이터 준비
        hover_data = {}
        if member_id_col and member_id_col in df_clean.columns:
            hover_data[member_id_col] = True

        # 점 크기 계산 (ratio1과 ratio2의 차이 기반)
        size_values = abs(df_clean[ratio2_col] - df_clean[ratio1_col]) + 0.1

        fig_plotly = px.scatter(df_clean,
                                x=ratio1_col,
                                y=ratio2_col,
                                hover_data=hover_data,
                                color=ratio1_col,
                                size=size_values,
                                color_continuous_scale='viridis',
                                title=f'🎯 인터랙티브 {ratio1_col} vs {ratio2_col} 산점도')

        fig_plotly.update_layout(
            title_font_size=20,
            xaxis_title=ratio1_col,
            yaxis_title=ratio2_col,
            font=dict(size=12),
            plot_bgcolor='rgba(240,240,240,0.8)',
            width=900,
            height=600
        )

        fig_plotly.update_traces(
            marker=dict(line=dict(width=1, color='white')),
            hovertemplate=f'<b>%{{customdata[0]}}</b><br>' +
                          f'{ratio1_col}: %{{x:.3f}}<br>' +
                          f'{ratio2_col}: %{{y:.3f}}<br>' +
                          '<extra></extra>' if member_id_col else
            f'{ratio1_col}: %{{x:.3f}}<br>' +
            f'{ratio2_col}: %{{y:.3f}}<br>' +
            '<extra></extra>'
        )

        # HTML 파일로 저장
        output_html = "interactive_scatter_plot.html"
        fig_plotly.write_html(output_html)
        print(f"✅ 인터랙티브 차트가 '{output_html}'로 저장되었습니다.")
    else:
        print(f"⚠️ Plotly가 없어서 인터랙티브 차트를 건너뜁니다.")

    # 8. 🎭 스타일 3: 고급 Seaborn 스타일
    print(f"🎭 고급 스타일 차트 생성 중...")

    plt.figure(figsize=(12, 8))
    sns.set_style("whitegrid")

    # 색상 팔레트 생성
    colors = plt.cm.Set3(np.linspace(0, 1, len(df_clean)))

    # 산점도 생성
    plt.scatter(df_clean[ratio1_col], df_clean[ratio2_col],
                c=colors, s=100, alpha=0.8,
                edgecolors='white', linewidth=1.5)

    # 제목과 레이블
    plt.title(f'✨ {ratio1_col} vs {ratio2_col} - 고급 스타일',
              fontsize=18, fontweight='bold', pad=20)
    plt.xlabel(ratio1_col, fontsize=14, fontweight='bold')
    plt.ylabel(ratio2_col, fontsize=14, fontweight='bold')

    # 격자와 스타일
    plt.grid(True, alpha=0.3, linestyle='--')
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)

    # 통계 정보 추가
    correlation = df_clean[ratio1_col].corr(df_clean[ratio2_col])
    stats_text = f'''통계 정보:
    • 상관계수: {correlation:.3f}
    • 데이터 수: {len(df_clean)}
    • {ratio1_col} 범위: {df_clean[ratio1_col].min():.2f} ~ {df_clean[ratio1_col].max():.2f}
    • {ratio2_col} 범위: {df_clean[ratio2_col].min():.2f} ~ {df_clean[ratio2_col].max():.2f}'''

    plt.text(0.02, 0.98, stats_text, transform=plt.gca().transAxes,
             fontsize=10, verticalalignment='top',
             bbox=dict(boxstyle="round,pad=0.5", facecolor="white", alpha=0.9))

    plt.tight_layout()
    plt.savefig('advanced_scatter_plot.png', dpi=300, bbox_inches='tight')
    plt.show()

    # 9. 📊 데이터 요약 보고서
    print(f"\n" + "=" * 60)
    print(f"📊 데이터 분석 요약 보고서")
    print(f"=" * 60)
    print(f"📁 파일: {os.path.basename(file_path)}")
    print(f"📋 분석 컬럼: {ratio1_col} (X축), {ratio2_col} (Y축)")
    print(f"🔢 총 데이터 포인트: {len(df_clean)}")
    print(f"📏 {ratio1_col} 범위: {df_clean[ratio1_col].min():.3f} ~ {df_clean[ratio1_col].max():.3f}")
    print(f"📏 {ratio2_col} 범위: {df_clean[ratio2_col].min():.3f} ~ {df_clean[ratio2_col].max():.3f}")
    print(f"📊 {ratio1_col} 평균: {df_clean[ratio1_col].mean():.3f}")
    print(f"📊 {ratio2_col} 평균: {df_clean[ratio2_col].mean():.3f}")
    print(f"🔗 상관계수: {correlation:.3f}")

    if abs(correlation) > 0.7:
        print(f"💡 강한 상관관계가 있습니다!")
    elif abs(correlation) > 0.3:
        print(f"💡 중간 정도의 상관관계가 있습니다.")
    else:
        print(f"💡 약한 상관관계입니다.")

    print(f"\n✅ 생성된 파일:")
    print(f"   1. scatter_plot_analysis.png - 종합 분석 차트")
    if plotly_available:
        print(f"   2. interactive_scatter_plot.html - 인터랙티브 차트")
        print(f"   3. advanced_scatter_plot.png - 고급 스타일 차트")
    else:
        print(f"   2. advanced_scatter_plot.png - 고급 스타일 차트")
        print(f"   (인터랙티브 차트는 Plotly 설치 후 이용 가능)")
    print(f"=" * 60)

    return df_clean


# 🚀 실행 함수
if __name__ == "__main__":
    print("🎯 시각화.xlsx 파일에서 ratio1, ratio2, member_id로 산점도를 생성합니다!")
    result = create_beautiful_scatter_plot('/Users/sfn/Downloads/시각화.xlsx')

    if result is not None:
        print("\n🎉 모든 시각화가 완료되었습니다!")
    else:
        print("\n❌ 시각화를 완료하지 못했습니다. 파일 경로와 데이터를 확인해주세요.")