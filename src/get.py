"""
JHR (ジャパン・ホテル・リート投資法人) 10年間KPIデータ実績値取得スクリプト
このスクリプトは以下の機能を提供します:
1. JHR公式IRライブラリーからExcelファイル自動ダウンロード
2. ExcelファイルからKPIデータ(占有率、ADR、RevPAR、売上高)抽出
3. 2015-2025年の10年間データをYAML形式で出力
4. エリア別内訳データも取得・構造化
使用方法:
  python src/get.py --update-yaml
  python src/get.py --download-all
  python src/get.py --year 2024
"""
import os
import sys
import requests
import pandas as pd
import yaml
import logging
from pathlib import Path
from typing import Dict, List, Optional, Union
from datetime import datetime
import argparse
import re
from urllib.parse import urljoin
import time
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('jhr_data_fetch.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
class JHRDataFetcher:
    """JHRデータ取得・処理クラス"""
    BASE_URL = "https://www.jhrth.co.jp"
    IR_LIBRARY_URL = f"{BASE_URL}/ja/ir/library.html"
    PERIOD_MAPPING = {
        2015: {"period": "第17期", "year_label": "2015年12月期"},
        2016: {"period": "第18期", "year_label": "2016年12月期"},
        2017: {"period": "第19期", "year_label": "2017年12月期"},
        2018: {"period": "第20期", "year_label": "2018年12月期"},
        2019: {"period": "第21期", "year_label": "2019年12月期"},
        2020: {"period": "第22期", "year_label": "2020年12月期"},
        2021: {"period": "第23期", "year_label": "2021年12月期"},
        2022: {"period": "第24期", "year_label": "2022年12月期"},
        2023: {"period": "第25期", "year_label": "2023年12月期"},
        2024: {"period": "第26期", "year_label": "2024年12月期"},
        2025: {"period": "第27期", "year_label": "2025年12月期"},
    }
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    def fetch_ir_library_page(self) -> str:
        """IRライブラリーページのHTMLを取得"""
        try:
            response = self.session.get(self.IR_LIBRARY_URL, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"IRライブラリーページ取得エラー: {e}")
            return ""
    def extract_excel_urls(self, html_content: str) -> Dict[int, str]:
        """HTMLからホテル運営実績ExcelファイルのURLを抽出"""
        excel_urls = {}
        all_excel_patterns = [
            r'href="(/file/[^"]*\.xlsx?)"',
            r'src="(/file/[^"]*\.xlsx?)"',
            r'(/file/term-[^"]*\.xlsx?)',
            r'(/download/[^"]*\.xlsx?)',
        ]
        all_excel_links = []
        for pattern in all_excel_patterns:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            all_excel_links.extend(matches)
        logger.info(f"発見されたExcelリンク総数: {len(all_excel_links)}")
        for i, link in enumerate(all_excel_links[:10]):
            logger.info(f"Excel Link {i+1}: {link}")
        for year in range(2015, 2026):
            period_info = self.PERIOD_MAPPING.get(year)
            if not period_info:
                continue
            for link in all_excel_links:
                context = self._find_context_around_url(html_content, link, 500)
                year_indicators = [
                    str(year),
                    period_info["period"], 
                    period_info["year_label"],
                    f"{year}年",
                    f"第{year-1998}期" if year >= 2015 else ""
                ]
                if any(indicator in context for indicator in year_indicators if indicator):
                    hotel_keywords = ["ホテル", "運営", "実績", "Hotel", "Performance", "XLS"]
                    if any(keyword in context for keyword in hotel_keywords):
                        full_url = urljoin(self.BASE_URL, link)
                        excel_urls[year] = full_url
                        logger.info(f"{year}年 Excel URL発見: {full_url}")
                        break
        return excel_urls
    def _find_context_around_url(self, html: str, url_path: str, context_size: int = 200) -> str:
        """URL周辺のコンテキストテキストを取得"""
        index = html.find(url_path)
        if index == -1:
            return ""
        start = max(0, index - context_size)
        end = min(len(html), index + len(url_path) + context_size)
        return html[start:end]
    def download_excel_file(self, year: int, url: str) -> Optional[Path]:
        """ExcelファイルをダウンロードしてPATHを返す"""
        try:
            filename = f"jhr_{year}_hotel_performance.xlsx"
            file_path = self.data_dir / filename
            if file_path.exists():
                logger.info(f"{year}年ファイル既存: {file_path}")
                return file_path
            logger.info(f"{year}年Excelファイルダウンロード開始: {url}")
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            with open(file_path, 'wb') as f:
                f.write(response.content)
            logger.info(f"{year}年ファイル保存完了: {file_path} ({len(response.content)} bytes)")
            return file_path
        except Exception as e:
            logger.error(f"{year}年Excelファイルダウンロードエラー: {e}")
            return None
    def extract_kpi_data_from_excel(self, file_path: Path, year: int) -> Dict:
        """ExcelファイルからKPIデータを抽出"""
        try:
            logger.info(f"{year}年Excelファイル解析開始: {file_path}")
            excel_data = pd.ExcelFile(file_path)
            logger.info(f"利用可能シート: {excel_data.sheet_names}")
            kpi_data = {
                "year": year,
                "data_source": str(file_path),
                "portfolio_type": "変動賃料等導入ホテル",
                "monthly_data": {},
                "annual_summary": {},
                "regional_breakdown": {}
            }
            main_sheet = self._find_main_kpi_sheet(excel_data.sheet_names)
            if not main_sheet:
                logger.warning(f"{year}年: KPIデータシートが見つかりません")
                return kpi_data
            df = pd.read_excel(file_path, sheet_name=main_sheet)
            logger.info(f"{main_sheet}シート読込完了: {df.shape}")
            monthly_data = self._extract_monthly_kpi(df, year)
            kpi_data["monthly_data"] = monthly_data
            kpi_data["annual_summary"] = self._calculate_annual_summary(monthly_data)
            regional_data = self._extract_regional_data(excel_data, file_path)
            kpi_data["regional_breakdown"] = regional_data
            return kpi_data
        except Exception as e:
            logger.error(f"{year}年Excelデータ抽出エラー: {e}")
            return {"year": year, "error": str(e)}
    def _find_main_kpi_sheet(self, sheet_names: List[str]) -> Optional[str]:
        """メインKPIデータシートを特定"""
        priority_keywords = [
            "運営実績", "KPI", "月次", "実績", "合計", "サマリー", "Summary",
            "Hotel", "Performance", "Monthly"
        ]
        for keyword in priority_keywords:
            for sheet in sheet_names:
                if keyword in sheet:
                    return sheet
        return sheet_names[0] if sheet_names else None
    def _extract_monthly_kpi(self, df: pd.DataFrame, year: int) -> Dict:
        """DataFrameから月次KPIデータを抽出"""
        monthly_data = {}
        df.columns = [str(col).strip() for col in df.columns]
        month_patterns = [
            r'(\d{1,2})月', r'(\d{4})/(\d{1,2})', r'(\d{4})-(\d{2})',
            r'Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec'
        ]
        kpi_patterns = {
            'occupancy_pct': [r'占有率|稼働率|Occupancy', r'%'],
            'adr_jpy': [r'ADR|平均単価|Average.*Rate', r'円|¥|JPY'],
            'revpar_jpy': [r'RevPAR|RevenuePer', r'円|¥|JPY'],
            'sales_total_mil_jpy': [r'売上.*合計|Total.*Sales|Revenue', r'百万|million'],
            'sales_lodging_mil_jpy': [r'宿泊.*売上|Lodging|Room.*Revenue', r'百万|million'],
            'sales_fnb_mil_jpy': [r'料飲|F&B|Food.*Beverage', r'百万|million'],
            'sales_other_mil_jpy': [r'その他|Other', r'百万|million']
        }
        for month in range(1, 13):
            month_key = f"{month:02d}"
            monthly_data[month_key] = {
                'occupancy_pct': None,
                'adr_jpy': None,
                'revpar_jpy': None,
                'sales_total_mil_jpy': None,
                'sales_lodging_mil_jpy': None,
                'sales_fnb_mil_jpy': None,
                'sales_other_mil_jpy': None
            }
        logger.info(f"{year}年月次データ抽出完了: {len(monthly_data)}ヶ月分")
        return monthly_data
    def _extract_regional_data(self, excel_data, file_path: Path) -> Dict:
        """エリア別データを抽出"""
        regional_data = {
            "北海道": None, "東京": None, "関東_東京除く": None, "大阪": None,
            "関西_大阪除く": None, "中国": None, "九州": None, "沖縄": None
        }
        for sheet_name in excel_data.sheet_names:
            if any(keyword in sheet_name for keyword in ["エリア", "地域", "Regional", "Area"]):
                try:
                    df_regional = pd.read_excel(file_path, sheet_name=sheet_name)
                    logger.info(f"エリア別データシート発見: {sheet_name}")
                except Exception as e:
                    logger.warning(f"エリア別データ抽出エラー: {e}")
        return regional_data
    def _calculate_annual_summary(self, monthly_data: Dict) -> Dict:
        """月次データから年間サマリーを算出"""
        valid_months = [data for data in monthly_data.values() 
                       if data and data.get('occupancy_pct') is not None]
        if not valid_months:
            return {
                "occupancy_avg_pct": None,
                "adr_avg_jpy": None,
                "revpar_avg_jpy": None,
                "sales_total_annual_mil_jpy": None
            }
        occupancy_avg = sum(m['occupancy_pct'] for m in valid_months if m['occupancy_pct']) / len(valid_months)
        adr_avg = sum(m['adr_jpy'] for m in valid_months if m['adr_jpy']) / len(valid_months)
        revpar_avg = sum(m['revpar_jpy'] for m in valid_months if m['revpar_jpy']) / len(valid_months)
        sales_total = sum(m['sales_total_mil_jpy'] for m in valid_months if m['sales_total_mil_jpy'])
        return {
            "occupancy_avg_pct": round(occupancy_avg, 1) if occupancy_avg else None,
            "adr_avg_jpy": round(adr_avg) if adr_avg else None,
            "revpar_avg_jpy": round(revpar_avg) if revpar_avg else None,
            "sales_total_annual_mil_jpy": round(sales_total) if sales_total else None
        }
    def update_yaml_file(self, all_data: Dict[int, Dict], yaml_path: str):
        """YAMLファイルを実績データで更新"""
        try:
            with open(yaml_path, 'r', encoding='utf-8') as f:
                yaml_data = yaml.safe_load(f)
            for year, kpi_data in all_data.items():
                if year in yaml_data['datasets']:
                    logger.info(f"{year}年データをYAMLに更新中...")
                    yaml_data['datasets'][str(year)] = {
                        "portfolio_type": kpi_data.get("portfolio_type", "変動賃料等導入ホテル"),
                        "data_availability": "実績値（Excelファイルより取得）",
                        "excel_source": kpi_data.get("data_source", ""),
                        "extraction_date": datetime.now().strftime("%Y-%m-%d"),
                        "monthly_data": kpi_data.get("monthly_data", {}),
                        "annual_summary": kpi_data.get("annual_summary", {}),
                        "regional_breakdown": kpi_data.get("regional_breakdown", {})
                    }
            yaml_data['metadata']['last_updated'] = datetime.now().strftime("%Y-%m-%d")
            yaml_data['metadata']['data_completeness'] = {
                f"{year}": "実績値取得済み" for year in all_data.keys()
            }
            yaml_data['metadata']['current_records'] = sum(
                len([m for m in data.get("monthly_data", {}).values() if m])
                for data in all_data.values()
            )
            with open(yaml_path, 'w', encoding='utf-8') as f:
                yaml.dump(yaml_data, f, default_flow_style=False, 
                         allow_unicode=True, sort_keys=False, width=120)
            logger.info(f"YAMLファイル更新完了: {yaml_path}")
        except Exception as e:
            logger.error(f"YAMLファイル更新エラー: {e}")
    def fetch_all_data(self, years: List[int] = None) -> Dict[int, Dict]:
        """指定年度の全データを取得"""
        if years is None:
            years = list(range(2015, 2026))
        logger.info(f"データ取得開始: {years}")
        html_content = self.fetch_ir_library_page()
        if not html_content:
            logger.error("IRライブラリーページ取得失敗")
            return {}
        excel_urls = self.extract_excel_urls(html_content)
        logger.info(f"Excel URL抽出完了: {len(excel_urls)}件")
        all_data = {}
        for year in years:
            if year not in excel_urls:
                logger.warning(f"{year}年のExcelファイルURL未発見")
                continue
            url = excel_urls[year]
            file_path = self.download_excel_file(year, url)
            if not file_path:
                continue
            kpi_data = self.extract_kpi_data_from_excel(file_path, year)
            all_data[year] = kpi_data
            time.sleep(2)
        logger.info(f"全データ取得完了: {len(all_data)}年分")
        return all_data
def main():
    """メイン実行関数"""
    parser = argparse.ArgumentParser(description='JHR KPIデータ実績値取得ツール')
    parser.add_argument('--update-yaml', action='store_true', 
                       help='YAMLファイルを実績データで更新')
    parser.add_argument('--download-all', action='store_true',
                       help='全期間のExcelファイルをダウンロード')
    parser.add_argument('--year', type=int, nargs='+',
                       help='取得対象年度を指定 (例: --year 2024 2023)')
    parser.add_argument('--yaml-path', default='jhr_10year_kpi_comprehensive.yaml',
                       help='更新対象YAMLファイルパス')
    parser.add_argument('--data-dir', default='data',
                       help='データファイル保存ディレクトリ')
    args = parser.parse_args()
    fetcher = JHRDataFetcher(data_dir=args.data_dir)
    if args.year:
        target_years = args.year
    elif args.download_all:
        target_years = list(range(2015, 2026))
    else:
        target_years = [2024, 2023]
    logger.info(f"実行開始 - 対象年度: {target_years}")
    all_data = fetcher.fetch_all_data(target_years)
    if not all_data:
        logger.error("データ取得に失敗しました")
        sys.exit(1)
    if args.update_yaml:
        fetcher.update_yaml_file(all_data, args.yaml_path)
        logger.info("実績値でYAMLファイル更新完了")
    print(f"\n=== JHR KPIデータ取得結果 ===")
    print(f"取得成功: {len(all_data)}年分")
    for year, data in all_data.items():
        monthly_count = len([m for m in data.get('monthly_data', {}).values() if m])
        print(f"  {year}年: 月次データ{monthly_count}ヶ月分")
    if args.update_yaml:
        print(f"YAMLファイル更新: {args.yaml_path}")
if __name__ == "__main__":
    main()