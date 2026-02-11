"""
取得した全ExcelファイルからYAMLを生成するスクリプト
"""
import pandas as pd
import yaml
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class JHRYAMLGenerator:
    """JHRデータからYAMLを生成するクラス"""
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
    def extract_monthly_data(self, df: pd.DataFrame, year: int) -> Dict[str, Dict]:
        """DataFrameから月次データを抽出"""
        monthly_data = {}
        month_cols = []
        for col_idx, col in enumerate(df.columns):
            if col_idx < df.shape[1]:
                for row_idx in range(min(10, len(df))):
                    cell_value = df.iloc[row_idx, col_idx]
                    if pd.isna(cell_value):
                        continue
                    cell_str = str(cell_value)
                    month_patterns = [r'(\d{1,2})月', r'1月', r'2月', r'3月', r'4月', r'5月', r'6月', 
                                    r'7月', r'8月', r'9月', r'10月', r'11月', r'12月']
                    for i, pattern in enumerate(month_patterns):
                        if re.search(pattern, cell_str):
                            if i == 0:
                                match = re.search(r'(\d{1,2})月', cell_str)
                                if match:
                                    month_num = int(match.group(1))
                                    if 1 <= month_num <= 12:
                                        month_cols.append((col_idx, f"{month_num:02d}"))
                            else:
                                month_cols.append((col_idx, f"{i:02d}"))
                            break
                    if month_cols and month_cols[-1][0] == col_idx:
                        break
        month_cols = list(set(month_cols))
        month_cols.sort()
        logger.info(f"{year}年: 月列発見 {len(month_cols)}個")
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
        for row_idx in range(len(df)):
            row = df.iloc[row_idx]
            indicator_type = None
            first_col = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
            if "客室稼働率" in first_col or "稼働率" in first_col:
                indicator_type = "occupancy_pct"
            elif "ADR" in first_col:
                indicator_type = "adr_jpy"
            elif "RevPAR" in first_col:
                indicator_type = "revpar_jpy"
            elif "売上" in first_col and "合計" in first_col:
                indicator_type = "sales_total_mil_jpy"
            elif "売上" in first_col and ("宿泊" in first_col or "Room" in first_col):
                indicator_type = "sales_lodging_mil_jpy"
            elif "売上" in first_col and ("料飲" in first_col or "F&B" in first_col):
                indicator_type = "sales_fnb_mil_jpy"
            elif "売上" in first_col and "その他" in first_col:
                indicator_type = "sales_other_mil_jpy"
            if not indicator_type:
                continue
            year_found = False
            for col_idx in range(min(5, len(row))):
                cell_value = str(row.iloc[col_idx]) if pd.notna(row.iloc[col_idx]) else ""
                year_patterns = [
                    f"{year}年",
                    f"平成{year-1988}年" if year >= 1989 else None,
                    f"令和{year-2018}年" if year >= 2019 else None
                ]
                for pattern in year_patterns:
                    if pattern and pattern in cell_value:
                        year_found = True
                        break
                if year_found:
                    break
            if not year_found:
                continue
            for col_idx, month_key in month_cols:
                if col_idx < len(row):
                    value = row.iloc[col_idx]
                    if pd.notna(value) and value not in ['-', 0, '0']:
                        try:
                            if isinstance(value, str):
                                value = value.replace(',', '').replace('円', '').replace('%', '')
                            numeric_value = float(value)
                            if indicator_type == "occupancy_pct":
                                if 0 <= numeric_value <= 100:
                                    monthly_data[month_key][indicator_type] = numeric_value
                            elif indicator_type in ["adr_jpy", "revpar_jpy"]:
                                if 1000 <= numeric_value <= 100000:
                                    monthly_data[month_key][indicator_type] = int(numeric_value)
                            elif indicator_type.startswith("sales_"):
                                if numeric_value > 0:
                                    monthly_data[month_key][indicator_type] = int(numeric_value)
                        except (ValueError, TypeError):
                            continue
        return monthly_data
    def process_excel_file(self, year: int) -> Optional[Dict]:
        """個別Excelファイルを処理"""
        file_path = self.data_dir / f"jhr_{year}_hotel_performance.xlsx"
        if not file_path.exists():
            logger.warning(f"{year}年ファイル未発見: {file_path}")
            return None
        logger.info(f"{year}年ファイル処理開始: {file_path}")
        try:
            excel_file = pd.ExcelFile(file_path)
            main_sheet = None
            priority_keywords = ["変動賃料等導入", "HMJ", "ACCOR"]
            for keyword in priority_keywords:
                for sheet_name in excel_file.sheet_names:
                    if keyword in sheet_name:
                        main_sheet = sheet_name
                        break
                if main_sheet:
                    break
            if not main_sheet:
                for sheet_name in excel_file.sheet_names:
                    if "注意" not in sheet_name:
                        main_sheet = sheet_name
                        break
            if not main_sheet:
                logger.error(f"{year}年: 適切なシートが見つかりません")
                return None
            logger.info(f"{year}年: シート '{main_sheet}' を使用")
            df = pd.read_excel(file_path, sheet_name=main_sheet, header=None)
            monthly_data = self.extract_monthly_data(df, year)
            annual_summary = self.calculate_annual_summary(monthly_data)
            return {
                "year": year,
                "data_source": str(file_path),
                "sheet_used": main_sheet,
                "portfolio_type": "ホテル運営実績",
                "monthly_data": monthly_data,
                "annual_summary": annual_summary,
                "extraction_date": datetime.now().strftime("%Y-%m-%d")
            }
        except Exception as e:
            logger.error(f"{year}年ファイル処理エラー: {e}")
            return None
    def calculate_annual_summary(self, monthly_data: Dict) -> Dict:
        """月次データから年間サマリーを算出"""
        valid_months = []
        for month_data in monthly_data.values():
            if month_data.get('occupancy_pct') is not None:
                valid_months.append(month_data)
        if not valid_months:
            return {
                "occupancy_avg_pct": None,
                "adr_avg_jpy": None,
                "revpar_avg_jpy": None,
                "sales_total_annual_mil_jpy": None
            }
        occupancies = [m['occupancy_pct'] for m in valid_months if m['occupancy_pct'] is not None]
        adrs = [m['adr_jpy'] for m in valid_months if m['adr_jpy'] is not None]
        revpars = [m['revpar_jpy'] for m in valid_months if m['revpar_jpy'] is not None]
        sales = [m['sales_total_mil_jpy'] for m in valid_months if m['sales_total_mil_jpy'] is not None]
        return {
            "occupancy_avg_pct": round(sum(occupancies) / len(occupancies), 1) if occupancies else None,
            "adr_avg_jpy": round(sum(adrs) / len(adrs)) if adrs else None,
            "revpar_avg_jpy": round(sum(revpars) / len(revpars)) if revpars else None,
            "sales_total_annual_mil_jpy": sum(sales) if sales else None
        }
    def generate_comprehensive_yaml(self) -> str:
        """包括的YAMLファイルを生成"""
        logger.info("11年分データ処理開始")
        all_data = {}
        successful_years = []
        for year in range(2015, 2026):
            year_data = self.process_excel_file(year)
            if year_data:
                all_data[year] = year_data
                successful_years.append(year)
        logger.info(f"処理成功: {len(successful_years)}年分 {successful_years}")
        yaml_data = {
            "jhr_comprehensive_kpi": {
                "schema_version": "3.0",
                "description": "JHR 11年間実績KPIデータベース（2015-2025）",
                "source": {
                    "primary_url": "https://www.jhrth.co.jp/ja/portfolio/review.html",
                    "ir_library_url": "https://www.jhrth.co.jp/ja/ir/library.html",
                    "last_updated": datetime.now().strftime("%Y-%m-%d")
                },
                "coverage_period": {
                    "start_year": min(successful_years) if successful_years else 2015,
                    "end_year": max(successful_years) if successful_years else 2025,
                    "total_years": len(successful_years)
                },
                "data_definitions": {
                    "metrics": {
                        "occupancy_pct": "客室稼働率（%）",
                        "adr_jpy": "ADR - Average Daily Rate（円）",
                        "revpar_jpy": "RevPAR - Revenue Per Available Room（円）",
                        "sales_total_mil_jpy": "売上高合計（百万円）",
                        "sales_lodging_mil_jpy": "宿泊部門売上高（百万円）",
                        "sales_fnb_mil_jpy": "料飲部門売上高（百万円）",
                        "sales_other_mil_jpy": "その他売上高（百万円）"
                    },
                    "geographical_regions": [
                        "北海道", "東京", "関東（東京除く）", "大阪", 
                        "関西（大阪除く）", "中国", "九州", "沖縄"
                    ]
                },
                "datasets": {}
            }
        }
        for year in sorted(successful_years):
            year_data = all_data[year]
            yaml_data["jhr_comprehensive_kpi"]["datasets"][str(year)] = {
                "portfolio_type": year_data["portfolio_type"],
                "data_availability": "実績値（Excelファイルより抽出）",
                "excel_source": year_data["data_source"],
                "sheet_used": year_data["sheet_used"],
                "extraction_date": year_data["extraction_date"],
                "monthly_data": year_data["monthly_data"],
                "annual_summary": year_data["annual_summary"]
            }
            special_notes = []
            if 2020 <= year <= 2022:
                special_notes.append("COVID-19パンデミック影響期")
            elif year == 2019:
                special_notes.append("ラグビーワールドカップ開催")
            elif year >= 2023:
                special_notes.append("インバウンド需要回復期")
            if special_notes:
                yaml_data["jhr_comprehensive_kpi"]["datasets"][str(year)]["special_notes"] = special_notes
        yaml_data["jhr_comprehensive_kpi"]["metadata"] = {
            "created_date": datetime.now().strftime("%Y-%m-%d"),
            "created_by": "JHR KPIデータ取得自動化システム",
            "purpose": "JHR 11年間実績KPI分析用データベース",
            "data_completeness": f"{len(successful_years)}年分の実績値取得済み",
            "total_expected_records": len(successful_years) * 12,
            "successful_extractions": sum(
                len([m for m in data["monthly_data"].values() if m.get('occupancy_pct') is not None])
                for data in all_data.values()
            )
        }
        yaml_output = yaml.dump(yaml_data, default_flow_style=False, allow_unicode=True, 
                               sort_keys=False, width=120, indent=2)
        return yaml_output
def main():
    """メイン実行関数"""
    generator = JHRYAMLGenerator()
    logger.info("YAMLファイル生成開始")
    yaml_content = generator.generate_comprehensive_yaml()
    output_file = Path("jhr_11year_comprehensive_kpi.yaml")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(yaml_content)
    logger.info(f"YAMLファイル生成完了: {output_file}")
    print(f"\n✅ JHR 11年間実績KPIデータベース作成完了")
    print(f"📄 出力ファイル: {output_file}")
    print(f"📊 データ範囲: 2015-2025年")
if __name__ == "__main__":
    main()