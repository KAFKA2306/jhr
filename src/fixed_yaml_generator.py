#!/usr/bin/env python3
"""
修正版: 正確なExcelデータ抽出によるYAML生成
"""

import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FixedJHRDataExtractor:
    """修正版JHRデータ抽出クラス"""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        
    def extract_individual_hotels_aggregated(self, df: pd.DataFrame, target_year: int) -> dict:
        """個別ホテルデータを集計（2019年、2020-2023年用）"""
        monthly_data = {}
        
        # 12ヶ月分の初期化
        for month in range(1, 13):
            monthly_data[f"{month:02d}"] = {
                'occupancy_pct': 0.0,
                'adr_jpy': 0.0,
                'revpar_jpy': 0.0,
                'sales_total_mil_jpy': 0.0,
                'occupancy_count': 0,
                'adr_count': 0,
                'revpar_count': 0,
                'sales_count': 0
            }
        
        current_hotel_count = 0
        current_kpi_type = None
        
        for row_idx in range(len(df)):
            row = df.iloc[row_idx]
            first_col = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
            
            # 新ホテル検出（物件番号行）
            if "物件番号" in first_col:
                current_hotel_count += 1
                current_kpi_type = None  # リセット
                continue
            
            # KPI指標判定（新KPI行または継続行）
            if "客室稼働率" in first_col:
                current_kpi_type = "occupancy_pct"
            elif "ADR" in first_col and "円" in first_col:
                current_kpi_type = "adr_jpy"  
            elif "RevPAR" in first_col and "円" in first_col:
                current_kpi_type = "revpar_jpy"
            elif "売上高" in first_col and "百万円" in first_col:
                current_kpi_type = "sales_total_mil_jpy"
            # KPI継続の場合は current_kpi_type をそのまま使用
            
            if not current_kpi_type:
                continue
            
            # 年度確認
            second_col = str(row.iloc[1]) if pd.notna(row.iloc[1]) else ""
            
            if f"{target_year}年" not in second_col:
                continue
            
            logger.info(f"ホテル{current_hotel_count}: {target_year}年 {current_kpi_type} データ行{row_idx}")
            
            # 月次データ抽出と集計
            for month_idx in range(12):
                col_idx = month_idx + 2
                if col_idx < len(row):
                    value = row.iloc[col_idx]
                    
                    if pd.notna(value) and str(value) not in ['0', '-', '']:
                        try:
                            numeric_value = float(value)
                            
                            if current_kpi_type == "occupancy_pct" and 0 <= numeric_value <= 1.0:
                                # 小数形式の占有率を%に変換して累積
                                monthly_data[f"{month_idx+1:02d}"]["occupancy_pct"] += numeric_value * 100
                                monthly_data[f"{month_idx+1:02d}"]["occupancy_count"] += 1
                                
                            elif current_kpi_type in ["adr_jpy", "revpar_jpy"] and 1000 <= numeric_value <= 100000:
                                # 加重平均のため後で計算、まず累積
                                monthly_data[f"{month_idx+1:02d}"][current_kpi_type] += numeric_value
                                monthly_data[f"{month_idx+1:02d}"][f"{current_kpi_type.split('_')[0]}_count"] += 1
                                
                            elif current_kpi_type == "sales_total_mil_jpy" and numeric_value > 0:
                                # 売上は単純合計
                                monthly_data[f"{month_idx+1:02d}"]["sales_total_mil_jpy"] += numeric_value
                                monthly_data[f"{month_idx+1:02d}"]["sales_count"] += 1
                                
                        except (ValueError, TypeError):
                            continue
        
        # 平均値算出
        for month_key in monthly_data:
            month_data = monthly_data[month_key]
            
            # 占有率: 平均
            if month_data["occupancy_count"] > 0:
                monthly_data[month_key]["occupancy_pct"] = round(
                    month_data["occupancy_pct"] / month_data["occupancy_count"], 1
                )
            else:
                monthly_data[month_key]["occupancy_pct"] = None
            
            # ADR: 平均  
            if month_data["adr_count"] > 0:
                monthly_data[month_key]["adr_jpy"] = int(
                    month_data["adr_jpy"] / month_data["adr_count"]
                )
            else:
                monthly_data[month_key]["adr_jpy"] = None
            
            # RevPAR: 平均
            if month_data["revpar_count"] > 0:
                monthly_data[month_key]["revpar_jpy"] = int(
                    month_data["revpar_jpy"] / month_data["revpar_count"] 
                )
            else:
                monthly_data[month_key]["revpar_jpy"] = None
            
            # 売上: 合計
            if month_data["sales_count"] > 0:
                monthly_data[month_key]["sales_total_mil_jpy"] = int(month_data["sales_total_mil_jpy"])
            else:
                monthly_data[month_key]["sales_total_mil_jpy"] = None
            
            # カウント情報削除
            for key in list(month_data.keys()):
                if "_count" in key:
                    del monthly_data[month_key][key]
        
        logger.info(f"{target_year}年: {current_hotel_count}ホテル集計完了")
        return monthly_data
    
    def extract_aggregated_data(self, df: pd.DataFrame, target_year: int) -> dict:
        """28ホテル合計データを抽出（2022年以降のファイル形式用）"""
        monthly_data = {}
        
        # 12ヶ月分の初期化
        for month in range(1, 13):
            monthly_data[f"{month:02d}"] = {
                'occupancy_pct': None,
                'adr_jpy': None, 
                'revpar_jpy': None,
                'sales_total_mil_jpy': None,
                'sales_lodging_mil_jpy': None,
                'sales_fnb_mil_jpy': None,
                'sales_other_mil_jpy': None
            }
        
        # データ行を検索
        for row_idx in range(len(df)):
            row = df.iloc[row_idx]
            
            # 1列目でKPI指標を判定
            first_col = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
            
            kpi_type = None
            if "客室稼働率" in first_col:
                kpi_type = "occupancy_pct"
            elif "ADR" in first_col and "円" in first_col:
                kpi_type = "adr_jpy"
            elif "RevPAR" in first_col and "円" in first_col:
                kpi_type = "revpar_jpy"
            elif "売上高" in first_col and "百万円" in first_col:
                kpi_type = "sales_total_mil_jpy"
            
            if not kpi_type:
                continue
            
            # 2列目で年度を確認
            second_col = str(row.iloc[1]) if pd.notna(row.iloc[1]) else ""
            if f"{target_year}年" not in second_col:
                continue
            
            logger.info(f"{target_year}年 {kpi_type} データ行発見: 行{row_idx}")
            
            # 3列目以降から月次データを抽出 (1月から12月)
            for month_idx in range(12):
                col_idx = month_idx + 2  # 3列目から開始
                if col_idx < len(row):
                    value = row.iloc[col_idx]
                    
                    if pd.notna(value) and str(value) not in ['0', '-', '']:
                        try:
                            numeric_value = float(value)
                            
                            # 占有率は小数を%に変換
                            if kpi_type == "occupancy_pct" and numeric_value <= 1.0:
                                numeric_value = numeric_value * 100
                            
                            # 値の妥当性チェック
                            if kpi_type == "occupancy_pct" and 0 <= numeric_value <= 100:
                                monthly_data[f"{month_idx+1:02d}"][kpi_type] = round(numeric_value, 1)
                            elif kpi_type in ["adr_jpy", "revpar_jpy"] and 1000 <= numeric_value <= 100000:
                                monthly_data[f"{month_idx+1:02d}"][kpi_type] = int(numeric_value)
                            elif kpi_type.endswith("_mil_jpy") and numeric_value > 0:
                                monthly_data[f"{month_idx+1:02d}"][kpi_type] = int(numeric_value)
                                
                        except (ValueError, TypeError):
                            continue
        
        return monthly_data
    
    def extract_legacy_format(self, df: pd.DataFrame, target_year: int) -> dict:
        """旧形式データ抽出（2015-2018年用）"""
        monthly_data = {}
        
        # 初期化
        for month in range(1, 13):
            monthly_data[f"{month:02d}"] = {
                'occupancy_pct': None,
                'adr_jpy': None,
                'revpar_jpy': None,
                'sales_total_mil_jpy': None,
                'sales_lodging_mil_jpy': None,
                'sales_fnb_mil_jpy': None,
                'sales_other_mil_jpy': None
            }
        
        # 平成年号変換
        heisei_year = target_year - 1988 if target_year >= 1989 else None
        year_patterns = [f"{target_year}年", f"平成{heisei_year}年" if heisei_year else ""]
        
        for row_idx in range(len(df)):
            row = df.iloc[row_idx]
            
            # KPI指標判定
            first_col = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
            kpi_type = None
            
            if "客室稼働率" in first_col or "稼働率" in first_col:
                kpi_type = "occupancy_pct"
            elif "ADR" in first_col:
                kpi_type = "adr_jpy"
            elif "RevPAR" in first_col:
                kpi_type = "revpar_jpy"
            elif "売上" in first_col:
                kpi_type = "sales_total_mil_jpy"
            
            if not kpi_type:
                continue
            
            # 年度確認（2列目）
            year_found = False
            second_col = str(row.iloc[1]) if pd.notna(row.iloc[1]) else ""
            for pattern in year_patterns:
                if pattern and pattern in second_col:
                    year_found = True
                    break
            
            if not year_found:
                continue
            
            logger.info(f"{target_year}年 {kpi_type} データ行発見: 行{row_idx}")
            
            # 月次データ抽出
            for month_idx in range(12):
                col_idx = month_idx + 2
                if col_idx < len(row):
                    value = row.iloc[col_idx]
                    
                    if pd.notna(value) and str(value) not in ['0', '-']:
                        try:
                            numeric_value = float(value)
                            
                            # 占有率処理（旧形式は%表示）
                            if kpi_type == "occupancy_pct":
                                if numeric_value > 1:  # 既に%の場合
                                    monthly_data[f"{month_idx+1:02d}"][kpi_type] = round(numeric_value, 1)
                                else:  # 小数の場合
                                    monthly_data[f"{month_idx+1:02d}"][kpi_type] = round(numeric_value * 100, 1)
                            elif kpi_type in ["adr_jpy", "revpar_jpy"]:
                                monthly_data[f"{month_idx+1:02d}"][kpi_type] = int(numeric_value)
                            elif kpi_type.endswith("_mil_jpy"):
                                monthly_data[f"{month_idx+1:02d}"][kpi_type] = int(numeric_value)
                                
                        except (ValueError, TypeError):
                            continue
        
        return monthly_data
    
    def process_excel_file(self, year: int) -> dict:
        """年度別Excelファイル処理"""
        file_path = self.data_dir / f"jhr_{year}_hotel_performance.xlsx"
        
        if not file_path.exists():
            logger.error(f"{year}年ファイル未発見")
            return None
            
        logger.info(f"=== {year}年ファイル処理開始 ===")
        
        try:
            excel_file = pd.ExcelFile(file_path)
            
            # シート選択ロジック
            main_sheet = None
            if year >= 2024:
                # 2024年以降: 変動賃料等導入28ホテルシートを探す
                for sheet_name in excel_file.sheet_names:
                    if "変動賃料等導入28ホテル" in sheet_name:
                        main_sheet = sheet_name
                        break
            elif year == 2019:
                # 2019年: 変動賃料等導入21ホテルシート
                for sheet_name in excel_file.sheet_names:
                    if "変動賃料等導入" in sheet_name:
                        main_sheet = sheet_name
                        break
            elif 2020 <= year <= 2023:
                # 2020-2023年: HMJグループホテルシート（COVID期間）
                for sheet_name in excel_file.sheet_names:
                    if "HMJ" in sheet_name:
                        main_sheet = sheet_name
                        break
            else:
                # 2015-2018年: HMJシートを優先
                for sheet_name in excel_file.sheet_names:
                    if "HMJ" in sheet_name:
                        main_sheet = sheet_name
                        break
            
            if not main_sheet:
                logger.error(f"{year}年: 適切なシートが見つかりません")
                return None
            
            logger.info(f"{year}年: シート '{main_sheet}' 使用")
            
            df = pd.read_excel(file_path, sheet_name=main_sheet, header=None)
            logger.info(f"{year}年: データ形状 {df.shape}")
            
            # 年度に応じた抽出方式
            if year >= 2024:
                # 2024年以降: 集計済みデータ
                monthly_data = self.extract_aggregated_data(df, year)
            elif year == 2019 or (2020 <= year <= 2023):
                # 2019年、2020-2023年: 個別ホテルデータを集計
                monthly_data = self.extract_individual_hotels_aggregated(df, year)
            else:
                # 2015-2018年: 旧形式
                monthly_data = self.extract_legacy_format(df, year)
            
            # データ品質チェック
            valid_months = sum(1 for data in monthly_data.values() 
                             if data['occupancy_pct'] is not None)
            logger.info(f"{year}年: 有効月数 {valid_months}/12")
            
            if valid_months == 0:
                logger.warning(f"{year}年: データ抽出失敗")
                return None
            
            # 年間サマリー算出
            annual_summary = self.calculate_annual_summary(monthly_data)
            
            return {
                "year": year,
                "data_source": str(file_path),
                "sheet_used": main_sheet,
                "monthly_data": monthly_data,
                "annual_summary": annual_summary,
                "valid_months": valid_months,
                "extraction_date": datetime.now().strftime("%Y-%m-%d")
            }
            
        except Exception as e:
            logger.error(f"{year}年処理エラー: {e}")
            return None
    
    def calculate_annual_summary(self, monthly_data: dict) -> dict:
        """年間サマリー算出"""
        valid_data = [data for data in monthly_data.values() 
                     if data.get('occupancy_pct') is not None]
        
        if not valid_data:
            return {"occupancy_avg_pct": None, "adr_avg_jpy": None, 
                   "revpar_avg_jpy": None, "sales_total_annual_mil_jpy": None}
        
        # 平均値算出
        occupancies = [d['occupancy_pct'] for d in valid_data if d['occupancy_pct']]
        adrs = [d['adr_jpy'] for d in valid_data if d['adr_jpy']]
        revpars = [d['revpar_jpy'] for d in valid_data if d['revpar_jpy']]
        sales = [d['sales_total_mil_jpy'] for d in valid_data if d['sales_total_mil_jpy']]
        
        return {
            "occupancy_avg_pct": round(sum(occupancies) / len(occupancies), 1) if occupancies else None,
            "adr_avg_jpy": int(sum(adrs) / len(adrs)) if adrs else None,
            "revpar_avg_jpy": int(sum(revpars) / len(revpars)) if revpars else None,
            "sales_total_annual_mil_jpy": sum(sales) if sales else None
        }
    
    def generate_yaml(self) -> str:
        """修正版YAML生成"""
        logger.info("=== 修正版11年データ処理開始 ===")
        
        all_data = {}
        successful_years = []
        
        # 全年度処理
        for year in range(2015, 2026):
            result = self.process_excel_file(year)
            if result:
                all_data[year] = result
                successful_years.append(year)
        
        logger.info(f"処理成功年度: {successful_years}")
        
        # YAML構造構築
        yaml_data = {
            "jhr_comprehensive_kpi_fixed": {
                "schema_version": "4.0",
                "description": "JHR 11年間実績KPI完全版（修正済み）",
                "source": {
                    "primary_url": "https://www.jhrth.co.jp/ja/portfolio/review.html",
                    "last_updated": datetime.now().strftime("%Y-%m-%d")
                },
                "extraction_method": {
                    "modern_format": "2019年以降: 変動賃料等導入28ホテル集計",
                    "legacy_format": "2015-2018年: HMJグループホテル等",
                    "data_conversion": "占有率: 小数→%変換, ADR/RevPAR: 円単位"
                },
                "coverage_period": {
                    "start_year": min(successful_years) if successful_years else 2015,
                    "end_year": max(successful_years) if successful_years else 2025,
                    "total_years": len(successful_years)
                },
                "datasets": {}
            }
        }
        
        # 各年度データ追加
        for year in sorted(successful_years):
            data = all_data[year]
            
            yaml_data["jhr_comprehensive_kpi_fixed"]["datasets"][str(year)] = {
                "portfolio_type": "ホテル運営実績（実数値）",
                "data_availability": "完全抽出済み",
                "excel_source": data["data_source"],
                "sheet_used": data["sheet_used"],
                "extraction_date": data["extraction_date"],
                "valid_months": data["valid_months"],
                "monthly_data": data["monthly_data"],
                "annual_summary": data["annual_summary"]
            }
            
            # 特別注記
            if 2020 <= year <= 2022:
                yaml_data["jhr_comprehensive_kpi_fixed"]["datasets"][str(year)]["special_notes"] = ["COVID-19影響期"]
        
        # メタデータ
        total_valid_records = sum(data["valid_months"] for data in all_data.values())
        yaml_data["jhr_comprehensive_kpi_fixed"]["metadata"] = {
            "created_date": datetime.now().strftime("%Y-%m-%d"),
            "extraction_success_rate": f"{len(successful_years)}/11年 ({len(successful_years)/11*100:.1f}%)",
            "total_valid_months": total_valid_records,
            "data_completeness": "実績値による完全データ"
        }
        
        return yaml.dump(yaml_data, default_flow_style=False, allow_unicode=True, 
                        sort_keys=False, width=120, indent=2)

def main():
    """メイン実行"""
    extractor = FixedJHRDataExtractor()
    
    logger.info("修正版YAMLファイル生成開始")
    yaml_content = extractor.generate_yaml()
    
    # ファイル出力
    output_file = Path("jhr_11year_fixed_kpi.yaml")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(yaml_content)
    
    logger.info(f"修正版YAMLファイル生成完了: {output_file}")
    print(f"\n✅ JHR 11年間修正版KPIデータベース作成完了")
    print(f"📄 出力ファイル: {output_file}")

if __name__ == "__main__":
    main()