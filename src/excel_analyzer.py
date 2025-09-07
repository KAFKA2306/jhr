#!/usr/bin/env python3
"""
Excelファイルの内容を分析して実際のデータ構造を確認するスクリプト
"""

import pandas as pd
import sys
from pathlib import Path

def analyze_excel(file_path):
    """Excelファイルを詳細分析"""
    print(f"\n=== {file_path} 分析結果 ===")
    
    try:
        # 全シートを読み込み
        excel_file = pd.ExcelFile(file_path)
        print(f"シート数: {len(excel_file.sheet_names)}")
        print(f"シート名: {excel_file.sheet_names}")
        
        for sheet_name in excel_file.sheet_names:
            if "注意" in sheet_name or sheet_name == "ご利用上の注意":
                continue
                
            print(f"\n--- {sheet_name} ---")
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                print(f"形状: {df.shape}")
                print("最初の10行:")
                print(df.head(10).to_string())
                
                # 数値データがありそうな行を探す
                for i in range(min(50, len(df))):
                    row = df.iloc[i]
                    if any(pd.to_numeric(val, errors='coerce') is not pd.NA for val in row if pd.notna(val)):
                        numeric_count = sum(1 for val in row if pd.to_numeric(val, errors='coerce') is not pd.NA and pd.notna(val))
                        if numeric_count >= 3:
                            print(f"数値データ行 {i}: {row.tolist()}")
                            break
                        
            except Exception as e:
                print(f"シート '{sheet_name}' 読み込みエラー: {e}")
        
    except Exception as e:
        print(f"ファイル読み込みエラー: {e}")

if __name__ == "__main__":
    data_dir = Path("data")
    
    # 最新と最古のファイルを分析
    files_to_analyze = [
        "jhr_2025_hotel_performance.xlsx",
        "jhr_2024_hotel_performance.xlsx", 
        "jhr_2015_hotel_performance.xlsx"
    ]
    
    for filename in files_to_analyze:
        filepath = data_dir / filename
        if filepath.exists():
            analyze_excel(filepath)
        else:
            print(f"ファイルが見つかりません: {filepath}")