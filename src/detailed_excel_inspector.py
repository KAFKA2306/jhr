#!/usr/bin/env python3
"""
Excelファイルの詳細構造調査スクリプト
"""

import pandas as pd
import numpy as np

def inspect_excel_detailed(filepath, sheet_name):
    """Excelシートの詳細調査"""
    print(f"\n=== {filepath} - {sheet_name} 詳細調査 ===")
    
    df = pd.read_excel(filepath, sheet_name=sheet_name, header=None)
    print(f"シート形状: {df.shape}")
    
    # 全データを確認（最初の50行）
    print("\n--- 最初の50行 ---")
    for i in range(min(50, len(df))):
        row = df.iloc[i]
        row_data = [str(val) if pd.notna(val) else "NaN" for val in row]
        print(f"行{i:2d}: {row_data[:10]}")  # 最初の10列
        
        # 数値が多く含まれる行を特定
        numeric_count = sum(1 for val in row if pd.notna(val) and str(val).replace('.', '').replace(',', '').isdigit())
        if numeric_count >= 5:
            print(f"    ↑ 数値データ行 (数値{numeric_count}個)")
        
        # KPI指標キーワードを含む行を特定
        row_text = ' '.join(row_data).lower()
        kpi_keywords = ['客室稼働率', '稼働率', 'adr', 'revpar', '売上']
        for keyword in kpi_keywords:
            if keyword in row_text:
                print(f"    ↑ KPI行: {keyword}")
                break

def main():
    # 2020年のHMJシートを調査
    filepath = "data/jhr_2020_hotel_performance.xlsx"
    
    excel_file = pd.ExcelFile(filepath)
    print(f"2020年ファイル: {excel_file.sheet_names}")
    
    # HMJグループホテルシートを調査
    target_sheet = "HMJグループホテル"
    inspect_excel_detailed(filepath, target_sheet)

if __name__ == "__main__":
    main()