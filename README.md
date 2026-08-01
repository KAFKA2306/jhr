# JHRホテル業績KPIデータベース

**リポジトリ:** https://github.com/KAFKA2306/jhr

ジャパン・ホテル・リート投資法人（JHR）が公開するホテル運営実績のExcel資料から、稼働率、ADR、RevPAR、売上高などを抽出し、年度をまたいで比較できるYAMLへ変換するプロジェクトです。

公式Excelの形式は年度によって変わるため、列位置だけで機械的に抽出せず、対象シート、ホテル範囲、集計範囲、単位、期間を保存します。

## 対象KPI

- 稼働率（Occupancy）
- ADR（平均客室単価）
- RevPAR（販売可能客室1室当たり売上）
- ホテル売上高
- 年度・月次集計

KPIの定義と対象ホテル群が年度によって同一とは限りません。

## 主なデータ源

- JHR公式ポートフォリオレビュー  
  https://www.jhrth.co.jp/ja/portfolio/review.html
- JHR公式IRライブラリ  
  https://www.jhrth.co.jp/ja/ir/library.html

ダウンロードしたExcelには、取得URL、公開日、取得日、ファイルハッシュを付けることを推奨します。

## 主な構成

```text
jhr/
├── data/                              # 取得した公式Excel
├── src/get.py                         # Excel取得処理
├── src/fixed_yaml_generator.py        # 主な抽出処理
├── src/create_comprehensive_yaml.py   # 代替抽出処理
├── src/detailed_excel_inspector.py    # Excel構造の調査
├── jhr_11year_fixed_kpi.yaml          # 主な出力
└── jhr_11year_comprehensive_kpi.yaml  # 代替出力
```

ファイル名に`11year`が含まれていますが、最新の対象年度・月数は生成YAMLのメタデータを正としてください。

## 実行

### 保存済みExcelからYAMLを生成

```bash
python src/fixed_yaml_generator.py
```

### 公式資料を取得

```bash
python src/get.py --download-all
```

取得とYAML更新をまとめて行う構成では:

```bash
python src/get.py --download-all --update-yaml
```

実行前に現在の公式ページ構造、アクセス頻度、保存先を確認してください。

## 形式変更への対応

過去資料には、個別ホテル表示、対象ホテル群の変更、集計表示など複数の形式があります。

抽出時に記録する項目:

```yaml
source:
  url: "https://..."
  published_at: YYYY-MM-DD
  retrieved_at: YYYY-MM-DD
  file_sha256: "..."
  sheet_name: "..."
scope:
  reporting_period: YYYY-MM
  hotel_set: "資料に記載された対象群"
  aggregation: individual | portfolio_total | selected_hotels
metrics:
  occupancy_pct: null
  adr_jpy: null
  revpar_jpy: null
  sales_million_jpy: null
```

## データ品質の確認

- Excelの対象年月とファイル名が一致するか
- 稼働率が小数か百分率か
- ADR・RevPAR・売上高の単位が正しいか
- 対象ホテル数が前年と変わっていないか
- 個別ホテル値と合計値を混ぜていないか
- 欠損を0として保存していないか
- RevPARが`稼働率 × ADR`と概ね整合するか
- 訂正版のExcelが公開されていないか
- 同じ月を複数ファイルから重複取得していないか

## READMEから削除した固定値

以前のREADMEには「126か月」「抽出成功率100%」「11年間すべて抽出完了」などが記載されていました。データ更新後に自動再計算される保証がないため、固定値としては削除しました。

最新の件数、対象期間、欠損率は、生成YAMLを検証するスクリプトから算出してください。

## 特殊期間の解釈

感染症、災害、大規模イベント、ホテル取得・売却、改装、休館などはKPIへ影響します。数値変化の理由を自動的に一つへ帰属させず、JHRの公式説明資料を確認してください。

## 利用上の注意

- JHRの公式資料が一次情報です
- 本リポジトリのYAMLは抽出・変換した二次データです
- 公式資料と変換データが異なる場合は公式資料を優先してください
- ホテル群や集計基準が違う年度を単純比較しないでください
- 本データは投資助言、REIT評価、将来業績予測ではありません

**README最終監査:** 2026-08-01
