# JHRホテル業績KPI抽出

[![Audit fixes](https://github.com/KAFKA2306/jhr/actions/workflows/audit-fixes.yml/badge.svg)](https://github.com/KAFKA2306/jhr/actions/workflows/audit-fixes.yml)

JHR公式Excelから稼働率、ADR、RevPAR、売上高を抽出し、出典・対象範囲・集計意味を保存する研究プロジェクトです。

## 現在の重要な状態

**2015〜2023年のホテル別行から作った稼働率・ADR・RevPARは、客室数や販売室数で重み付けしていない単純平均です。公式ポートフォリオKPIとして扱わず、`quarantined_equal_weight_hotel_summary`へ隔離します。**

2024年以後に公式資料上の集計行を一意に特定できた場合だけ、`source_aggregate_observation`として保存します。それでも対象ホテル群・定義・訂正版の確認は必要です。

## 監査で確認・修正した問題

- ホテル別稼働率・ADR・RevPARを単純平均しながら「28ホテル集計」「ポートフォリオ実績」と表示
- 0%稼働や売上0を欠損扱い
- 月次ADR・稼働率を単純平均した値を公式年間KPIのように表示
- 同じKPIに複数候補行があっても後勝ちで上書き
- 抽出失敗年度を黙って除外
- 1か月以上取れれば「完全抽出済み」と表示
- 部分年度も「完全データ」と表示
- URL周辺500文字から年度を推測してExcelファイルを自動対応付け
- ダウンロードファイルの形式・容量・ハッシュを未検証
- 件数を固定して「126か月」「100%成功」と表示

現在は次の挙動へ変更しています。

- 0を有効な観測値として保存
- 公式集計行はKPIごとに一意でなければ停止
- ホテル別行の単純平均と売上合計を別の集計意味として記録
- `portfolio_weighted: false`と比較不能理由を保存
- KPIごとの有効月数を保存
- 年平均は`arithmetic_mean_of_available_months`と明記
- RevPARと`ADR × 稼働率`の不整合を品質フラグ化
- 元ExcelのSHA-256、シート、抽出時刻を保存
- 指定年度のファイル欠落・抽出失敗はエラーで停止
- HTML周辺文によるURL・年度推測を廃止
- JHR公式ドメイン上の明示URLだけを取得
- XLSX/XLSシグネチャ、最大容量、任意の期待ハッシュを検証

## 公式資料

- ポートフォリオレビュー  
  https://www.jhrth.co.jp/ja/portfolio/review.html
- IRライブラリ  
  https://www.jhrth.co.jp/ja/ir/library.html

## Excel取得

年度とURLを利用者が公式ページで確認して明示します。

```bash
python src/get.py \
  --year 2024 \
  --url "https://www.jhrth.co.jp/file/example.xlsx" \
  --expected-sha256 "確認済みハッシュ"
```

自動的な年度推測や全ファイル一括推測取得はありません。取得記録は`data/source_manifest.json`へ保存します。

## 抽出

```bash
python src/fixed_yaml_generator.py \
  --data-dir data \
  --start-year 2015 \
  --end-year 2025 \
  --output jhr_audited_kpi.yaml
```

出力例:

```yaml
publication_status: partially_quarantined
quarantined_years:
  - "2015"
  - "2016"
datasets:
  "2020":
    source_sha256: "..."
    coverage_months_by_kpi:
      occupancy_pct: 12
      adr_jpy: 12
    aggregation:
      portfolio_weighted: false
      comparability_status: not_comparable_to_portfolio_aggregate_without_...
    publication_status: quarantined_equal_weight_hotel_summary
```

## なぜ単純平均では公式KPIにならないか

ホテルAが100室、ホテルBが1,000室の場合、両ホテルの稼働率を同じ重みで平均すると、販売可能室数に基づくポートフォリオ稼働率とは一致しません。

正しい再集計には、月・ホテルごとに少なくとも次が必要です。

- 販売可能客室数
- 販売客室数
- 客室売上
- 対象ホテルの在籍期間
- 休館・改装・取得・売却の扱い

```text
ポートフォリオ稼働率 = Σ販売客室数 / Σ販売可能客室数
ポートフォリオADR    = Σ客室売上 / Σ販売客室数
ポートフォリオRevPAR = Σ客室売上 / Σ販売可能客室数
```

これらの分母がない場合、ホテル別KPIの平均を公式集計へ変換できません。

## テスト

```bash
python -m unittest discover -s tests -v
```

- 0%稼働を欠損にしない
- ホテル別値を非加重平均として表示
- 公式集計候補行の重複を拒否
- 売上は合計、ADRは平均という現在の変換意味を検証
- RevPAR恒等式の不整合を品質フラグ化

## 利用上の注意

- 公式Excelを一次情報として優先してください
- 旧`jhr_11year_fixed_kpi.yaml`と`jhr_11year_comprehensive_kpi.yaml`は監査前成果物です
- 隔離年を時系列グラフ・投資分析へ接続しないでください
- 対象ホテル群や集計定義が違う年度を単純比較しないでください
- 本データは投資助言や将来業績予測ではありません

**README最終監査:** 2026-08-02
