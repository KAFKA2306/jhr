# JHR Hotel Performance KPI Database (2015-2025)

A comprehensive 11-year database of Japan Hotel REIT Investment Corporation (JHR) hotel performance KPIs extracted from official Excel files.

## 📊 Dataset Overview

- **Coverage Period**: 2015-2025 (11 years)
- **Data Source**: Official JHR IR library Excel files
- **Total Records**: 126 months of actual performance data
- **Success Rate**: 100% (11/11 years extracted)

## 🎯 Key Performance Indicators

- **Occupancy Rate (%)**: Monthly room occupancy percentages
- **ADR (JPY)**: Average Daily Rate in Japanese Yen
- **RevPAR (JPY)**: Revenue Per Available Room in Japanese Yen
- **Sales (Million JPY)**: Total monthly sales revenue

## 📁 Project Structure

```
jhr/
├── README.md                              # This file
├── data/                                  # Excel source files (11 years)
│   ├── jhr_2015_hotel_performance.xlsx
│   ├── jhr_2016_hotel_performance.xlsx
│   └── ... (through 2025)
├── src/                                   # Processing scripts
│   ├── get.py                            # Automated Excel download
│   ├── fixed_yaml_generator.py           # Corrected extraction logic
│   ├── create_comprehensive_yaml.py      # Alternative extraction method
│   └── detailed_excel_inspector.py       # Excel structure analysis
├── jhr_11year_fixed_kpi.yaml            # Primary output (corrected)
└── jhr_11year_comprehensive_kpi.yaml    # Alternative output
```

## 🚀 Usage

### Quick Start

1. **Generate YAML Database**:
   ```bash
   python3 src/fixed_yaml_generator.py
   ```

2. **Download Latest Excel Files**:
   ```bash
   python3 src/get.py --download-all
   ```

### Data Files

- **Primary**: `jhr_11year_fixed_kpi.yaml` - Recommended dataset with corrected extraction logic
- **Alternative**: `jhr_11year_comprehensive_kpi.yaml` - Alternative extraction approach

## 📈 Data Quality & Coverage

### Successful Extractions
- ✅ **2015-2018**: Legacy HMJ format (individual hotels)
- ✅ **2019**: Variable rent 21-hotel format (individual → aggregated)
- ✅ **2020-2023**: COVID-period HMJ format (individual → aggregated)  
- ✅ **2024-2025**: Modern 28-hotel aggregated format

### Special Periods
- **2020-2022**: COVID-19 impact period (low occupancy rates documented)
- **2019**: Rugby World Cup impact
- **2023+**: Inbound tourism recovery period

## 🛠️ Technical Features

### Multi-Format Excel Processing
- Handles 4 different Excel sheet structures across 11 years
- Automatic sheet detection and format adaptation
- Robust data validation and conversion

### Data Extraction Capabilities
- Decimal to percentage conversion (0.816 → 81.6%)
- Multi-hotel aggregation for individual hotel data
- KPI continuation pattern handling (2019 format)
- COVID-period data normalization

## 📊 Sample Data

```yaml
'2024':
  monthly_data:
    '01':
      occupancy_pct: 87.2
      adr_jpy: 9168
      revpar_jpy: 7990
      sales_total_mil_jpy: 46
  annual_summary:
    occupancy_avg_pct: 87.9
    adr_avg_jpy: 11464
    sales_total_annual_mil_jpy: 672
```

## 🔄 Updates

The dataset can be updated by running:
```bash
python3 src/get.py --download-all --update-yaml
```

This will:
1. Download latest Excel files from JHR IR library
2. Process new data using established extraction patterns
3. Regenerate YAML database with updated information

## 📋 Data Sources

- **Primary Source**: [JHR Official Portfolio Review](https://www.jhrth.co.jp/ja/portfolio/review.html)
- **IR Library**: [JHR IR Document Library](https://www.jhrth.co.jp/ja/ir/library.html)
- **Data Format**: Official monthly performance Excel files

## ⚠️ Notes

- All values represent **actual performance data**, not estimates
- 2025 data is partial (January-June available as of extraction date)
- Currency values are in Japanese Yen (JPY)
- Sales figures are in millions of JPY

---

**Generated**: 2025-09-07  
**Data Range**: 2015-2025 (11 years)  
**Total Coverage**: 126 months of hotel performance data