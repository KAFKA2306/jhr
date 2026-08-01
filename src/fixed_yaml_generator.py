from __future__ import annotations

import argparse
import hashlib
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import yaml


class ExtractionError(RuntimeError):
    pass


KPI_KEYS = (
    "occupancy_pct",
    "adr_jpy",
    "revpar_jpy",
    "sales_total_mil_jpy",
)


@dataclass(frozen=True)
class RowObservation:
    row_index: int
    kpi: str
    values: tuple[float | None, ...]


def _empty_months() -> dict[str, dict[str, float | None]]:
    return {
        f"{month:02d}": {key: None for key in KPI_KEYS}
        for month in range(1, 13)
    }


def _number(value: Any) -> float | None:
    if pd.isna(value) or str(value).strip() in {"", "-", "—", "N/A"}:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _normalize_value(kpi: str, value: Any) -> float | None:
    number = _number(value)
    if number is None:
        return None
    if kpi == "occupancy_pct":
        if 0 <= number <= 1:
            number *= 100
        if not 0 <= number <= 100:
            raise ExtractionError(f"occupancy out of range: {number}")
        return round(number, 1)
    if kpi in {"adr_jpy", "revpar_jpy", "sales_total_mil_jpy"}:
        if number < 0:
            raise ExtractionError(f"negative {kpi}: {number}")
        return number
    raise ExtractionError(f"unknown KPI: {kpi}")


def _kpi_from_label(label: str) -> str | None:
    normalized = label.replace(" ", "")
    if "客室稼働率" in normalized or "稼働率" in normalized:
        return "occupancy_pct"
    if "RevPAR" in label:
        return "revpar_jpy"
    if "ADR" in label:
        return "adr_jpy"
    if "売上高" in normalized or normalized.startswith("売上"):
        return "sales_total_mil_jpy"
    return None


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class AuditedJHRDataExtractor:
    """公式集計行だけを公開系列へ変換する、fail-closedな抽出器。"""

    def __init__(self, data_dir: str | Path = "data") -> None:
        self.data_dir = Path(data_dir)

    @staticmethod
    def _select_sheet(sheet_names: Iterable[str], year: int) -> str:
        names = list(sheet_names)
        if year >= 2024:
            matches = [name for name in names if "変動賃料等導入28ホテル" in name]
        elif year == 2019:
            matches = [name for name in names if "変動賃料等導入" in name]
        else:
            matches = [name for name in names if "HMJ" in name]
        if len(matches) != 1:
            raise ExtractionError(
                f"{year}: expected one KPI sheet, found {matches or 'none'}"
            )
        return matches[0]

    @staticmethod
    def _row_values(row: pd.Series, kpi: str) -> tuple[float | None, ...]:
        return tuple(
            _normalize_value(kpi, row.iloc[index]) if index < len(row) else None
            for index in range(2, 14)
        )

    def _candidate_rows(
        self, df: pd.DataFrame, year: int
    ) -> list[RowObservation]:
        observations: list[RowObservation] = []
        western = f"{year}年"
        era = f"平成{year - 1988}年" if 1989 <= year <= 2018 else None
        for index in range(len(df)):
            row = df.iloc[index]
            label = "" if pd.isna(row.iloc[0]) else str(row.iloc[0])
            kpi = _kpi_from_label(label)
            if not kpi:
                continue
            year_label = "" if len(row) < 2 or pd.isna(row.iloc[1]) else str(row.iloc[1])
            if western not in year_label and (not era or era not in year_label):
                continue
            observations.append(RowObservation(index, kpi, self._row_values(row, kpi)))
        return observations

    @staticmethod
    def _source_aggregate(
        observations: list[RowObservation], year: int
    ) -> tuple[dict[str, dict[str, float | None]], dict[str, Any]]:
        months = _empty_months()
        rows_by_kpi: dict[str, list[RowObservation]] = {key: [] for key in KPI_KEYS}
        for observation in observations:
            rows_by_kpi[observation.kpi].append(observation)
        duplicate = {
            key: [row.row_index for row in rows]
            for key, rows in rows_by_kpi.items()
            if len(rows) > 1
        }
        if duplicate:
            raise ExtractionError(
                f"{year}: multiple candidate aggregate rows for the same KPI: {duplicate}"
            )
        if not any(rows_by_kpi.values()):
            raise ExtractionError(f"{year}: no source aggregate rows found")
        for kpi, rows in rows_by_kpi.items():
            if not rows:
                continue
            for month, value in enumerate(rows[0].values, start=1):
                months[f"{month:02d}"][kpi] = value
        return months, {
            "aggregation_semantics": "source_reported_aggregate_row",
            "portfolio_weighted": "as_reported_by_source_not_recomputed",
            "source_rows": {
                key: [row.row_index for row in rows]
                for key, rows in rows_by_kpi.items()
            },
        }

    @staticmethod
    def _quarantine_individual_rows(
        observations: list[RowObservation], year: int
    ) -> dict[str, Any]:
        rows_by_kpi = {
            key: [row.row_index for row in observations if row.kpi == key]
            for key in KPI_KEYS
        }
        return {
            "year": year,
            "publication_status": "quarantined_no_verified_portfolio_weights",
            "monthly_data": _empty_months(),
            "annual_summary": None,
            "candidate_rows": rows_by_kpi,
            "reason": (
                "The source contains individual-hotel rows. Occupancy requires "
                "available-room weights, ADR requires sold-room weights, and RevPAR "
                "requires available-room or room-revenue inputs. Equal-weight means "
                "are not a portfolio KPI and are therefore not calculated."
            ),
        }

    @staticmethod
    def _quality_flags(
        months: dict[str, dict[str, float | None]]
    ) -> list[dict[str, Any]]:
        flags: list[dict[str, Any]] = []
        for month, data in months.items():
            occupancy = data.get("occupancy_pct")
            adr = data.get("adr_jpy")
            revpar = data.get("revpar_jpy")
            if occupancy is not None and adr is not None and revpar is not None:
                implied = adr * occupancy / 100
                tolerance = max(100.0, abs(revpar) * 0.05)
                if abs(implied - revpar) > tolerance:
                    flags.append(
                        {
                            "month": month,
                            "flag": "revpar_identity_mismatch",
                            "reported_revpar": revpar,
                            "implied_revpar": round(implied, 3),
                            "severity": "warning",
                        }
                    )
        return flags

    @staticmethod
    def _annual_summary(
        months: dict[str, dict[str, float | None]]
    ) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "method": "arithmetic_mean_of_available_source_aggregate_months",
            "official_annual_value": False,
        }
        for key in ("occupancy_pct", "adr_jpy", "revpar_jpy"):
            values = [data[key] for data in months.values() if data[key] is not None]
            summary[f"{key}_mean"] = round(sum(values) / len(values), 3) if values else None
            summary[f"{key}_months"] = len(values)
        sales = [
            data["sales_total_mil_jpy"]
            for data in months.values()
            if data["sales_total_mil_jpy"] is not None
        ]
        summary["sales_total_available_months_mil_jpy"] = round(sum(sales), 3) if sales else None
        summary["sales_total_months"] = len(sales)
        return summary

    def process_excel_file(self, year: int) -> dict[str, Any]:
        path = self.data_dir / f"jhr_{year}_hotel_performance.xlsx"
        if not path.exists():
            raise ExtractionError(f"Missing source file: {path}")
        excel = pd.ExcelFile(path)
        sheet = self._select_sheet(excel.sheet_names, year)
        frame = pd.read_excel(path, sheet_name=sheet, header=None)
        observations = self._candidate_rows(frame, year)
        common = {
            "year": year,
            "source_file": str(path),
            "source_sha256": _file_sha256(path),
            "sheet_used": sheet,
            "extracted_at_utc": datetime.now(timezone.utc).isoformat(),
        }

        if year < 2024:
            return {**common, **self._quarantine_individual_rows(observations, year)}

        months, semantics = self._source_aggregate(observations, year)
        coverage = {
            key: sum(data[key] is not None for data in months.values())
            for key in KPI_KEYS
        }
        if max(coverage.values(), default=0) == 0:
            raise ExtractionError(f"{year}: no valid source aggregate observations")
        return {
            **common,
            "monthly_data": months,
            "coverage_months_by_kpi": coverage,
            "aggregation": semantics,
            "annual_summary": self._annual_summary(months),
            "quality_flags": self._quality_flags(months),
            "publication_status": "source_aggregate_observation",
        }

    def generate_yaml(
        self, start_year: int = 2015, end_year: int = 2025
    ) -> str:
        if start_year > end_year:
            raise ValueError("start_year must not exceed end_year")
        datasets: dict[str, Any] = {}
        for year in range(start_year, end_year + 1):
            try:
                datasets[str(year)] = self.process_excel_file(year)
            except ExtractionError as exc:
                datasets[str(year)] = {
                    "year": year,
                    "publication_status": "quarantined_extraction_error",
                    "monthly_data": _empty_months(),
                    "annual_summary": None,
                    "reason": str(exc),
                }

        quarantined = [
            year
            for year, data in datasets.items()
            if str(data["publication_status"]).startswith("quarantined")
        ]
        document = {
            "jhr_kpi_audited": {
                "schema_version": "6.0",
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "source_page": "https://www.jhrth.co.jp/ja/portfolio/review.html",
                "coverage": {
                    "start_year": start_year,
                    "end_year": end_year,
                    "requested_years": end_year - start_year + 1,
                },
                "publication_status": "partially_quarantined" if quarantined else "source_observations",
                "quarantined_years": quarantined,
                "warning": (
                    "Individual-hotel rows are never averaged into portfolio KPIs. "
                    "Only source-reported aggregate rows are publishable."
                ),
                "datasets": datasets,
            }
        }
        return yaml.safe_dump(document, allow_unicode=True, sort_keys=False)


FixedJHRDataExtractor = AuditedJHRDataExtractor


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--start-year", type=int, default=2015)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--output", default="jhr_audited_kpi.yaml")
    args = parser.parse_args()
    extractor = AuditedJHRDataExtractor(args.data_dir)
    content = extractor.generate_yaml(args.start_year, args.end_year)
    Path(args.output).write_text(content, encoding="utf-8")
    print(args.output)


if __name__ == "__main__":
    main()
