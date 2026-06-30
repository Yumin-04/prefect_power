"""
Power Consumption 시계열 데이터 ETL 파이프라인 (Prefect)

데이터 출처: Kaggle Power Consumption of Tetouan City
(10분 간격, 기상변수 + 3개 구역 전력소비량)

흐름:
    extract  -> 원본 CSV 로드
    validate -> 스키마/품질 점검 (결측치, 중복, 시간 연속성, 음수값)
    transform-> 컬럼 정리, 파생 피처 생성, 리샘플링용 정렬
    load     -> 정제된 데이터를 parquet/csv로 저장 + 품질 리포트 저장

실행:
    python etl_flow.py --input powerconsumption.csv --outdir ./output
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact


# --------------------------------------------------------------------------
# Tasks
# --------------------------------------------------------------------------

@task(name="extract-csv", retries=2, retry_delay_seconds=5, log_prints=True)
def extract(input_path: str) -> pd.DataFrame:
    """원본 CSV를 읽어 DataFrame으로 반환."""
    logger = get_run_logger()
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {path}")

    df = pd.read_csv(path)
    logger.info(f"원본 데이터 로드 완료: {df.shape[0]}행 x {df.shape[1]}열")
    return df


@task(name="validate-data", log_prints=True)
def validate(df: pd.DataFrame) -> dict:
    """
    데이터 품질을 점검하고 리포트를 dict로 반환.
    파이프라인을 막을 정도의 치명적 오류가 있으면 예외 발생.
    """
    logger = get_run_logger()
    report: dict = {}

    expected_cols = {
        "Datetime", "Temperature", "Humidity", "WindSpeed",
        "GeneralDiffuseFlows", "DiffuseFlows",
        "PowerConsumption_Zone1", "PowerConsumption_Zone2", "PowerConsumption_Zone3",
    }
    missing_cols = expected_cols - set(df.columns)
    if missing_cols:
        raise ValueError(f"필수 컬럼 누락: {missing_cols}")

    report["row_count"] = int(len(df))
    report["duplicate_rows"] = int(df.duplicated().sum())

    null_counts = df.isna().sum()
    report["null_counts"] = {k: int(v) for k, v in null_counts.items() if v > 0}

    numeric_cols = [c for c in df.columns if c != "Datetime"]
    negative_counts = (df[numeric_cols] < 0).sum()
    report["negative_value_counts"] = {
        k: int(v) for k, v in negative_counts.items() if v > 0
    }

    # 시간 연속성 체크 (10분 간격 가정)
    dt = pd.to_datetime(df["Datetime"])
    diffs = dt.diff().dropna()
    expected_delta = pd.Timedelta(minutes=10)
    irregular = diffs[diffs != expected_delta]
    report["irregular_interval_count"] = int(len(irregular))
    report["time_range"] = [str(dt.min()), str(dt.max())]

    logger.info(f"검증 리포트: {json.dumps(report, ensure_ascii=False, indent=2)}")

    if report["duplicate_rows"] > 0:
        logger.warning(f"중복 행 {report['duplicate_rows']}건 발견 -> transform 단계에서 제거 예정")
    if report["null_counts"]:
        logger.warning(f"결측치 발견: {report['null_counts']} -> transform 단계에서 보간 예정")
    if report["negative_value_counts"]:
        logger.warning(f"음수값 발견(이상치 가능성): {report['negative_value_counts']}")
    if report["irregular_interval_count"] > 0:
        logger.warning(f"시간 간격 불규칙 구간 {report['irregular_interval_count']}건 발견")

    return report


@task(name="transform-data", log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    정제 + 파생 피처 생성:
      - Datetime 파싱, 정렬, 중복 제거
      - 결측치 시간 보간(interpolate)
      - 음수 전력소비량 등 물리적으로 불가능한 값 NaN 처리 후 보간
      - 시간 파생 피처(hour, dayofweek, month, is_weekend) 생성
      - 3개 구역 합산 컬럼(PowerConsumption_Total) 생성
    """
    logger = get_run_logger()
    df = df.copy()

    df["Datetime"] = pd.to_datetime(df["Datetime"])
    before = len(df)
    df = df.drop_duplicates(subset="Datetime").sort_values("Datetime").reset_index(drop=True)
    logger.info(f"중복 제거: {before - len(df)}건 제거됨")

    # 전력소비/기상 컬럼은 음수가 나오면 안 되므로 NaN 처리
    non_negative_cols = [
        "Humidity", "WindSpeed", "GeneralDiffuseFlows", "DiffuseFlows",
        "PowerConsumption_Zone1", "PowerConsumption_Zone2", "PowerConsumption_Zone3",
    ]
    for col in non_negative_cols:
        invalid_mask = df[col] < 0
        if invalid_mask.any():
            logger.warning(f"{col}: 음수값 {invalid_mask.sum()}건을 NaN으로 치환")
            df.loc[invalid_mask, col] = pd.NA

    # 시간 기준 선형 보간 (결측치 + 위에서 NaN 처리한 값)
    numeric_cols = [c for c in df.columns if c != "Datetime"]
    df[numeric_cols] = df[numeric_cols].interpolate(method="linear", limit_direction="both")

    # 시간 파생 피처
    df["hour"] = df["Datetime"].dt.hour
    df["dayofweek"] = df["Datetime"].dt.dayofweek  # 0=월요일
    df["month"] = df["Datetime"].dt.month
    df["is_weekend"] = df["dayofweek"].isin([5, 6]).astype(int)

    # 총 전력소비량 파생 컬럼
    zone_cols = ["PowerConsumption_Zone1", "PowerConsumption_Zone2", "PowerConsumption_Zone3"]
    df["PowerConsumption_Total"] = df[zone_cols].sum(axis=1)

    logger.info(f"transform 완료: 최종 {df.shape[0]}행 x {df.shape[1]}열")
    return df


@task(name="load-data", log_prints=True)
def load(df: pd.DataFrame, outdir: str) -> dict:
    """정제된 데이터를 parquet, csv로 저장."""
    logger = get_run_logger()
    out_path = Path(outdir)
    out_path.mkdir(parents=True, exist_ok=True)

    parquet_path = out_path / "powerconsumption_clean.parquet"
    csv_path = out_path / "powerconsumption_clean.csv"

    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)

    logger.info(f"저장 완료: {parquet_path}, {csv_path}")
    return {"parquet_path": str(parquet_path), "csv_path": str(csv_path)}


@task(name="save-quality-report")
def save_quality_report(report: dict, outdir: str) -> str:
    """검증 리포트를 JSON 파일로 저장."""
    out_path = Path(outdir)
    out_path.mkdir(parents=True, exist_ok=True)
    report_path = out_path / "quality_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    return str(report_path)


# --------------------------------------------------------------------------
# Flow
# --------------------------------------------------------------------------

@flow(name="power-consumption-etl", log_prints=True)
def power_consumption_etl(input_path: str = "powerconsumption.csv", outdir: str = "./output"):
    """Power Consumption 시계열 데이터 ETL 메인 플로우."""
    logger = get_run_logger()

    raw_df = extract(input_path)
    quality_report = validate(raw_df)
    clean_df = transform(raw_df)
    output_paths = load(clean_df, outdir)
    report_path = save_quality_report(quality_report, outdir)

    # Prefect UI에서 바로 확인 가능한 마크다운 아티팩트 생성
    summary_md = f"""# Power Consumption ETL 실행 결과

| 항목 | 값 |
|---|---|
| 원본 행 수 | {quality_report['row_count']} |
| 중복 행 (제거됨) | {quality_report['duplicate_rows']} |
| 결측치 컬럼 | {quality_report['null_counts'] or '없음'} |
| 음수값 발견 컬럼 | {quality_report['negative_value_counts'] or '없음'} |
| 시간 간격 불규칙 구간 | {quality_report['irregular_interval_count']} |
| 데이터 기간 | {quality_report['time_range'][0]} ~ {quality_report['time_range'][1]} |
| 최종 출력 행 수 | {len(clean_df)} |
| 출력 파일 | {output_paths['parquet_path']} |
"""
    create_markdown_artifact(key="etl-summary", markdown=summary_md)

    logger.info(f"ETL 파이프라인 완료. 품질 리포트: {report_path}")
    return output_paths


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Power Consumption ETL Pipeline")
    parser.add_argument("--input", type=str, default="powerconsumption.csv", help="입력 CSV 경로")
    parser.add_argument("--outdir", type=str, default="./output", help="출력 디렉토리")
    args = parser.parse_args()

    power_consumption_etl(input_path=args.input, outdir=args.outdir)
