"""
Power Consumption 시계열 데이터 ETL 파이프라인 (Prefect)

데이터 출처: Kaggle Power Consumption of Tetouan City
(10분 간격, 기상변수 + 3개 구역 전력소비량)

흐름:
    extract  -> 원본 CSV 로드
    validate -> 스키마/품질 점검 (결측치, 중복, 시간 연속성, 음수값)
    transform-> 타입/시간/중복/범위 기반 정제
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


# 컬럼별 물리적으로 유효한 값 범위 (도메인 지식 기반)
VALID_RANGES = {
    "Temperature": (-30, 55),          # 섭씨, 극단적 이상기후 감안
    "Humidity": (0, 100),              # %
    "WindSpeed": (0, 60),              # m/s, 태풍급 초과는 이상치로 간주
    "GeneralDiffuseFlows": (0, 1500),  # W/m^2
    "DiffuseFlows": (0, 1500),         # W/m^2
    "PowerConsumption_Zone1": (0, 100000),
    "PowerConsumption_Zone2": (0, 100000),
    "PowerConsumption_Zone3": (0, 100000),
}

NUMERIC_COLS_EXPECTED = list(VALID_RANGES.keys())


@task(name="validate-data", log_prints=True)
def validate(df: pd.DataFrame, strict: bool = False) -> dict:
    """
    데이터 품질을 점검하고 리포트를 dict로 반환.

    Args:
        df: 원본 DataFrame
        strict: True면 critical 이슈 발견 시 예외를 발생시켜 파이프라인 중단.
                False면 경고만 남기고 계속 진행 (transform 단계에서 자체 처리).

    report 구조:
        - critical_issues: 반드시 확인해야 하는 심각한 문제 목록
        - warnings: 참고할 만한 경미한 문제 목록
        - (세부 지표들)
    """
    logger = get_run_logger()
    report: dict = {"critical_issues": [], "warnings": []}

    # 1) 필수 컬럼 존재 여부 (없으면 이후 검증 자체가 불가능하므로 즉시 중단)
    expected_cols = {"Datetime", *NUMERIC_COLS_EXPECTED}
    missing_cols = expected_cols - set(df.columns)
    if missing_cols:
        raise ValueError(f"필수 컬럼 누락: {missing_cols}")

    report["row_count"] = int(len(df))

    # 2) 컬럼 타입 검증 - 숫자여야 할 컬럼에 문자/object가 섞여 있는지
    dtype_issues = {}
    for col in NUMERIC_COLS_EXPECTED:
        if not pd.api.types.is_numeric_dtype(df[col]):
            coerced = pd.to_numeric(df[col], errors="coerce")
            bad_count = int(coerced.isna().sum() - df[col].isna().sum())
            dtype_issues[col] = bad_count
    report["dtype_issues"] = dtype_issues
    if dtype_issues:
        msg = f"숫자형이어야 할 컬럼에 비정상 값 발견: {dtype_issues}"
        report["critical_issues"].append(msg)
        logger.error(msg)

    # 3) Datetime 파싱 검증
    dt_parsed = pd.to_datetime(df["Datetime"], errors="coerce")
    unparsable = int(dt_parsed.isna().sum())
    report["unparsable_datetime_count"] = unparsable
    if unparsable > 0:
        msg = f"Datetime 파싱 실패 {unparsable}건"
        report["critical_issues"].append(msg)
        logger.error(msg)

    # 4) 전체 행 중복 / Datetime 값 중복(값이 다른데 시각만 같은 경우 포함) 구분
    report["duplicate_rows"] = int(df.duplicated().sum())
    dup_datetime_mask = dt_parsed.duplicated(keep=False) & dt_parsed.notna()
    dup_datetime_count = int(dup_datetime_mask.sum())
    # 완전 중복 행이 아닌데 Datetime만 겹치는 경우 = 같은 시각에 값이 다른 레코드가 존재 (더 심각)
    conflicting_datetime_count = int(
        (dup_datetime_mask & ~df.duplicated(keep=False)).sum()
    )
    report["duplicate_datetime_count"] = dup_datetime_count
    report["conflicting_datetime_count"] = conflicting_datetime_count
    if conflicting_datetime_count > 0:
        msg = f"동일 시각에 값이 다른 레코드 {conflicting_datetime_count}건 발견 (단순 중복 아님, 데이터 신뢰성 문제)"
        report["critical_issues"].append(msg)
        logger.error(msg)

    # 5) 결측치
    null_counts = df.isna().sum()
    report["null_counts"] = {k: int(v) for k, v in null_counts.items() if v > 0}
    null_ratio = df.isna().mean()
    high_null_cols = {k: round(float(v), 4) for k, v in null_ratio.items() if v > 0.3}
    if high_null_cols:
        msg = f"결측 비율 30% 초과 컬럼: {high_null_cols}"
        report["critical_issues"].append(msg)
        logger.error(msg)

    # 6) 음수값 (전체 컬럼 기준, transform 단계에서 참고)
    numeric_cols = [c for c in df.columns if c != "Datetime"]
    negative_counts = (df[numeric_cols] < 0).sum()
    report["negative_value_counts"] = {
        k: int(v) for k, v in negative_counts.items() if v > 0
    }

    # 7) 도메인 범위(물리적 유효 범위) 기반 이상치 검증
    out_of_range = {}
    for col, (low, high) in VALID_RANGES.items():
        mask = (df[col] < low) | (df[col] > high)
        cnt = int(mask.sum())
        if cnt > 0:
            out_of_range[col] = {
                "count": cnt,
                "valid_range": [low, high],
                "actual_min": float(df[col].min()),
                "actual_max": float(df[col].max()),
            }
    report["out_of_range_values"] = out_of_range
    if out_of_range:
        logger.warning(f"물리적 유효 범위를 벗어난 값 발견: {out_of_range}")

    # 8) IQR 기반 통계적 이상치 탐지 (범위 검증과 별개로, 급격한 스파이크 탐지용)
    outlier_summary = {}
    for col in NUMERIC_COLS_EXPECTED:
        series = df[col].dropna()
        if len(series) < 10:
            continue
        q1, q3 = series.quantile(0.25), series.quantile(0.75)
        iqr = q3 - q1
        lower, upper = q1 - 3 * iqr, q3 + 3 * iqr  # 3*IQR: 완만한 극단치만 탐지
        cnt = int(((series < lower) | (series > upper)).sum())
        if cnt > 0:
            outlier_summary[col] = {"count": cnt, "ratio": round(cnt / len(series), 4)}
    report["statistical_outliers"] = outlier_summary

    # 9) 시간 정렬 여부
    is_sorted = bool(dt_parsed.dropna().is_monotonic_increasing)
    report["is_time_sorted"] = is_sorted
    if not is_sorted:
        logger.warning("Datetime이 시간순으로 정렬되어 있지 않음 -> transform 단계에서 정렬 예정")

    # 10) 시간 간격 연속성 (10분 간격 가정)
    dt_sorted = dt_parsed.dropna().sort_values()
    diffs = dt_sorted.diff().dropna()
    expected_delta = pd.Timedelta(minutes=10)
    irregular = diffs[diffs != expected_delta]
    report["irregular_interval_count"] = int(len(irregular))
    # 간격이 비정상적으로 크게 벌어진 구간(데이터 누락 구간) 별도 표기
    gap_threshold = expected_delta * 3
    big_gaps = irregular[irregular > gap_threshold]
    report["large_gap_count"] = int(len(big_gaps))
    if len(dt_sorted) > 0:
        report["time_range"] = [str(dt_sorted.min()), str(dt_sorted.max())]
    else:
        report["time_range"] = [None, None]

    if report["duplicate_rows"] > 0:
        report["warnings"].append(f"중복 행 {report['duplicate_rows']}건 -> transform에서 제거 예정")
    if report["null_counts"]:
        report["warnings"].append(f"결측치 발견: {report['null_counts']} -> transform에서 보간 예정")
    if report["negative_value_counts"]:
        report["warnings"].append(f"음수값 발견: {report['negative_value_counts']} -> transform에서 NaN 처리 후 보간")
    if report["irregular_interval_count"] > 0:
        report["warnings"].append(
            f"시간 간격 불규칙 {report['irregular_interval_count']}건 (그중 3배 이상 갭 {report['large_gap_count']}건)"
        )

    report["is_valid"] = len(report["critical_issues"]) == 0

    logger.info(f"검증 리포트: {json.dumps(report, ensure_ascii=False, indent=2, default=str)}")

    for w in report["warnings"]:
        logger.warning(w)
    for c in report["critical_issues"]:
        logger.error(c)

    if strict and not report["is_valid"]:
        raise ValueError(f"치명적 데이터 품질 문제 발견, 파이프라인 중단: {report['critical_issues']}")

    return report


@task(name="transform-data", log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    validate 단계에서 점검한 각 항목을 실제로 정제한다.
    validate의 검증 순서와 최대한 대응되도록 구성:

      1) 타입 정제   : 숫자형이어야 할 컬럼을 강제 변환 (dtype_issues 대응)
      2) Datetime 정제: 파싱 실패 행 제거 (unparsable_datetime_count 대응)
      3) 중복/충돌 정제: 동일 시각 레코드를 평균으로 집계
                        (duplicate_rows, duplicate_datetime_count,
                         conflicting_datetime_count 대응)
      4) 정렬         : 시간순 정렬 (is_time_sorted 대응)
      5) 범위 기반 정제: 물리적으로 불가능한 값(out_of_range_values,
                        negative_value_counts) -> NaN 처리
      6) 결측치 보간   : 5)에서 생긴 NaN + 원래 결측치(null_counts)를
                        시간 기준 선형 보간

    * statistical_outliers(IQR)와 irregular_interval/large_gap은
      정상적인 자연 변동(일사량의 일중 패턴, 계절적 수요 변화 등)일 수 있어
      임의로 수정/삭제하지 않고 quality_report에만 남겨 참고하도록 한다.
    """
    logger = get_run_logger()
    df = df.copy()

    # 1) 타입 정제 - 숫자형이어야 할 컬럼에 문자 등이 섞여 있으면 강제 변환
    for col in NUMERIC_COLS_EXPECTED:
        if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
            before_na = df[col].isna().sum()
            df[col] = pd.to_numeric(df[col], errors="coerce")
            new_na = int(df[col].isna().sum() - before_na)
            if new_na > 0:
                logger.warning(f"{col}: 숫자로 변환 불가능한 값 {new_na}건을 NaN으로 치환")

    # 2) Datetime 정제 - 파싱 실패 행은 시간축 자체가 없어 보간 불가하므로 제거
    df["Datetime"] = pd.to_datetime(df["Datetime"], errors="coerce")
    before = len(df)
    df = df.dropna(subset=["Datetime"])
    dropped_unparsable = before - len(df)
    if dropped_unparsable > 0:
        logger.warning(f"Datetime 파싱 실패 행 {dropped_unparsable}건 제거")

    # 3) 중복/충돌 레코드 정제 - 같은 시각에 여러 레코드가 있으면(완전 중복이든
    #    값이 다른 충돌이든) 평균으로 집계해 시각당 하나의 레코드로 만든다.
    before = len(df)
    numeric_cols_all = [c for c in df.columns if c != "Datetime"]
    df = df.groupby("Datetime", as_index=False)[numeric_cols_all].mean()
    logger.info(f"동일 시각 레코드 집계: {before - len(df)}건 병합됨")

    # 4) 정렬
    df = df.sort_values("Datetime").reset_index(drop=True)

    # 5) 범위 기반 정제 - 물리적으로 불가능한 값(음수 포함) -> NaN 처리
    #    VALID_RANGES가 validate 단계와 동일한 기준이므로, 음수값 체크도
    #    자연스럽게 이 범위 체크에 포함된다 (예: Humidity < 0, Zone1 < 0 등).
    for col, (low, high) in VALID_RANGES.items():
        if col not in df.columns:
            continue
        invalid_mask = (df[col] < low) | (df[col] > high)
        if invalid_mask.any():
            logger.warning(
                f"{col}: 유효 범위[{low}, {high}]를 벗어난 값 {int(invalid_mask.sum())}건을 NaN으로 치환"
            )
            df.loc[invalid_mask, col] = pd.NA

    # 6) 결측치 보간 (원래 결측치 + 위에서 NaN 처리한 값)
    numeric_cols = [c for c in df.columns if c != "Datetime"]
    df[numeric_cols] = df[numeric_cols].interpolate(method="linear", limit_direction="both")

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
def power_consumption_etl(
    input_path: str = "powerconsumption.csv",
    outdir: str = "./output",
    strict_validation: bool = False,
):
    """Power Consumption 시계열 데이터 ETL 메인 플로우.

    Args:
        input_path: 입력 CSV 경로
        outdir: 출력 디렉토리
        strict_validation: True면 validate 단계에서 critical 이슈 발견 시 파이프라인 중단
    """
    logger = get_run_logger()

    raw_df = extract(input_path)
    quality_report = validate(raw_df, strict=strict_validation)
    clean_df = transform(raw_df)
    output_paths = load(clean_df, outdir)
    report_path = save_quality_report(quality_report, outdir)

    critical_issues = quality_report.get("critical_issues", [])
    warnings = quality_report.get("warnings", [])
    critical_md = "\n".join(f"- ⚠️ {c}" for c in critical_issues) or "없음"
    warnings_md = "\n".join(f"- {w}" for w in warnings) or "없음"

    # Prefect UI에서 바로 확인 가능한 마크다운 아티팩트 생성
    summary_md = f"""# Power Consumption ETL 실행 결과

## 종합 판정: {"✅ PASS" if quality_report.get("is_valid") else "❌ FAIL (critical issue 존재)"}

### Critical Issues
{critical_md}

### Warnings
{warnings_md}

## 세부 지표

| 항목 | 값 |
|---|---|
| 원본 행 수 | {quality_report['row_count']} |
| 중복 행 | {quality_report['duplicate_rows']} |
| Datetime 중복 (값 충돌 포함) | {quality_report['duplicate_datetime_count']} (충돌 {quality_report['conflicting_datetime_count']}) |
| Datetime 파싱 실패 | {quality_report['unparsable_datetime_count']} |
| 시간 정렬 여부 | {quality_report['is_time_sorted']} |
| 타입 이상 컬럼 | {quality_report['dtype_issues'] or '없음'} |
| 결측치 컬럼 | {quality_report['null_counts'] or '없음'} |
| 음수값 발견 컬럼 | {quality_report['negative_value_counts'] or '없음'} |
| 물리적 범위 이탈 | {quality_report['out_of_range_values'] or '없음'} |
| 통계적 이상치(IQR) | {quality_report['statistical_outliers'] or '없음'} |
| 시간 간격 불규칙 구간 | {quality_report['irregular_interval_count']} (3배 이상 갭 {quality_report['large_gap_count']}건) |
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
    parser.add_argument(
        "--strict",
        action="store_true",
        help="설정 시 검증 단계에서 critical 이슈 발견 시 파이프라인 중단",
    )
    args = parser.parse_args()

    power_consumption_etl(input_path=args.input, outdir=args.outdir, strict_validation=args.strict)
