# Power Consumption ETL with Prefect

Kaggle "Power Consumption of Tetouan City" 데이터셋(10분 간격, 기상변수 + 3개 구역 전력소비량)을
정제하는 Prefect ETL 파이프라인입니다.

## 파일 구성

- `etl_flow.py` : ETL 메인 플로우 (extract → validate → transform → load)
- `deploy.py` : 정기 스케줄링(cron) 배포 등록 예시 코드
- `requirements.txt` : 의존성 목록
- `HOW_TO_USE_ON_OTHER_MACHINE.md` : 다른 컴퓨터에서 이 레포를 clone해서 실행하는 방법 안내

## 데이터 준비 (필수)

이 레포에는 데이터 파일(`powerconsumption.csv`)이 포함되어 있지 않습니다.
아래 Kaggle 데이터셋을 직접 다운로드한 뒤, 이 README와 같은 폴더에
`powerconsumption.csv` 라는 이름으로 넣어주세요.

- Kaggle: "Electric Power Consumption" 데이터셋 검색 후 다운로드
- 파일을 받으면 프로젝트 루트 폴더(이 파일들과 같은 위치)에 복사

폴더 구조 예시:
```
prefect_power/
├── etl_flow.py
├── deploy.py
├── requirements.txt
├── README.md
└── powerconsumption.csv   <- 직접 다운로드해서 추가
```

## 데이터 설명

모로코 북부, 지중해 연안에 위치한 테투안(Tetouan)시의 3개 배전망(distribution network)에 대한 전력소비량 데이터입니다. 2017년 1월 1일부터 12월 30일까지 10분 간격으로 수집되었으며, 총 52,416개 관측치로 구성되어 있습니다.

| 컬럼 | 설명 |
| --- | --- |
| `Datetime` | 10분 단위 시간 창(timestamp) |
| `Temperature` | 기온 |
| `Humidity` | 습도 (%) |
| `WindSpeed` | 풍속 |
| `GeneralDiffuseFlows` | 일반 확산 일사량 |
| `DiffuseFlows` | 확산 일사량 |
| `PowerConsumption_Zone1` | 1구역 전력소비량 |
| `PowerConsumption_Zone2` | 2구역 전력소비량 |
| `PowerConsumption_Zone3` | 3구역 전력소비량 |

지중해성 기후 특성상 겨울엔 온화하고 비가 오며 여름엔 덥고 건조한데, 이 계절적 기상 패턴이 냉난방 수요를 통해 전력소비량과 강하게 연동되는 경향이 있습니다. 일사량 컬럼(`GeneralDiffuseFlows`, `DiffuseFlows`)은 낮 동안에는 값이 크고 밤에는 0에 가까운 일중 패턴을 보이는데, validate 단계에서 이 패턴이 통계적 이상치(IQR)로 일부 잡히는 것도 이 때문입니다.

## 파이프라인 단계

### 1. extract
원본 CSV 로드 (재시도 2회 포함)

### 2. validate

[#validate](#validate)

`validate(df, strict=False)` 함수가 담당합니다. 문제를 **critical**(심각)과 **warning**(경미)으로 구분해 `dict` 리포트로 반환하며, `strict=True`(CLI `--strict`) 시 critical 이슈가 하나라도 있으면 파이프라인을 중단시킵니다.

1. **스키마 확인** — `expected_cols - set(df.columns)`로 필수 컬럼 누락 여부를 확인. 누락 시 이후 검증이 무의미하므로 strict 여부와 무관하게 즉시 중단.
2. **타입 검증** — `NUMERIC_COLS_EXPECTED`의 각 컬럼을 `pd.api.types.is_numeric_dtype()`으로 확인하고, 아니라면 `pd.to_numeric(errors="coerce")`로 강제 변환해 새로 생긴 결측치 개수(`dtype_issues`)를 셈.
3. **Datetime 파싱** — `pd.to_datetime(errors="coerce")`로 파싱 실패 건수(`unparsable_datetime_count`)를 집계.
4. **중복 / 충돌 레코드 검사**
   - 전체 행 완전 중복 → `duplicate_rows`
   - Datetime 값만 중복 → `duplicate_datetime_count`
   - **충돌 레코드**: Datetime은 같은데 나머지 값이 다른 경우 → `conflicting_datetime_count` (단순 중복보다 심각, critical)
5. **결측치** — 컬럼별 개수(`null_counts`)와 비율을 집계하고, **30% 초과** 컬럼은 `high_null_cols`로 잡아 critical 처리.
6. **음수값** — `Datetime`을 제외한 전 컬럼에서 음수 개수(`negative_value_counts`) 집계 (warning).
7. **물리적 유효 범위 검증** — 컬럼별 상식적 범위(`VALID_RANGES`)를 벗어난 값 탐지:

   | 컬럼 | 유효 범위 |
   | --- | --- |
   | Temperature | -30 ~ 55 (℃) |
   | Humidity | 0 ~ 100 (%) |
   | WindSpeed | 0 ~ 60 (m/s) |
   | GeneralDiffuseFlows / DiffuseFlows | 0 ~ 1500 (W/m²) |
   | PowerConsumption_Zone1/2/3 | 0 ~ 100,000 |

   범위를 벗어난 개수·실제 min/max를 `out_of_range_values`에 기록 (warning).
8. **통계적 이상치 탐지 (IQR)** — 컬럼별 `q1`, `q3`로 `iqr = q3 - q1`을 구해 `[q1 - 3·iqr, q3 + 3·iqr]` 범위를 벗어나는 값을 `statistical_outliers`에 기록. 일반적인 1.5×IQR보다 관대한 **3×IQR**을 써서 정상적인 계절/일중 변동을 오탐하지 않도록 함 (warning).
9. **시간 정렬 여부** — `is_monotonic_increasing`으로 확인 (`is_time_sorted`); 정렬 안 되어 있으면 warning만 남기고 transform에서 자동 정렬.
10. **시간 간격 연속성** — 정렬된 Datetime의 `diff()`를 10분 간격 기준과 비교해 불규칙 구간(`irregular_interval_count`)을 집계하고, 그중 **30분(3배) 이상** 벌어진 큰 갭은 `large_gap_count`로 별도 표기 (데이터 통째 누락 가능성).

**반환되는 report 예시:**
```json
{
  "critical_issues": [],
  "warnings": [],
  "row_count": 52416,
  "dtype_issues": {},
  "unparsable_datetime_count": 0,
  "duplicate_rows": 0,
  "duplicate_datetime_count": 0,
  "conflicting_datetime_count": 0,
  "null_counts": {},
  "negative_value_counts": {},
  "out_of_range_values": {},
  "statistical_outliers": {
    "DiffuseFlows": { "count": 1690, "ratio": 0.0322 }
  },
  "is_time_sorted": true,
  "irregular_interval_count": 0,
  "large_gap_count": 0,
  "time_range": ["2017-01-01 00:00:00", "2017-12-30 23:50:00"],
  "is_valid": true
}
```

### 3. transform

[#transform](#transform)

`transform(df)` 함수가 담당하며, validate 단계에서 점검한 항목들과 최대한 1:1로 대응되도록 구성되어 있습니다.

1. **타입 정제** — `NUMERIC_COLS_EXPECTED` 중 숫자형이 아닌 컬럼을 `pd.to_numeric(errors="coerce")`로 강제 변환 (validate의 **타입 검증**에 대응).
2. **Datetime 정제** — `pd.to_datetime(errors="coerce")`로 다시 파싱하고, 파싱 실패(`NaT`)인 행은 시간축 자체가 없어 보간이 불가능하므로 제거 (validate의 **Datetime 파싱 검증**에 대응).
3. **중복/충돌 레코드 정제** — 같은 시각(`Datetime`)에 여러 레코드가 있으면 완전 중복이든 값이 다른 충돌이든 구분하지 않고 `groupby("Datetime").mean()`으로 평균 집계해 시각당 하나의 레코드로 병합 (validate의 **중복/충돌 검사**에 대응).
4. **정렬** — 시각순으로 정렬 (validate의 **시간 정렬 여부**에 대응).
5. **범위 기반 정제** — validate와 동일한 `VALID_RANGES` 기준으로 물리적으로 불가능한 값을 `NaN`으로 치환. 음수값도 대부분 컬럼에서 하한이 0이므로 이 단계에서 자연스럽게 함께 처리됨 (validate의 **물리적 유효 범위 검증**, **음수값 검사**에 대응).
6. **결측치 보간** — 원래 결측치와 5)에서 새로 생긴 결측치를 모두 `interpolate(method="linear", limit_direction="both")`로 시간 기준 선형 보간 (validate의 **결측치 검사**에 대응).

> `statistical_outliers`(IQR)와 `irregular_interval_count`/`large_gap_count`는 정상적인 자연 변동(일사량의 일중 패턴, 계절적 수요 변화, 실측 장비 특성 등)일 수 있어 transform 단계에서 임의로 수정하지 않고 quality_report에만 남겨 참고용으로 둡니다.

### 4. load

[#load](#load)

정제 데이터를 parquet + csv로 저장하고, 품질 리포트(JSON)를 저장하며, Prefect UI에 마크다운 아티팩트를 생성합니다 (PASS/FAIL 종합 판정 및 critical/warning 목록 포함).

## 로컬 실행

```bash
pip install -r requirements.txt

python etl_flow.py --input powerconsumption.csv --outdir ./output

# critical 품질 이슈 발견 시 파이프라인을 중단시키고 싶다면
python etl_flow.py --input powerconsumption.csv --outdir ./output --strict
```

실행 후 `./output/` 디렉토리에 아래 파일이 생성됩니다.

- `powerconsumption_clean.parquet`
- `powerconsumption_clean.csv`
- `quality_report.json`

Prefect UI를 함께 보고 싶다면 별도 터미널에서 `prefect server start` 실행 후
`http://127.0.0.1:4200` 에서 플로우 실행 기록과 아티팩트를 확인할 수 있습니다.

## 정기 스케줄 배포 (선택)

매일 자동 실행되도록 등록하려면:

```bash
# 1) Prefect 서버 또는 Cloud 연결
prefect server start            # 로컬 서버
# 또는: prefect cloud login

# 2) 워크풀 생성 (최초 1회)
prefect work-pool create my-process-pool --type process

# 3) 배포 등록 (매일 새벽 1시 cron)
python deploy.py

# 4) 워커 실행 (실제로 작업을 수행)
prefect worker start --pool my-process-pool
```

`deploy.py`의 `cron` 값이나 `parameters`는 필요에 맞게 수정하면 됩니다.

## 참고: 데이터 사전 점검 결과 (제공된 파일 기준)

- 행 수: 52,416 (2017-01-01 ~ 2017-12-30, 10분 간격)
- 결측치: 없음
- 중복 행 / 중복 Datetime: 없음
- 음수값: 없음
- 물리적 유효 범위 이탈: 없음
- 시간 간격: 전 구간 10분 간격으로 끊김 없음, 시간순 정렬됨
- 종합 판정: `is_valid: true` (critical issue 없음)
- 참고용 경고: `DiffuseFlows` 컬럼에서 IQR 기준 통계적 이상치 1,690건(약 3.2%) 발견 — 이는 데이터 오류가 아니라 야간에 일사량이 0에 가깝고 낮에 급증하는 자연스러운 패턴이라 정상적인 현상임

다만 ETL 코드는 위와 다른(결측/중복/음수/간격 불규칙/범위 이탈이 있는) 데이터가 들어와도
안전하게 처리되도록 일반화해서 작성했습니다.
