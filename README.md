# Power Consumption ETL

Kaggle `Power Consumption of Tetouan City` 데이터셋을 분석 가능한 형태로 정제하기 위한 전처리 문서입니다. 
시간 변수, lag/rolling, zone 비중, 모델 입력용 스케일링 등은 별도 단계에서 수행하고, 여기서는 원본 데이터의 품질을 검증하고 분석 가능한 시계열 테이블로 정리하는 데 집중합니다.

## Dataset Grain

원본 데이터는 하나의 CSV 파일이며, 한 행은 하나의 10분 관측 시점입니다.

| 항목 | 값 |
| --- | --- |
| 파일 | `powerconsumption.csv` |
| 행 수 | 52,416 |
| 기간 | `2017-01-01 00:00:00` ~ `2017-12-30 23:50:00` |
| 간격 | 10분 |
| 하루 정상 행 수 | 144 |
| 시간 컬럼 | `Datetime` |
| 수치 컬럼 | 기상 5개 + 전력 소비 3개 |
| 타깃 후보 | `PowerConsumption_Zone1`, `PowerConsumption_Zone2`, `PowerConsumption_Zone3` |

컬럼 구성:

| 컬럼 | 역할 | 원본 기준 min | 원본 기준 max | 비고 |
| --- | --- | ---: | ---: | --- |
| `Datetime` | timestamp | - | - | `%m/%d/%Y %H:%M` 형식 |
| `Temperature` | weather | 3.247 | 40.010 | 기온 |
| `Humidity` | weather | 11.340 | 94.800 | 습도 |
| `WindSpeed` | weather | 0.050 | 6.483 | 풍속 |
| `GeneralDiffuseFlows` | weather | 0.004 | 1163.000 | 일반 확산 일사량 |
| `DiffuseFlows` | weather | 0.011 | 936.000 | 확산 일사량 |
| `PowerConsumption_Zone1` | target candidate | 13895.696 | 52204.395 | 1구역 전력 소비 |
| `PowerConsumption_Zone2` | target candidate | 8560.081 | 37408.861 | 2구역 전력 소비 |
| `PowerConsumption_Zone3` | target candidate | 5935.174 | 47598.326 | 3구역 전력 소비 |

원본 파일 사전 점검 결과:

| 점검 항목 | 결과 |
| --- | --- |
| 결측치 | 없음 |
| 완전 중복 행 | 없음 |
| 중복 `Datetime` | 없음 |
| 시간 정렬 | 정렬됨 |
| 시간 간격 | 전체 10분 간격 유지 |
| 하루 144개 미만/초과 날짜 | 없음 |
| 음수값 | 없음 |
| 물리 범위 초과값 | 없음 |
| critical issue | 없음 |

## Validate

`validate(df, strict=False)`는 원본 DataFrame을 수정하지 않고 품질 검증 리포트만 생성합니다. 리포트는 `critical_issues`, `warnings`, 세부 지표를 담은 dict입니다.

`strict=True` 또는 CLI `--strict` 사용 시 critical issue가 하나라도 있으면 transform/load로 넘어가지 않고 파이프라인을 중단합니다.

### Severity Policy

| 등급 | 의미 | 예시 | strict 모드 동작 |
| --- | --- | --- | --- |
| `critical` | 이후 처리 결과를 신뢰하기 어려운 문제 | 필수 컬럼 누락, Datetime 파싱 실패, 충돌 레코드, 과도한 결측 | 중단 |
| `warning` | 자동 정제 가능하거나 분석자가 확인할 문제 | 정렬 안 됨, IQR 이상치, 일부 범위 초과값, 짧은 결측 | 계속 진행 가능 |

필수 컬럼 누락은 `strict` 값과 무관하게 즉시 중단합니다. 스키마가 깨지면 이후 검증 항목 대부분이 의미 없어지기 때문입니다.

### 1. Schema Validation

필수 컬럼이 모두 존재하는지 확인합니다.

```python
EXPECTED_COLS = {
    "Datetime",
    "Temperature",
    "Humidity",
    "WindSpeed",
    "GeneralDiffuseFlows",
    "DiffuseFlows",
    "PowerConsumption_Zone1",
    "PowerConsumption_Zone2",
    "PowerConsumption_Zone3",
}

missing_cols = EXPECTED_COLS - set(df.columns)
```

판정:

| 조건 | 리포트 | 등급 |
| --- | --- | --- |
| `missing_cols`가 비어 있음 | 정상 | - |
| 필수 컬럼 누락 | `critical_issues`, `missing_columns` | critical |
| 예상 외 컬럼 존재 | `extra_columns` | warning 또는 info |

### 2. Numeric Type Validation

`Datetime`을 제외한 모든 컬럼은 숫자형이어야 합니다.

```python
NUMERIC_COLS_EXPECTED = [
    "Temperature",
    "Humidity",
    "WindSpeed",
    "GeneralDiffuseFlows",
    "DiffuseFlows",
    "PowerConsumption_Zone1",
    "PowerConsumption_Zone2",
    "PowerConsumption_Zone3",
]
```

각 컬럼에 대해 `pd.api.types.is_numeric_dtype()`로 확인합니다. 숫자형이 아니면 검증용 복사본에서만 다음 변환을 시도합니다.

```python
converted = pd.to_numeric(df[col], errors="coerce")
new_nulls = converted.isna().sum() - df[col].isna().sum()
```

판정:

| 조건 | 리포트 | 등급 |
| --- | --- | --- |
| 이미 숫자형 | 정상 | - |
| 변환 가능하지만 dtype이 object | `dtype_issues[col] = new_nulls` | warning |
| 변환 불가능 값으로 결측 발생 | `dtype_issues[col] > 0` | warning 또는 critical |
| 전력 컬럼에서 변환 실패 다수 | `critical_issues` | critical |

validate 단계에서는 원본 `df`를 직접 바꾸지 않습니다. 실제 변환은 transform에서 수행합니다.

### 3. Datetime Parsing Validation

`Datetime`은 시간축의 기준이므로 가장 중요한 컬럼입니다. 이 데이터셋의 원본 형식은 `1/1/2017 0:00` 형태입니다.

```python
parsed_dt = pd.to_datetime(
    df["Datetime"],
    format="%m/%d/%Y %H:%M",
    errors="coerce",
)
```

리포트 항목:

| 항목 | 의미 |
| --- | --- |
| `unparsable_datetime_count` | 파싱 실패로 `NaT`가 된 행 수 |
| `time_range` | 파싱 가능한 Datetime의 min/max |
| `row_count` | 전체 행 수 |

판정:

| 조건 | 등급 |
| --- | --- |
| 파싱 실패 0건 | 정상 |
| 파싱 실패 1건 이상 | critical |

Datetime 파싱 실패 행은 시간 위치를 알 수 없어 안전하게 보간할 수 없습니다.

### 4. Duplicate & Conflict Validation

중복은 세 단계로 나눠 봅니다.

| 검사 | 기준 | 리포트 |
| --- | --- | --- |
| 완전 중복 | 모든 컬럼 값이 동일 | `duplicate_rows` |
| Datetime 중복 | 같은 시각이 2번 이상 등장 | `duplicate_datetime_count` |
| 충돌 레코드 | 같은 시각인데 나머지 값이 다름 | `conflicting_datetime_count` |

충돌 레코드 예시:

```text
Datetime           Temperature  ...  PowerConsumption_Zone1
2017-01-01 00:00   6.559        ...  34055.6962
2017-01-01 00:00   7.100        ...  35000.0000
```

판정:

| 조건 | 등급 | 이유 |
| --- | --- | --- |
| 완전 중복 | warning | 제거 가능 |
| Datetime 중복이지만 값 동일 | warning | 중복 제거 가능 |
| Datetime 중복 + 값 다름 | critical | 같은 시각의 참값이 불명확 |

transform에서는 같은 시각의 여러 값이 있으면 평균 병합할 수 있지만, 충돌 자체는 반드시 리포트에 남깁니다.

### 5. Null Validation

컬럼별 결측 개수와 비율을 계산합니다.

```python
null_counts = df.isna().sum()
null_ratios = df.isna().mean()
```

판정 기준:

| 컬럼 그룹 | 기준 | 등급 |
| --- | --- | --- |
| `Datetime` | 결측 또는 파싱 실패 1건 이상 | critical |
| `PowerConsumption_Zone1/2/3` | 결측률 5~10% 이상 | critical 또는 strong warning |
| weather columns | 결측률 30% 초과 | critical |
| any column | 소량 결측 | warning |

전력 컬럼은 분석 대상이므로 기상 컬럼보다 더 엄격하게 판단합니다.

### 6. Negative Value Validation

`Datetime`을 제외한 수치 컬럼의 음수 개수를 계산합니다.

```python
negative_value_counts[col] = (df[col] < 0).sum()
```

판정:

| 컬럼 | 음수 허용 여부 | 등급 |
| --- | --- | --- |
| `Temperature` | 가능 | 물리 범위에서 판단 |
| `Humidity` | 불가 | warning 또는 critical |
| `WindSpeed` | 불가 | warning 또는 critical |
| `GeneralDiffuseFlows` | 불가 | warning 또는 critical |
| `DiffuseFlows` | 불가 | warning 또는 critical |
| `PowerConsumption_Zone1/2/3` | 불가 | critical에 가까운 warning |

이 데이터셋 원본에는 음수값이 없습니다.

### 7. Physical Range Validation

실제 원본 범위보다 넉넉한 유효 범위를 둡니다. 목적은 정상적인 계절/일중 변동을 제거하는 것이 아니라, 물리적으로 불가능하거나 단위가 잘못된 값을 잡는 것입니다.

```python
VALID_RANGES = {
    "Temperature": (-30, 55),
    "Humidity": (0, 100),
    "WindSpeed": (0, 60),
    "GeneralDiffuseFlows": (0, 1500),
    "DiffuseFlows": (0, 1500),
    "PowerConsumption_Zone1": (0, 100000),
    "PowerConsumption_Zone2": (0, 100000),
    "PowerConsumption_Zone3": (0, 100000),
}
```

리포트:

```json
"out_of_range_values": {
  "Humidity": {
    "count": 3,
    "min": -5.0,
    "max": 103.2,
    "valid_min": 0,
    "valid_max": 100
  }
}
```

판정:

| 조건 | 등급 |
| --- | --- |
| 범위 초과 없음 | 정상 |
| 소량 범위 초과 | warning |
| 특정 컬럼 대부분 범위 초과 | critical 가능 |

### 8. Statistical Outlier Validation

IQR 기반 이상치를 탐지합니다.

```python
q1 = df[col].quantile(0.25)
q3 = df[col].quantile(0.75)
iqr = q3 - q1
lower = q1 - 3 * iqr
upper = q3 + 3 * iqr
```

`1.5 * IQR`이 아니라 `3 * IQR`을 사용합니다. 이 데이터는 일사량과 전력 소비가 낮/밤, 계절, 온도에 따라 크게 움직이기 때문에 일반적인 IQR 기준을 쓰면 정상 피크를 과하게 잡을 수 있습니다.

리포트:

```json
"statistical_outliers": {
  "DiffuseFlows": {
    "count": 1690,
    "ratio": 0.0322,
    "lower": -302.51,
    "upper": 403.63
  }
}
```

판정:

| 조건 | 등급 | transform 처리 |
| --- | --- | --- |
| IQR 이상치 발견 | warning | 자동 수정하지 않음 |

특히 `GeneralDiffuseFlows`, `DiffuseFlows`는 밤에는 거의 0이고 낮에는 크게 증가하므로 IQR 이상치가 곧 데이터 오류는 아닙니다.

### 9. Time Order Validation

파싱된 `Datetime`이 오름차순인지 확인합니다.

```python
is_time_sorted = parsed_dt.is_monotonic_increasing
```

판정:

| 조건 | 등급 | transform 처리 |
| --- | --- | --- |
| 정렬됨 | 정상 | 유지 |
| 정렬 안 됨 | warning | `Datetime` 기준 정렬 |

### 10. Time Interval Validation

10분 간격 시계열이므로 시간 간격 검증이 중요합니다.

```python
diffs = parsed_dt.sort_values().diff()
irregular_interval_count = (diffs.dropna() != pd.Timedelta(minutes=10)).sum()
large_gap_count = (diffs.dropna() >= pd.Timedelta(minutes=30)).sum()
```

리포트:

| 항목 | 의미 |
| --- | --- |
| `irregular_interval_count` | 10분이 아닌 간격 수 |
| `large_gap_count` | 30분 이상 벌어진 구간 수 |
| `incomplete_daily_counts` | 하루 144개가 아닌 날짜 목록 |

판정:

| 조건 | 등급 | 이유 |
| --- | --- | --- |
| 10분 간격 유지 | 정상 | 원본처럼 연속 시계열 |
| 짧은 누락 | warning | reindex 후 보간 가능 |
| 30분 이상 큰 갭 | warning 또는 critical | 자동 보간 시 왜곡 가능 |

### Validate Report Shape

`validate()`는 아래 형태의 dict를 반환합니다.

```json
{
  "critical_issues": [],
  "warnings": [],
  "row_count": 52416,
  "missing_columns": [],
  "extra_columns": [],
  "dtype_issues": {},
  "unparsable_datetime_count": 0,
  "duplicate_rows": 0,
  "duplicate_datetime_count": 0,
  "conflicting_datetime_count": 0,
  "null_counts": {},
  "null_ratios": {},
  "high_null_cols": [],
  "negative_value_counts": {},
  "out_of_range_values": {},
  "statistical_outliers": {
    "DiffuseFlows": {
      "count": 1690,
      "ratio": 0.0322
    }
  },
  "is_time_sorted": true,
  "irregular_interval_count": 0,
  "large_gap_count": 0,
  "incomplete_daily_counts": {},
  "time_range": [
    "2017-01-01 00:00:00",
    "2017-12-30 23:50:00"
  ],
  "is_valid": true
}
```

## Transform

`transform(df)`는 validate에서 점검한 문제에 대응해 데이터를 정제합니다. 이 단계의 출력은 여전히 **한 행 = 10분 관측 시점**인 테이블입니다. 행의 의미를 바꾸는 집계나 모델용 파생 피처 생성은 하지 않습니다.

### Transform Contract

| 항목 | 내용 |
| --- | --- |
| 입력 | raw pandas DataFrame |
| 출력 | cleaned pandas DataFrame |
| grain | one row = one 10-minute timestamp |
| index | `Datetime` 기준 정렬된 시계열 |
| 포함 | 타입 변환, Datetime 파싱, 중복 처리, 정렬, reindex, 범위 밖 값 치환, 짧은 결측 보간 |
| 제외 | 시간 파생 변수, lag/rolling, 스케일링, 타깃 선택, train/test split |

### 1. Numeric Type Cleaning

validate에서 숫자형이 아니라고 판단된 컬럼을 실제로 숫자형으로 변환합니다.

```python
for col in NUMERIC_COLS_EXPECTED:
    df[col] = pd.to_numeric(df[col], errors="coerce")
```

변환할 수 없는 값은 `NaN`이 됩니다. 이 결측은 이후 보간 대상입니다.

### 2. Datetime Cleaning

`Datetime`을 명시적 format으로 파싱합니다.

```python
df["Datetime"] = pd.to_datetime(
    df["Datetime"],
    format="%m/%d/%Y %H:%M",
    errors="coerce",
)
```

파싱 실패 행은 제거합니다.

```python
df = df.dropna(subset=["Datetime"])
```

이유:

| 문제 | 처리 |
| --- | --- |
| Datetime 파싱 실패 | 행 제거 |
| 수치값 결측 | 보간 가능 |

수치값 결측은 앞뒤 시간값으로 추정할 수 있지만, 시간값 자체가 없으면 어느 위치에 들어가야 하는지 알 수 없습니다.

### 3. Duplicate & Conflict Cleaning

먼저 완전 중복을 제거합니다.

```python
df = df.drop_duplicates()
```

이후 같은 `Datetime`에 여러 행이 있으면 하나로 병합합니다.

```python
df = df.groupby("Datetime", as_index=False)[NUMERIC_COLS_EXPECTED].mean()
```

이 데이터셋은 `Datetime` 외 컬럼이 모두 숫자형이라 평균 병합이 가능합니다.

주의:

| 상황 | 처리 | 리스크 |
| --- | --- | --- |
| 완전 중복 | 제거 | 낮음 |
| 같은 시각, 거의 같은 값 | 평균 병합 | 낮음 |
| 같은 시각, 값이 크게 다름 | 평균 병합 가능하지만 report 확인 필요 | 중간~높음 |

`strict=True`에서 충돌 레코드가 critical로 잡힌 경우에는 transform 전에 중단하는 것이 더 안전합니다.

### 4. Time Sorting

`Datetime` 기준으로 정렬합니다.

```python
df = df.sort_values("Datetime").reset_index(drop=True)
```

정렬은 이후 `diff()`, `reindex()`, `interpolate()` 결과의 전제 조건입니다.

### 5. 10-Minute Reindexing

전체 기간을 10분 단위 grid로 맞춥니다.

```python
df = df.set_index("Datetime").sort_index()
full_index = pd.date_range(df.index.min(), df.index.max(), freq="10min")
df = df.reindex(full_index)
df.index.name = "Datetime"
```

효과:

| reindex 전 | reindex 후 |
| --- | --- |
| 누락 시각이 행으로 보이지 않음 | 누락 시각이 `NaN` 행으로 드러남 |
| 시간 간격 불규칙성을 따로 추적해야 함 | 모든 행이 10분 grid 위에 놓임 |
| 보간 대상이 불명확할 수 있음 | 보간 대상이 명확해짐 |

원본 데이터는 이미 10분 간격이 완전하지만, 운영 파이프라인에서는 누락 시각을 명시화하기 위해 이 단계를 두는 것이 안전합니다.

### 6. Physical Range Cleaning

validate와 동일한 `VALID_RANGES`를 사용합니다. 범위를 벗어난 값은 삭제하지 않고 `NaN`으로 치환합니다.

```python
for col, (lo, hi) in VALID_RANGES.items():
    mask = ~df[col].between(lo, hi)
    df.loc[mask, col] = np.nan
```

삭제가 아니라 `NaN` 치환을 선택하는 이유:

| 방식 | 장점 | 단점 |
| --- | --- | --- |
| 행 삭제 | 간단함 | 10분 시간축이 깨질 수 있음 |
| `NaN` 치환 | 시간축 유지, 보간 가능 | 보간 정책 필요 |

시계열 데이터에서는 시간축 보존이 중요하므로 `NaN` 치환이 더 적절합니다.

### 7. Missing Value Interpolation

결측은 시간순 선형 보간으로 처리합니다.

```python
df[NUMERIC_COLS_EXPECTED] = df[NUMERIC_COLS_EXPECTED].interpolate(
    method="linear",
    limit_direction="both",
)
```

다만 큰 갭을 무조건 보간하면 실제 전력 패턴을 왜곡할 수 있습니다. 따라서 운영 기준에서는 보간 가능한 연속 결측 길이를 제한하는 편이 좋습니다.

권장 정책:

| 결측 길이 | 처리 |
| --- | --- |
| 1~6개 연속 결측, 최대 1시간 | 선형 보간 허용 |
| 7개 이상 연속 결측 | 자동 보간하지 않고 report 확인 |
| 하루 단위 누락 | transform 중단 또는 별도 복구 필요 |

구현 예시:

```python
df[NUMERIC_COLS_EXPECTED] = df[NUMERIC_COLS_EXPECTED].interpolate(
    method="linear",
    limit=6,
    limit_direction="both",
)
```

원본 파일에는 결측이 없으므로 이 로직은 주로 향후 다른 입력 파일을 안전하게 처리하기 위한 방어 코드입니다.

### 8. What Transform Does Not Change

아래 항목은 transform에서 수정하지 않습니다.

| 항목 | 이유 |
| --- | --- |
| IQR 통계적 이상치 | 정상적인 일중/계절 피크일 수 있음 |
| 전력 소비 피크 | 실제 수요 패턴일 수 있음 |
| 일사량의 낮 시간 급증 | 자연스러운 태양광 패턴일 수 있음 |
| 30분 이상 큰 갭 | 단순 선형 보간으로 왜곡될 수 있음 |
| 날짜 범위 끝의 `2017-12-31` 부재 | 원본 데이터 범위 자체가 `2017-12-30`까지임 |

특히 `DiffuseFlows`의 IQR 이상치는 밤에는 거의 0, 낮에는 크게 증가하는 분포 때문에 생기는 경우가 많습니다. 오류라고 단정하지 않고 품질 리포트에 남긴 뒤 분석자가 확인하도록 둡니다.

### Transform Summary

validate 항목과 transform 대응 관계는 다음과 같습니다.

| validate 항목 | transform 대응 |
| --- | --- |
| 스키마 확인 | 필수 컬럼 없으면 transform 진입 전 중단 |
| 숫자 타입 검증 | `pd.to_numeric(errors="coerce")` |
| Datetime 파싱 검증 | `pd.to_datetime(..., errors="coerce")`, `NaT` 행 제거 |
| 완전 중복 | `drop_duplicates()` |
| Datetime 중복/충돌 | `groupby("Datetime").mean()` 병합, report 유지 |
| 시간 정렬 여부 | `sort_values("Datetime")` |
| 시간 간격 불규칙 | 10분 `date_range`로 `reindex()` |
| 물리 범위 초과 | `NaN` 치환 |
| 결측치 | 짧은 구간 선형 보간 |
| IQR 이상치 | 자동 수정하지 않음 |
| 큰 시간 갭 | 자동 수정하지 않음, report 확인 |

### Clean Output

정제 후 기대되는 출력 특성:

| 항목 | 기대값 |
| --- | --- |
| 행 단위 | 10분 timestamp |
| 정렬 | `Datetime` 오름차순 |
| 중복 `Datetime` | 없음 |
| 수치 컬럼 dtype | numeric |
| 물리 범위 초과값 | 없음 또는 보간 후 해소 |
| 짧은 결측 | 보간 완료 |
| 파생 피처 | 없음 |

최종 저장은 보통 아래 두 형식을 사용합니다.

```text
powerconsumption_clean.parquet
powerconsumption_clean.csv
quality_report.json
```
