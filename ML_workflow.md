# Power Consumption ML Workflow

---

## 1. End-to-End Pipeline Architecture

```text
                       [ TRAINING LANE ]                                     [ TEST LANE ]
                 data/powerconsumption.csv                             data/powerconsumption.csv
               (earliest rows, before the cut)                   (newest rows = last test_fraction)
                               │                                                     │
                               ▼                                                     ▼
                       ┌────────────────┐                                    ┌────────────────┐
     prepare.json ───> │  train_prepare │                  prepare.json ──>  │  test_prepare  │
                       └───────┬────────┘                                    └───────┬────────┘
                               │ trainval_raw + val_start                            │ test_raw
                               ▼                                                     ▼
                       ┌────────────────┐                                    ┌────────────────┐
                       │ train_featurize│── scaler.json + features.json ──>  │ test_featurize │
                       └───────┬────────┘                                    └───────┬────────┘
                               │ train/val.parquet                                   │ test.parquet
                               ▼                                                     ▼
                       ┌────────────────┐                                    ┌────────────────┐
  optuna.json ───────> │      train     │────────── model.txt ────────────>  │      test      │
                       └───────┬────────┘                                    └───────┬────────┘
   parity_plot (train) <───────┤                                                     ├──> parity_plot (test)
publish_artifacts (train) <────┤ model + metrics                           metrics   └──> publish_artifacts (test)
                               ▼                                           + pred.csv
                       ┌────────────────┐
                       │    validate    │
                       └───────┬────────┘
 parity_plot (validation) <────┤
publish_artifacts (validation) <┤ val metrics
```

---

## 2. Detailed Logic by Stage

다이어그램의 각 박스(task)를 헤더로 하여, 실행 순서대로 상세 로직을 기술합니다.

### train_prepare

1. **원본 데이터 분할:** 원본 csv 로드 후 `test_fraction` 기준으로 Training Lane 데이터를 물리적 분할합니다.
2. **데이터 품질 검증 및 시간축 확정:**
   * **필수 컬럼 검증:** `Datetime`, 기상 5개, 전력 3개 등 총 9개 필수 스키마의 존재 여부를 체크합니다. (※ 정답을 미리 상정한 정적 검증이 아닌지 검사 로직 재확인 필요)
   * **시계열 정렬 및 보간:** 10분 단위 그리드로 `reindex` 후 누락 구간은 선형 보간(`limit=6`) 처리합니다.

### test_prepare

1. **원본 데이터 분할:** 원본 csv 로드 후 `test_fraction` 기준으로 Test Lane 데이터를 물리적 분할합니다.
2. **데이터 품질 검증 및 시간축 확정:**
   * **필수 컬럼 검증:** `Datetime`, 기상 5개, 전력 3개 등 총 9개 필수 스키마의 존재 여부를 체크합니다. (※ 정답을 미리 상정한 정적 검증이 아닌지 검사 로직 재확인 필요)
   * **시계열 정렬 및 보간:** 10분 단위 그리드로 `reindex` 후 누락 구간은 선형 보간(`limit=6`) 처리합니다.

### train_featurize

1. **X/Y 변수 정의 및 그룹핑:**
   * **Y (Target, 3개):** `PowerConsumption_Zone1`, `PowerConsumption_Zone2`, `PowerConsumption_Zone3`
   * **X (Features, 총 23개):**
     * *기상 그룹 (5개):* `Temperature`, `Humidity`, `WindSpeed`, `GeneralDiffuseFlows`, `DiffuseFlows`
     * *시간/주기 그룹 (6개):* `Hour_sin`, `Hour_cos`, `Month_sin`, `Month_cos`, `DayOfWeek`, `is_weekend`
     * *Lag 그룹 (6개):* 주요 변수의 Lag 1(10분 전), Lag 6(1시간 전)
     * *Rolling 그룹 (6개):* 과거 1시간(Window 6), 3시간(Window 18)의 이동 평균 및 표준편차 (`closed='left'`)
2. **Data Split (Silver Layer):** Training Lane 데이터를 `val_start` 시점 기준으로 Train과 Validation 세트로 분할합니다.
3. **Robust Scaling:** `RobustScaler`를 사용하며, 오직 **Training Lane의 Train 데이터로만 `fit`을 수행**합니다. 도출된 `scaler.json`과 `features.json`을 고정하여 Test Lane에는 `transform`만 적용합니다.

### test_featurize

1. **X/Y 변수 정의:** `train_featurize`에서 확정된 `features.json`의 X/Y 스키마를 그대로 따릅니다.
2. **Transform 적용:** `train_featurize`가 산출한 `scaler.json`을 로드하여 **`transform`만** 적용합니다 (Test Lane에서는 재`fit` 하지 않음).

### train

1. **구역별 개별 모델 학습:** `Zone 1, 2, 3` 각각 독립된 앙상블 모델(LightGBM, XGBoost, CatBoost)을 구성합니다.
2. **하이퍼파라미터 최적화:** `optuna.json` 규칙 기반으로 튜닝하며 오버피팅 방지를 위해 `early_stopping_rounds`를 적용합니다.
3. **결과 및 아티팩트 방출:** 최적 모델(`model.txt`)을 저장하고 학습 오차 확인을 위한 **`work/parity_train.png`**를 그린 후 Prefect UI로 메트릭을 발행합니다.

### validate

1. **교차 검증 수행:** 격리되었던 `val.parquet`과 `model.txt`를 로드하여 시계열 순서가 보존된 일반화 성능을 평가합니다.
2. **결과 및 아티팩트 방출:** 검증 오차 확인용 **`work/parity_validation.png`**를 생성하고 `val metrics`를 Prefect UI에 기록합니다.

### test

1. **최종 평가:** 완전히 격리되어 있던 최신 데이터 `test.parquet`을 모델에 투입하여 최종 성과를 검증합니다.
2. **결과 및 아티팩트 방출:** 예측 테이블인 `pred.csv`를 빌드하고, 최종 **`work/parity_test.png`** 저장 및 평가 메트릭을 Prefect UI에 최종 발행합니다.

---

## 3. Configuration & Governance

### 3.1 Script & Metadata Management
* **AI Agent 동시 수정 규칙:** 파이프라인 로직 변경 시, Python 소스 코드 스크립트와 본 `README.md` 문서의 명세가 항상 일치하도록 양쪽을 동시에 갱신해야 합니다.
* **버전 관리 규격:** 소스 코드 내 메타데이터에 유일한 버전 변수 `__version__ = "1.0.0"`을 관리합니다. (Major.minor.patch 규칙 준수)
* **형상 추적:** 모든 모델 및 평가 아티팩트 발행 시, 실행 시점의 7자리 단축 Git 커밋 해시(`git rev-parse --short HEAD` 결과물)를 메타데이터에 강제 결합하여 추적성을 확보합니다.