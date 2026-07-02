# Power Consumption ML Workflow (Edge 3)

본 문서는 Edge 2(Raw Data 및 기초 정제) 단계를 거쳐 **Edge 3(Dataset 및 Validation)** 단계로 진입하는 머신러닝 파이프라인의 핵심 로직과 계층 체계를 정의합니다.

---

## 1. End-to-End Pipeline Architecture

```text
                        [ TRAINING LANE ]                                     [ TEST LANE ]
                  data/powerconsumption.csv                             data/powerconsumption.csv
                (test cut 이전, earliest rows)                    (test cut 이후, 최근 test_fraction)
                                │                                                     │
                                ▼                                                     │
                        ┌────────────────┐                                           │
   optuna.json ───────> │  load_config   │──cfg──────────────────────────────────────┤
                        └───────┬────────┘                                           │
                                │ cfg                                                 ▼
                                ▼                                            ┌────────────────┐
                        ┌────────────────┐                                  │  test_prepare  │
                        │  train_prepare │                                  └───────┬────────┘
                        └───────┬────────┘                                          │ test_raw.parquet
                                │ trainval_raw.parquet + split.json                  ▼
                                ▼                                            ┌────────────────┐
                        ┌────────────────┐  scaler.json + features.json ──> │ test_featurize │
                        │ train_featurize│                                  └───────┬────────┘
                        └───────┬────────┘                                          │ test.parquet
                                │ train.parquet + val.parquet                       ▼
     ┌───────────┐      ┌────────────────┐          model.txt             ┌────────────────┐
     │ optuna DB │◀────▶│      train     │───────────────────────────────▶│      test      │
     └───────────┘      └───────┬────────┘                                └───────┬────────┘
   parity_plot (train) ◀────────┤                                                  ├──▶ parity_plot (test)
publish_artifacts (train) ◀─────┤ model + metrics                        metrics   └──▶ publish_artifacts (test)
                                ▼                                        + pred.csv
                        ┌────────────────┐
                        │    validate    │
                        └───────┬────────┘
   parity_plot (validation) ◀───┤
publish_artifacts (validation) ◀┤ val metrics
```

> `train_featurize` → `test_featurize` 로 넘어가는 화살표(`scaler.json` + `features.json`)와 `train` → `test` 로 넘어가는 화살표(`model.txt`)가 두 레인을 잇는 유일한 연결점입니다. Test 레인은 학습 레인의 스케일러/피처 정의/모델을 그대로 재사용할 뿐, 자체적으로 fit 하지 않습니다.

---

## 2. Detailed Logic by Header

다이어그램의 각 박스(task)를 헤더로 하여, 실행 순서대로 상세 로직을 기술합니다.

### load_config

1. `optuna.json`을 **매 실행마다 새로 읽습니다** (재시작 없이 설정 변경이 즉시 반영되도록).
2. `n_trials`, `cv_folds`, `val_fraction`, `test_fraction`, `sample_rows`, `target_zone`, `random_state`, `storage`, `mlflow_uri`, `study_name`, `lgbm_fixed` 등을 담은 `cfg` 딕셔너리를 반환합니다.

### train_prepare

1. **Edge 2 (Bronze Layer):** `powerconsumption.csv`를 로드하고 `Datetime`을 파싱한 뒤 시간순 정렬합니다. (원본 데이터는 2017-01-01 00:00 ~ 2017-12-30 23:50, 10분 간격 52,416행이며 **결측 타임스탬프·결측값 없이 완전한 균일 그리드**이므로 별도의 reindex/보간 단계는 필요하지 않습니다.)
2. `sample_rows`가 지정된 경우, 가장 최근 N행만 취해 빠른 스모크 테스트를 수행합니다 (`null`이면 전체 52,416행 사용).
3. `test_fraction` 기준으로 test cut을 계산하고, 그 이전 구간을 train+val span으로 확정합니다.
4. `val_fraction`을 적용해 train+val span의 뒤쪽(test 슬라이스 바로 앞)을 validation으로 고정합니다 (`val_start` 시점).
5. **Edge 3 (Silver Layer 진입 전, 필수 컬럼 검증):** `Datetime`, 기상 5개, 전력 3개 등 총 9개 필수 스키마의 존재 여부를 체크합니다. (※ 정답을 미리 상정한 정적 검증이 아닌지 검사 로직 재확인 필요)
6. `trainval_raw.parquet`, `split.json`(`val_start` 시점)을 기록합니다. (raw rows + split 결정만 저장, 스케일링·피처는 아직 없음)

### test_prepare

1. 동일한 CSV에서 가장 최근 `test_fraction` 구간만 슬라이스합니다 (test 구간은 config로 고정).
2. `test_raw.parquet`을 기록합니다. (target 선택, 스케일링, split 없음)

### train_featurize

1. `trainval_raw.parquet`, `split.json`을 읽습니다.
2. **정규화 (Min-Max 0-1 Scaling, Training rows에만 `fit`):** 기상 5개 피처 각각에 대해 **train 구간(val_start 이전)** 의 `lo = min`, `hi = max`를 구해 `x_scaled = (x - lo) / (hi - lo)` 로 0~1 범위로 스케일링합니다. (`hi == lo`인 상수 피처는 0으로 처리) 캘린더 피처는 스케일링하지 않습니다.
3. `{feature: [lo, hi]}` 맵을 **`scaler.json`**으로 저장합니다 (Test 레인이 그대로 재사용, 재fit 없음).
4. **피처 엔지니어링:** `Datetime`으로부터 캘린더 피처 9개(`hour`, `dayofweek`, `is_weekend` + `hour`/`dayofweek`/`month`의 sin·cos 6개)를 파생하고, 선택된 `target_zone`을 `y`로 부착합니다. → 최종 x 피처는 **기상 5개 + 캘린더 9개 = 14개**.
5. `split.json`을 적용해 `train.parquet`, `val.parquet`, `features.json`을 기록합니다.

> LightGBM은 스케일에 불변(magnitude가 아닌 순서로 split)이므로 이 0-1 스케일링은 성능을 바꾸지 않는 구조적 전처리 단계입니다.

### test_featurize

1. `test_raw.parquet`, 학습 레인의 `scaler.json`, `features.json`을 읽습니다.
2. **정규화 (apply 모드):** 학습 시 구한 `lo`/`hi`로 동일한 `(x - lo) / (hi - lo)`를 적용합니다. **재fit 없음** — train/test가 같은 스케일을 공유하며, 학습 범위를 벗어난 test 값은 `[0, 1]` 밖에 위치할 수 있습니다.
3. 동일한 14개 피처를 파생하고 (채점용) 실제 target `y`를 부착합니다.
4. 학습 시 확정된 `features.json` 스키마에 맞춰 정렬합니다 (누락 컬럼은 NaN으로 채움).
5. `test.parquet`을 기록합니다 (split 없음).

### train

1. `train.parquet`, `features.json`을 읽습니다.
2. **단일 LightGBM 모델**(`LGBMRegressor`, 선택된 `target_zone` 하나)에 대해 **Optuna**(TPE sampler, `random_state`)로 하이퍼파라미터(`n_estimators` 200–1200, `learning_rate`, `num_leaves`, `max_depth`, `min_child_samples`, `subsample`, `colsample_bytree`, `reg_alpha`, `reg_lambda`)를 튜닝하며, 각 trial은 **5-fold CV-RMSE**(순수 `KFold` `cross_val_score`)로 채점합니다. (Zone별 개별 앙상블(XGBoost/CatBoost 등)은 사용하지 않습니다 — `target_zone`으로 선택된 단일 타깃에 대해서만 학습)
3. 모든 trial을 `optuna DB`에, best-effort로 MLflow에도 기록합니다.
4. best params로 전체 training set을 재학습(refit)하고 `model.txt`를 저장합니다.
5. train-set RMSE/MAE/R²와 gain 기준 top-15 피처를 계산합니다.
6. 모델과 메트릭(best CV-RMSE + train 메트릭 + top features)을 반환합니다.

### validate

1. `val.parquet`, `model.txt`를 읽습니다.
2. Test 슬라이스 바로 앞의, 격리되어 있던 held-out validation 구간을 예측합니다.
3. RMSE/MAE/R²를 계산합니다 — **단일 시간순 hold-out**이며, k-fold도 랜덤 분할도 아닙니다 (5-fold CV는 `train` 내부 튜닝 전용).

### test

1. `test.parquet`, `model.txt`를 읽습니다.
2. 예측 후 `work/powerconsumption-test-pred.csv`(`Datetime, y_true, y_pred`)를 기록합니다.
3. test 구간의 RMSE/MAE/R²를 계산합니다 (해당 데이터셋은 항상 실제 target을 포함).

### parity_plot / publish_artifacts

1. `train` / `validate` / `test` 각 스테이지 직후 실행되어 `y_true` vs `y_pred` 1:1 산점도(`y = x` 라인 + R²)를 `work/parity_<stage>.png`로 저장하고 Prefect UI에 첨부합니다.
2. `publish_artifacts`는 동일 시점에 해당 스테이지의 메트릭 테이블/마크다운을 Prefect UI에 발행합니다 (best-effort — API 백엔드가 없는 로컬 실행은 건너뜀).

---

## 3. Dataset — 크기·분할 요약

| Split | 규칙 | 전체 실행 (`sample_rows: null`) |
|---|---|---|
| train | 가장 이른 64% (`(1 - val_fraction) × (1 - test_fraction)`) | 33,545행 |
| validation | 그 다음 16% (test 직전 held-out tail) | 8,387행 |
| test | 가장 최근 20% (`test_fraction`) | 10,484행 |

- 분할은 **시간 기준**(셔플 없음)이므로 train < validation < test 순으로 시간이 진행되며, 미래 행이 학습에 leak 되지 않습니다.
- 원본 CSV는 총 52,416행(2017-01-01 00:00 ~ 2017-12-30 23:50, 10분 간격), 9개 컬럼(`Datetime` + 기상 5개 + Zone 전력 3개), **결측치 없음**입니다.

---

## 4. Configuration & Governance

### 4.1 Script & Metadata Management
* **AI Agent 동시 수정 규칙:** 파이프라인 로직 변경 시, Python 소스 코드 스크립트와 본 `README.md`/`ML_workflow.md` 문서의 명세가 항상 일치하도록 양쪽을 동시에 갱신해야 합니다.
* **버전 관리 규격:** 소스 코드 내 메타데이터에 유일한 버전 변수 `__version__ = "1.0.0"`을 관리합니다. (Major.minor.patch 규칙 준수)
* **형상 추적:** 모든 모델 및 평가 아티팩트 발행 시, 실행 시점의 7자리 단축 Git 커밋 해시(`git rev-parse --short HEAD` 결과물)를 메타데이터에 강제 결합하여 추적성을 확보합니다.