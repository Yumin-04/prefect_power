# Power Consumption ETL with Prefect

Kaggle "Power Consumption of Tetouan City" 데이터셋(10분 간격, 기상변수 + 3개 구역 전력소비량)을
정제하는 Prefect ETL 파이프라인입니다.

## 파일 구성

- `etl_flow.py` : ETL 메인 플로우 (extract → validate → transform → load)
- `deploy.py` : 정기 스케줄링(cron) 배포 등록 예시 코드
- `requirements.txt` : 의존성 목록

## 데이터 준비 (필수)

이 레포에는 데이터 파일(`powerconsumption.csv`)이 포함되어 있지 않습니다.
아래 Kaggle 데이터셋을 직접 다운로드한 뒤, 이 README와 같은 폴더에
`powerconsumption.csv` 라는 이름으로 넣어주세요.

- Kaggle: "Power Consumption of Tetouan City" 데이터셋 검색 후 다운로드
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

## 파이프라인 단계

1. **extract**: 원본 CSV 로드 (재시도 2회 포함)
2. **validate**: 결측치, 중복, 음수값, 시간 간격(10분 연속성) 점검 후 품질 리포트 생성
3. **transform**:
   - 중복 제거, 시간순 정렬
   - 음수값(물리적으로 불가능한 값) → NaN 처리 후 시간 기준 선형 보간
   - 결측치 보간
   - 파생 피처 생성: `hour`, `dayofweek`, `month`, `is_weekend`, `PowerConsumption_Total`
4. **load**: 정제 데이터를 parquet + csv로 저장, 품질 리포트(JSON) 저장, Prefect UI 마크다운 아티팩트 생성

## 로컬 실행

```bash
pip install -r requirements.txt

python etl_flow.py --input powerconsumption.csv --outdir ./output
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
- 중복 행: 없음
- 음수값: 없음
- 시간 간격: 전 구간 10분 간격으로 끊김 없음

다만 ETL 코드는 위와 다른(결측/중복/음수/간격 불규칙이 있는) 데이터가 들어와도
안전하게 처리되도록 일반화해서 작성했습니다.
