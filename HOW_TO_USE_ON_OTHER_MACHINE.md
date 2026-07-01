# 다른 로컬 환경에서 Prefect ETL 파이프라인 사용하기

GitHub에 올린 `prefect_power` 레포를 다른 컴퓨터에서 받아 똑같이 실행하는 방법입니다.

## 1. 레포 clone 받기

**GitHub Desktop 사용 시**
File → Clone Repository → 본인 GitHub 계정에서 `prefect_power` 레포 선택 → 원하는 로컬 경로 지정 → Clone

**명령어 사용 시**
```
git clone https://github.com/[본인계정]/prefect_power.git
cd prefect_power
```

## 2. Kaggle에서 데이터 다운로드

csv 파일은 레포에 포함되어 있지 않으므로 직접 받아야 합니다. Kaggle에서 "Power Consumption of Tetouan City" 데이터셋을 검색해서 다운로드한 뒤, `powerconsumption.csv`라는 이름으로 clone받은 폴더(코드 파일들과 같은 위치)에 넣습니다.

## 3. Python 설치 확인

```
python --version
```
3.x 버전이 출력되면 정상입니다. 출력되지 않으면 python.org에서 설치가 필요합니다.

## 4. 패키지 설치

레포 폴더 안에서:
```
pip install -r requirements.txt
```

## 5. 파이프라인 실행

```
python etl_flow.py --input powerconsumption.csv --outdir ./output
```

`Finished in state Completed()`가 보이면 성공입니다. `output` 폴더에 정제된 csv/parquet 파일과 품질 리포트(`quality_report.json`)가 생성됩니다.

데이터 품질에 심각한(critical) 문제가 있을 때 파이프라인을 바로 중단시키고 싶다면 `--strict` 옵션을 추가하세요.

```
python etl_flow.py --input powerconsumption.csv --outdir ./output --strict
```

> `quality_report.json`에는 결측치/중복/음수값/시간 간격뿐 아니라 물리적으로 불가능한 값, 통계적 이상치(IQR), 컬럼 타입 이상 등도 함께 기록됩니다. 문제는 `critical_issues`(반드시 확인 필요)와 `warnings`(참고용)로 구분되어 있습니다.

## 6. (선택) Prefect 대시보드로 시각적으로 확인하기

새 명령 프롬프트 창을 열고:
```
prefect server start
```
이 창은 켜둔 채로 두고, 브라우저에서 `http://127.0.0.1:4200` 에 접속합니다.

그 다음 원래 창으로 돌아가 5단계 명령을 다시 실행하면, 그 컴퓨터의 로컬 대시보드(Runs 메뉴)에 실행 기록이 표시되고 Artifacts 탭에서 ETL 요약 리포트도 확인할 수 있습니다.

## 참고

- 대시보드(`127.0.0.1:4200`)는 각자의 컴퓨터에서만 보이는 로컬 주소입니다. 공유되는 것은 코드뿐이며, 각 컴퓨터에서 직접 서버를 띄워 자신의 실행 기록을 보는 구조입니다.
- 정기 자동 실행(스케줄링)이 필요하면 레포의 `deploy.py`와 `README.md`의 "정기 스케줄 배포" 섹션을 참고하세요.
