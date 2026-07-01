"""
Prefect Deployment 등록 스크립트 (예시)

이 스크립트는 etl_flow.py 의 power_consumption_etl 플로우를
- 매일 새벽 1시(cron)에 자동 실행되도록 스케줄을 등록하는 예시입니다.
- 실제 운영에서는 work pool을 미리 만들어야 합니다.
  예: prefect work-pool create my-process-pool --type process

사용법:
    # 1) Prefect 서버/클라우드 로그인 또는 로컬 서버 기동
    prefect server start          # 로컬에서 띄울 경우
    # 또는 Prefect Cloud 사용 시 `prefect cloud login`

    # 2) work pool 생성 (최초 1회)
    prefect work-pool create my-process-pool --type process

    # 3) 배포 등록 + 스케줄 활성화
    python deploy.py

    # 4) 워커 실행 (배포된 플로우를 실제로 실행시켜 줌)
    prefect worker start --pool my-process-pool
"""

from etl_flow import power_consumption_etl


if __name__ == "__main__":
    power_consumption_etl.from_source(
        source=".",  # 현재 디렉토리의 코드를 사용 (Git repo URL로 교체 가능)
        entrypoint="etl_flow.py:power_consumption_etl",
    ).deploy(
        name="power-consumption-etl-daily",
        work_pool_name="my-process-pool",
        cron="0 1 * * *",  # 매일 새벽 1시 실행
        parameters={
            "input_path": "powerconsumption.csv",
            "outdir": "./output",
            # 운영 배치는 strict=True 권장: critical 품질 이슈 발생 시
            # 잘못된 데이터를 그대로 적재하지 않고 파이프라인을 중단시킴
            "strict_validation": True,
        },
        tags=["etl", "power-consumption", "kaggle"],
        description="Power Consumption 시계열 데이터 일일 ETL 배치",
    )
