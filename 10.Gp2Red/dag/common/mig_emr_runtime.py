from __future__ import annotations

from typing import Any


def terminate_profile(profile: dict[str, Any]) -> None:
    if not bool(profile.get("dedicated_yn")) or not bool(profile.get("auto_term_yn")):
        return
    cluster_id = str(profile.get("emr_cluster_id") or "").strip()
    if not cluster_id:
        raise RuntimeError("자동 종료할 EMR 클러스터 또는 애플리케이션 ID가 없습니다.")
    aws_conn_id = str(profile.get("aws_conn_id") or "").strip()
    if not aws_conn_id:
        raise RuntimeError("EMR AWS 접속 ID가 없습니다.")
    try:
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
    except ImportError as error:
        raise RuntimeError("Airflow Amazon Provider를 설치하십시오.") from error
    hook = AwsBaseHook(aws_conn_id=aws_conn_id)
    emr_type = str(profile.get("emr_type_cd") or "").upper()
    if emr_type == "EMR_EC2":
        hook.get_client_type("emr").terminate_job_flows(JobFlowIds=[cluster_id])
        return
    if emr_type == "EMR_SERVERLESS":
        hook.get_client_type("emr-serverless").stop_application(applicationId=cluster_id)
        return
    raise RuntimeError("지원하지 않는 EMR 유형입니다.")
