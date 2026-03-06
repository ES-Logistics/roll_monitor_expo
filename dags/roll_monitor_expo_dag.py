import pendulum

from datetime import datetime, timedelta

from airflow import DAG
from docker.types import Mount

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable


LOCAL_TIMEZONE = pendulum.timezone("America/Sao_Paulo")
ENV_VARS = Variable.get("ROLL_MONITOR_ENV", deserialize_json=True, default_var={})

DEFAULT_ARGS = {
    "owner": "ES Logistics",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

COMMON_ENV = {**ENV_VARS, "PYTHONUNBUFFERED": "1"}

SECRETS_MOUNT = Mount(
    target="/app/src/assets",
    source="/home/ubuntu/projects/data-platform/secrets",
    type="bind",
    read_only=True,
)

with DAG(
    dag_id="roll_monitor_expo",
    default_args=DEFAULT_ARGS,
    description="Executa ciclo de monitoramento do Roll Monitor Expo a cada 10 minutos",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 3, 6, tzinfo=LOCAL_TIMEZONE),
    catchup=False,
    tags=["Roll Monitor", "Expo", "Processo"],
) as dag:

    run_monitor = DockerOperator(
        task_id="run_monitor",
        image="roll-monitor-expo:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        command=["python", "src/main.py", "monitor"],
        environment=COMMON_ENV,
        mounts=[SECRETS_MOUNT],
        mount_tmp_dir=False,
    )
