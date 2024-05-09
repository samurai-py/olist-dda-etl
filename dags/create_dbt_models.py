import os
import logging
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag

from cosmos.profiles import DatabricksTokenProfileMapping
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig

logger = logging.getLogger(__name__)
doc_md = """
"""

default_args = default_args = {
    "owner": "Mário Vasconcelos",
    "retries": 1,
    "retry_delay": 0
}

default_dbt_root_path = Path(__file__).parent / "dbt"
dbt_root_path = Path(os.getenv("DBT_ROOT_PATH", default_dbt_root_path))

@dag(
    dag_id="create_dbt_models",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    max_active_runs=1,
    default_args=default_args,
    catchup=False,
    tags=['dev', 'dbt', 'databricks', 'cosmos']
)

def create_dbt_models():
    """
    """


    profile_config = ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=DatabricksTokenProfileMapping(
            conn_id="databricks_conn",
            profile_args={
                "schema": "dev"
            }
        )
    )

    dbt_task_group = DbtTaskGroup(
        project_config=ProjectConfig(default_dbt_root_path),
        profile_config=profile_config,
        operator_args={
            "install_deps": True,
        }
    )


    dbt_task_group


dag = create_dbt_models()