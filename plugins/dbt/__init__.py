import os, os.path
from pathlib import Path

try:
    _root_dir = os.environ['AIRFLOW_HOME']
except KeyError:
    raise KeyError('please set AIRFLOW_HOME in your env variables.')
SNOWFLAKE_PROJECT_DIR = Path(_root_dir) / "my_dbt"

DBT_PATH = os.path.join(_root_dir, ".dbt-venv/bin/dbt")

BASE_OPERATOR_ALLOWED_KWARGS = {
    "task_id",
    "owner",
    "email",
    "email_on_retry",
    "email_on_failure",
    "retries",
    "retry_delay",
    "retry_exponential_backoff",
    "max_retry_delay",
    "start_date",
    "end_date",
    "depends_on_past",
    "wait_for_downstream",
    "dag",
    "params",
    "default_args",
    "priority_weight",
    "weight_rule",
    "queue",
    "pool",
    "pool_slots",
    "sla",
    "execution_timeout",
    "on_execute_callback",
    "on_failure_callback",
    "on_success_callback",
    "on_retry_callback",
    "trigger_rule",
    "resources",
    "run_as_user",
    "task_concurrency",
    "executor_config",
    "do_xcom_push",
    "inlets",
    "outlets",
    "task_group",
    "doc",
    "doc_md",
    "doc_json",
    "doc_yaml",
    "doc_rst",
}