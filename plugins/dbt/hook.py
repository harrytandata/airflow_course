import subprocess
from pathlib import Path
from typing import Dict, List, Optional

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook

from dbt import SNOWFLAKE_PROJECT_DIR, DBT_PATH
ENVIRONMENT = 'local'


class DbtHook(BaseHook):
    """
    DbtHook is a hook wrapper of dbt command line.

    See https://docs.getdbt.com/docs/running-a-dbt-project/dbt-api for
    any updates to stability/documentation of python api.

    Inspired by code from https://github.com/gocardless/airflow-dbt.
    """

    def __init__(
        self,
        snowflake_connection_id: str,
        profiles_dir: str = str(SNOWFLAKE_PROJECT_DIR),
        models: Optional[List[str]] = None,
        dbt_bin: str = DBT_PATH,
        snowflake_account: Optional[str] = None,
        snowflake_user: Optional[str] = None,
        snowflake_password: Optional[str] = None,
        snowflake_role: Optional[str] = None,
        snowflake_database: Optional[str] = None,
        snowflake_warehouse: Optional[str] = None,
        snowflake_schema: Optional[str] = None,
        threads: Optional[int] = None,
        *args,  # to allow passthrough of arbitrary arguments from dbt operator
        **kwargs,
    ):
        super().__init__(snowflake_connection_id)
        self.snowflake_connection_id = snowflake_connection_id
        self.profiles_dir = profiles_dir
        self.models = models
        self.dbt_bin = dbt_bin
        self.snowflake_account = snowflake_account
        self.snowflake_user = snowflake_user
        self.snowflake_password = snowflake_password
        self.snowflake_role = snowflake_role
        self.snowflake_database = snowflake_database
        self.snowflake_warehouse = snowflake_warehouse
        self.snowflake_schema = snowflake_schema
        self.threads = threads
        self.sp: Optional[subprocess.Popen[bytes]] = None

    def run_cli(self, command: Optional[List[str]] = None):
        """Runs dbt command"""
        command = command or ["run"]
        dbt_cmd = [self.dbt_bin, *command, "--profiles-dir", self.profiles_dir]

        if self.threads is not None:
            dbt_cmd.extend(["--threads", str(self.threads)])

        if self.models is not None:
            dbt_cmd.extend(["--models", *self.models])

        if command[0] == "run" and not self.models:
            raise AirflowException("Must specify models for dbt run")

        self.log.info(" ".join(dbt_cmd))
        try:
            sp = subprocess.Popen(
                dbt_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=self.profiles_dir,
                env=self._make_env(),
            )
            self.sp = sp
            output = sp.stdout
            assert output, "Failed to find stdout of dbt subprocess"
            self.log.info("Output:")
            for line in iter(output.readline, b""):
                self.log.info(line.decode("utf-8", errors="replace").rstrip())
            sp.wait()
            self.log.info("Command exited with return code %s", sp.returncode)

            if sp.returncode:
                raise AirflowException(
                    f"dbt command failed with exit code {sp.returncode}"
                )
        finally:
            self.kill()

    def generate_docs(self) -> Path:
        self.run_cli(["docs", "generate"])
        return Path(self.profiles_dir).joinpath("target")

    def kill(self):
        """Ensures subprocess is killed"""
        if self.sp is not None and self.sp.poll() is None:
            self.log.info("Killing dbt")
            self.sp.kill()

    def _make_env(self) -> Dict[str, str]:
        hook = SnowflakeHook(
            self.snowflake_connection_id,
            account=self.snowflake_account,
            user=self.snowflake_user,
            password=self.snowflake_password,
            role=self.snowflake_role,
            database=self.snowflake_database,
            warehouse=self.snowflake_warehouse,
            schema=self.snowflake_schema,
        )
        conn_params = hook._get_conn_params()

        return {
            "SNOWFLAKE_ACCOUNT": conn_params["account"],
            "SNOWFLAKE_USER": conn_params["user"],
            "SNOWFLAKE_PASSWORD": conn_params["password"],
            "SNOWFLAKE_ROLE": conn_params["role"],
            "SNOWFLAKE_DATABASE": conn_params["database"],
            "SNOWFLAKE_WAREHOUSE": conn_params["warehouse"],
            "SNOWFLAKE_SCHEMA": conn_params["schema"],
            "APPLICATION_ENV": ENVIRONMENT
        }
