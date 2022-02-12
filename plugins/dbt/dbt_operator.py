from typing import List, Optional

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from dbt.hook import DbtHook
from dbt import BASE_OPERATOR_ALLOWED_KWARGS


class DbtOperator(BaseOperator):
    """
    Operator that takes snowflake credentials from a snowflake
    connection id and uses them to run dbt commands.

    Makes assumptions specific to this project such as the location of
    the profiles.yml file and the virtualenv that dbt is installed in.
    dbt is installed in a separate virtualenv than airflow due to many
    conflicting dependency versions.

    Inspired by code from https://github.com/gocardless/airflow-dbt.
    That codebase is copyright 2019 GoCardless licensed under the MIT
    License.
    """

    @apply_defaults
    def __init__(
        self,
        snowflake_connection_id: str,
        dbt_command: Optional[List[str]] = None,
        *args,
        **kwargs
    ):
        super_kwargs = {k: kwargs[k] for k in kwargs.keys()
                        if k in BASE_OPERATOR_ALLOWED_KWARGS}
        super().__init__(*args, **super_kwargs)
        self.snowflake_connection_id = snowflake_connection_id
        self.dbt_command = dbt_command
        self.args = args
        self.kwargs = kwargs
        self.hook: Optional[DbtHook] = None

    def execute(self, context):
        """
        Runs dbt using snowflake credentials specified in the connection,
        allowing overrides from the constructor.
        """
        self.hook = DbtHook(
            snowflake_connection_id=self.snowflake_connection_id,
            *self.args,
            **self.kwargs
        )
        print(self.dbt_command)
        self.hook.run_cli(self.dbt_command)

    def on_kill(self):
        if self.hook is not None:
            self.hook.kill()
