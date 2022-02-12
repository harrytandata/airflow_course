# DBT

## Development
One can develop and iterate on the dbt models locally by following the steps below

#### Create Python Environment
1. Run `make dbt` in the repo's root directory. Then, run `cd my_dbt` to work inside
this directory.  The rest of the commands assumes that `my_dbt` is the current directory.

#### Export local environment variables
Copy `snowflake_env.sh` into another file.  For the following examples, this assumes that
the copied filename is `my_snowflake_env.sh`.  For example, `cp snowflake_env.sh my_snowflake_env.sh`.
Then, override one's credentials in the copied file. *Please make sure to never commit this file to git!*

#### Activate the local python environment
Run `source ../.dbt-venv/bin/activate` to activate the local environment.

#### Export the snowflake credentials into the current environment
Run `source my_snowflake_env.sh`.

#### Developing a model
##### Add model schema and sql files
Choose a model name and create a directory under `models/`.
For example, `mkdir looker_optimization` will create a model called `looker_optimization`.
Then, create the necessary sql files under that directory.
The `schema.yml` allows one to document the columns in markdown, column conditions, link to markdown pages and plenty more.
But this file is NOT strictly necessary to create a model.  For an example, please look at
`models/campaign_events/clicks/schema.yml`.  For more info on `schema.yml`, please see [here](https://docs.getdbt.com/reference/declaring-properties).

##### Add model to `dbt_project.yml`
In this file, the name of the model MUST be added under `my_dbt`.
Choose a schema (snowflake schema) where the tables and views in this model will live.
Note, the sql files that have the `config` pragma can override the settings here.  These are just the
default settings that will be applied.

##### Troubleshooting
Sometimes the sql fails to run.  In these cases, check the fully compiled SQL in the directory `target/run/my_dbt/models/`. 
The line numbers in the error messages should correspond to the sql files here.

##### Run a model
Run `dbt run --profiles-dir . --models {insert_model_name_here}`.  If you would like to run a specific table or view
within the model, you may further drill into it by running `dbt run --profiles-dir . --models {model_name}.{table or view name}`.
The model entity path mirrors the directory path and instead of `/` separating each level, 

For example, to run all campaign_event models, run `dbt run --profiles-dir . --models campaign_events`.
To run all campaign_event open models, run `dbt run --profiles-dir . --models campaign_events.opens`.

If a model is missing, you can list all models via the command `dbt ls`.

#### Generate and view auto generated dbt documentation
Run `dbt docs generate && dbt docs serve`.  This should generate the documentation and pop open the local
browser and open generate the dbt docs.

#### Running the model on a schedule
Please follow the examples of `campaign_events_dbt.py` or `cloudflare_log_aggregation.py` to run your model
on a regular basis.  If you **really** need the functionality to refresh the dbt model right after snowflake
ingestion, follow the example in `dags/daily_append/em_log.py`, where the `DbtOperator` is chained into the upstream tasks.
For example, the following line demonstrate what chaining an ingestion task into a dbt model run looks like.
