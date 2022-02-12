PACKAGES := "dags"


.PHONY: build
build:
	docker build . -f Dockerfile --tag airflow:harrytan

.PHONY: version
version:
	docker run --rm --name testairflow airflow:harrytan version

.PHONY: up
up:
	docker-compose up

.PHONY: down
down: 
	docker-compose down

.PHONY: dbt
dbt:
	python -m venv .dbt-venv
	.dbt-venv/bin/pip install -r requirements_dbt.txt -c constraints_dbt.txt