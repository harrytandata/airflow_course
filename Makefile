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