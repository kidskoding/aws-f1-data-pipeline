SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

SEASONS     ?= 2021-current
TERRAFORM   ?= terraform
TF          := $(TERRAFORM) -chdir=infra
SPARK_IMAGE := apache/spark:3.5.4-scala2.12-java17-python3-ubuntu

# Spark 3.5 in Docker: the same Spark version AWS Glue 5.0 runs.
SPARK := docker run --rm -u $(shell id -u):$(shell id -g) -e HOME=/tmp -v /etc/passwd:/etc/passwd:ro \
	-v $(CURDIR):/app -w /app $(SPARK_IMAGE) /opt/spark/bin/spark-submit --master 'local[2]' \
	--conf spark.jars.ivy=/tmp/.ivy2 --driver-java-options -Duser.home=/tmp

.PHONY: help install lint format typecheck test test-spark check extract transform preview package plan deploy run query teardown clean

help: ## Show this help
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  \033[36m%-11s\033[0m %s\n", $$1, $$2}'

install: ## Install dev tools into .venv (uv)
	uv sync

lint: ## Lint Python and Terraform
	uv run ruff check .
	uv run ruff format --check .
	$(TF) fmt -check -recursive
	$(TF) init -backend=false -input=false >/dev/null
	$(TF) validate

format: ## Auto-format Python and Terraform
	uv run ruff check --fix .
	uv run ruff format .
	$(TF) fmt -recursive

typecheck: ## Static type check the package
	uv run mypy

test: ## Unit tests (extract, warehouse, load)
	uv run pytest

test-spark: ## Glue transform tests on local Spark 3.5 (Docker)
	$(SPARK) tests/spark/run.py

check: lint typecheck test test-spark ## Everything CI runs

extract: ## Pull F1 data from the API into data/raw (SEASONS=2021-current)
	uv run python -m f1_pipeline.extract --seasons $(SEASONS) --out data/raw

transform: ## Run the Glue job locally: data/raw to data/curated
	$(SPARK) glue/transform_f1.py --raw_path data/raw --curated_path data/curated --rejects_path data/rejects/results

preview: ## Run the analytics SQL on local Parquet with DuckDB
	uv run python -m f1_pipeline.query --local

package: ## Build the Lambda zip (build/lambda.zip)
	uv run python scripts/package_lambda.py

plan: package ## Show what Terraform would change in AWS
	$(TF) init -input=false
	$(TF) plan

deploy: package ## Create or update every AWS resource, then save the outputs
	$(TF) init -input=false
	$(TF) apply
	$(TF) output -json > build/terraform-outputs.json

run: ## Start the Step Functions pipeline and wait (SEASONS=2021-current)
	./scripts/run.sh '$(SEASONS)'

query: ## Run the analytics SQL on Redshift Serverless
	uv run python -m f1_pipeline.query

teardown: package ## Delete every AWS resource this project created
	$(TF) destroy
	rm -f build/terraform-outputs.json

clean: ## Delete local data and build output
	rm -rf data build
