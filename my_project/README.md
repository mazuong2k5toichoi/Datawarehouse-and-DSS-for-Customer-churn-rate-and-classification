# my_project

Lightweight service and ELT pipeline for the Customer Churn assignment.

## Quick links
- Project entrypoint: [my_project/main.py](my_project/main.py)  
- Container config: [my_project/docker-compose.yaml](my_project/docker-compose.yaml)  
- Python packaging / deps: [my_project/pyproject.toml](my_project/pyproject.toml)  
- ELT pipeline package: [my_project/elt-pipeline/__init__.py](my_project/elt-pipeline/__init__.py)  
- Ingest implementation: [my_project/elt-pipeline/ingest/psql_ingest.py](my_project/elt-pipeline/ingest/psql_ingest.py)  
- Database init utilities: [my_project/init-db](my_project/init-db)  
- Sample/backup DB file: [my_project/CompanyX.bak](my_project/CompanyX.bak)

## Structure
- main.py — CLI / runtime bootstrap for local runs and orchestration. See [my_project/main.py](my_project/main.py).
- docker-compose.yaml — local service + db composition for development.
- elt-pipeline/ — package containing ELT code (extraction, transformation, load). Inspect [my_project/elt-pipeline/ingest/psql_ingest.py](my_project/elt-pipeline/ingest/psql_ingest.py) for ingestion logic.
- init-db/ — schema and seed helpers for initializing the database before running the pipeline.

## Setup (local, quick)
1. Create virtual env and install deps from [my_project/pyproject.toml](my_project/pyproject.toml).
2. Start services with Docker Compose:
   - docker-compose up --build (uses [my_project/docker-compose.yaml](my_project/docker-compose.yaml))
3. Initialize DB if needed using scripts in [my_project/init-db](my_project/init-db) or restore [my_project/CompanyX.bak](my_project/CompanyX.bak).

## Run
- Local (quick): python [my_project/main.py](my_project/main.py)
- Using Docker: docker-compose up --build

## Development notes
- Place new ELT modules under [my_project/elt-pipeline](my_project/elt-pipeline).
- Keep ingestion code in [my_project/elt-pipeline/ingest/psql_ingest.py](my_project/elt-pipeline/ingest/psql_ingest.py).

## Contact / Issues
Open issues in the repository or edit this file with improvements.