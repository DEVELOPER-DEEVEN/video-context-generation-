# video-context-crj

Batch-process a list of video (or any URL) resources stored in BigQuery by calling an existing Cloud Run service that processes a single URL and returns text (e.g., a summary/"context"). This repo contains a Cloud Run Job friendly Python app that:

- Fetches pending URLs from a BigQuery source table.
- Marks them as PROCESSING to avoid duplication.
- Calls your existing URL Processor service in parallel.
- Writes results and status to a BigQuery target table.

You can use this to turn a backlog of links (e.g., Code Vipassana session videos) into structured text chapters/insights using your own Cloud Run service (which might leverage Gemini or any other model).


## Architecture

```mermaid
flowchart TD
  subgraph BigQuery
    SRC[Source Table\n(id, url, status=PENDING|PROCESSING)]
    TGT[Target Table\n(id, context, status=COMPLETED|FAILED_PROCESSING)]
  end

  CRJ[Cloud Run Job\nthis repo: main.py] -->|select PENDING| SRC
  CRJ -->|mark PROCESSING| SRC

  CRJ -- parallel POST --> SRV[Your URL Processor\n(Cloud Run service)]
  SRV -- text response --> CRJ

  CRJ -->|write results| TGT
```


## Repository structure

- `main.py`: Orchestrates batch processing using BigQuery and calls your URL Processor service.
- `requirements.txt`: Python dependencies.
- `Dockerfile`: Containerizes the job for Cloud Run Jobs.
- `Cloud_Run_Function/`: Example functions folder for generating video insights (optional for this job; you may not need it if you already have a processor service).
- `LICENSE`: License file.


## Prerequisites

- A Google Cloud project with billing enabled.
- Local tools:
  - `gcloud` CLI (authenticated: `gcloud auth login` and `gcloud config set project <PROJECT_ID>`)
  - Docker (for local image builds) or Cloud Build
  - Python 3.10+ if running locally without Docker
- IAM permissions to use BigQuery and Cloud Run.

Enable required APIs:

```bash
gcloud services enable \
  run.googleapis.com \
  bigquery.googleapis.com \
  artifactregistry.googleapis.com
```


## BigQuery schema

This job expects two tables in the same dataset:

- Source table: holds work queue of URLs
  - Columns: `id STRING` (unique id), `url STRING`, `status STRING` (values: `PENDING`, `PROCESSING`, optionally `DONE`)
- Target table: holds results
  - Columns: `id STRING` (same id as source), `context STRING`, `status STRING` (e.g., `COMPLETED`, `FAILED_PROCESSING`)

Example DDL (replace variables accordingly):

```sql
-- Variables
-- PROJECT:   your GCP project id
-- DATASET:   dataset name (must exist)
-- SRC_TABLE: source table name
-- TGT_TABLE: target table name

-- Create source table
CREATE TABLE `PROJECT.DATASET.SRC_TABLE` (
  id STRING NOT NULL,
  url STRING NOT NULL,
  status STRING NOT NULL
);

-- Create target table
CREATE TABLE `PROJECT.DATASET.TGT_TABLE` (
  id STRING NOT NULL,
  context STRING,
  status STRING
);

-- Example: seed the source table
INSERT INTO `PROJECT.DATASET.SRC_TABLE` (id, url, status) VALUES
  ('1', 'https://example.com/video1', 'PENDING'),
  ('2', 'https://example.com/video2', 'PENDING');
```


## Environment variables

Set these for local runs, Docker, and your Cloud Run Job:

- `BIGQUERY_PROJECT`: GCP project id containing the dataset.
- `BIGQUERY_DATASET`: BigQuery dataset name.
- `BIGQUERY_TABLE_SOURCE`: Source table name (with URLs and `status`).
- `BIGQUERY_TABLE_TARGET`: Target table name (where results are written).
- `URL_PROCESSOR_SERVICE_URL`: HTTPS URL of your Cloud Run service endpoint that processes a single URL and returns text.

See `main.py` for defaults and validation. The job will exit if these are not set appropriately.


## How the job works

Key logic in `main.py`:

- Reads up to `BATCH_SIZE` rows where `status='PENDING'` from `BIGQUERY_TABLE_SOURCE`.
- Immediately marks those rows `PROCESSING` to avoid duplicate work.
- In parallel (configurable via `MAX_CONCURRENT_TASKS_PER_INSTANCE`), calls `URL_PROCESSOR_SERVICE_URL` with a JSON payload `{"name": "<url>"}`.
- Collects text results. If the call times out or fails, marks as `FAILED_PROCESSING`.
- Writes the `context` and `status` by `id` into `BIGQUERY_TABLE_TARGET`.

Tunable knobs in `main.py`:

- `BATCH_SIZE = 5`
- `MAX_CONCURRENT_TASKS_PER_INSTANCE = 5`
- `URL_PROCESSOR_TIMEOUT_SECONDS = 600`


## Run locally (without Docker)

1) Create and activate a Python virtual environment (optional):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2) Authenticate for BigQuery and set env vars:

```bash
gcloud auth application-default login
export BIGQUERY_PROJECT="<PROJECT_ID>"
export BIGQUERY_DATASET="<DATASET>"
export BIGQUERY_TABLE_SOURCE="<SRC_TABLE>"
export BIGQUERY_TABLE_TARGET="<TGT_TABLE>"
export URL_PROCESSOR_SERVICE_URL="https://<your-service>-<hash>-<region>.run.app"
```

3) Run:

```bash
python main.py
```


## Docker build and run

Build the image:

```bash
docker build -t video-context-crj:local .
```

Run the container (ADC via workload identity is preferred in Cloud; locally you can mount a key if needed):

```bash
docker run --rm \
  -e BIGQUERY_PROJECT="<PROJECT_ID>" \
  -e BIGQUERY_DATASET="<DATASET>" \
  -e BIGQUERY_TABLE_SOURCE="<SRC_TABLE>" \
  -e BIGQUERY_TABLE_TARGET="<TGT_TABLE>" \
  -e URL_PROCESSOR_SERVICE_URL="https://<your-service>.run.app" \
  video-context-crj:local
```


## Deploy as a Cloud Run Job

You can deploy with either Docker/Artifact Registry or Cloud Buildpacks. Below uses Artifact Registry and Cloud Run Jobs.

1) Create an Artifact Registry repo (if you don't already have one):

```bash
gcloud artifacts repositories create containers \
  --repository-format=docker \
  --location=us \
  --description="General Docker repo"
```

2) Configure Docker to use Artifact Registry and build/push:

```bash
gcloud auth configure-docker us-docker.pkg.dev
docker build -t us-docker.pkg.dev/<PROJECT_ID>/containers/video-context-crj:latest .
docker push us-docker.pkg.dev/<PROJECT_ID>/containers/video-context-crj:latest
```

3) Create the Cloud Run Job (replace values):

```bash
gcloud run jobs create video-context-crj \
  --image us-docker.pkg.dev/<PROJECT_ID>/containers/video-context-crj:latest \
  --region us-central1 \
  --set-env-vars BIGQUERY_PROJECT=<PROJECT_ID>,BIGQUERY_DATASET=<DATASET>,BIGQUERY_TABLE_SOURCE=<SRC_TABLE>,BIGQUERY_TABLE_TARGET=<TGT_TABLE>,URL_PROCESSOR_SERVICE_URL=https://<your-service>.run.app \
  --max-retries 3 \
  --tasks 1 \
  --task-timeout 3600s
```

4) Run the job on-demand:

```bash
gcloud run jobs run video-context-crj --region us-central1 --wait
```

5) Update the job after pushing a new image:

```bash
gcloud run jobs update video-context-crj \
  --image us-docker.pkg.dev/<PROJECT_ID>/containers/video-context-crj:latest \
  --region us-central1
```


## Your URL Processor service

The job calls your existing Cloud Run service at `URL_PROCESSOR_SERVICE_URL` with:

```http
POST / HTTP/1.1
Content-Type: application/json

{"name": "https://example.com/video1"}
```

The service should return plain text in the response body (e.g., a summary). Non-2xx responses are treated as failures. Timeouts are handled by the job.

Auth options:

- Make the service public (no auth) for simplicity, or
- Keep it authenticated and grant the Cloud Run Job’s service account permission to invoke it. If you use auth, modify `main.py` to obtain and send an identity token in the request (current sample expects an open endpoint).

Quick smoke test:

```bash
curl -X POST "$URL_PROCESSOR_SERVICE_URL" \
  -H "Content-Type: application/json" \
  -d '{"name": "https://example.com/video1"}' -i
```


## Operations and monitoring

- Logs: view via Cloud Logging for the job executions.
- Concurrency: controlled in `main.py` by `MAX_CONCURRENT_TASKS_PER_INSTANCE`.
- Idempotency: rows are marked `PROCESSING` at fetch time to minimize duplicate work.
- Failures: when the processor fails or times out, the target status is `FAILED_PROCESSING`. You can rerun the job after fixing the processor; consider adding retry logic to move failed rows back to `PENDING` if desired.


## Troubleshooting

- BigQuery auth errors: ensure Application Default Credentials are available to the job’s service account and it has `BigQuery Data Viewer` and `BigQuery Job User` (and write permissions for target table updates).
- No pending URLs found: confirm `status='PENDING'` rows exist in the source table.
- 4xx/5xx from processor: test the processor endpoint with `curl`. Ensure it accepts the payload `{"name": "<url>"}` and returns text.
- Timeouts: increase `URL_PROCESSOR_TIMEOUT_SECONDS` or ensure the processor is optimized.
- Region mismatches: keep Artifact Registry, Cloud Run Job, and service regions consistent where possible.


## Notes about `Cloud_Run_Function/`

This folder demonstrates a function setup for generating video insights. It is optional for running the job in this repo. If you already have a URL Processor service elsewhere, you can ignore or remove this folder.


## License

This project is licensed under the terms of the `LICENSE` file in this repository.

