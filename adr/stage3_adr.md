# Architecture Decision Record: Stage 3 Streaming Extension

**File:** `adr/stage3_adr.md`  
**Author:** Milton Flusk  
**Date:** 29 April 2026  
**Status:** Final

---

## Context

Stage 3 required extending the existing batch medallion pipeline with a streaming-style micro-batch ingestion path. The mobile product team needed updated account-level streaming outputs based on JSONL transaction batches delivered to `/data/stream/` while the container was running. The pipeline had to poll that directory, detect new files, process them without manual input, and write two Delta tables under `/data/output/stream_gold/`: `current_balances` and `recent_transactions`. `current_balances` needed to maintain one latest balance row per `account_id`, while `recent_transactions` needed to retain recent transaction rows and cap them to the latest 50 records per account. The stream ingestion loop also needed to terminate after a quiesce period so that the container would not run indefinitely.

Coming into Stage 3, the project already had a working Stage 2 batch pipeline with separate modules for Bronze ingestion, Silver transformation, and Gold provisioning: `pipeline/ingest.py`, `pipeline/transform.py`, and `pipeline/provision.py`. The entry point, `pipeline/run_all.py`, executed those stages sequentially inside Docker. Stage 2 already produced Delta outputs and a `dq_report.json`, and the Docker image had been tested under the scoring-style constraints. The Stage 3 work therefore focused on extending the existing flow rather than replacing it: I added `pipeline/stream_ingest.py` and updated `pipeline/run_all.py` so the streaming extension runs after the Gold layer has been provisioned.

---

## Decision 1: How did your existing Stage 1 architecture facilitate or hinder the streaming extension?

The existing Stage 1 and Stage 2 structure helped because the project was already divided into clear pipeline stages. `ingest.py`, `transform.py`, and `provision.py` each had a specific responsibility, which made it possible to add `stream_ingest.py` as a fourth stage without rewriting the batch pipeline. The existing Delta Lake pattern also helped because Stage 3 required Delta outputs, and the earlier work had already established how the project writes and reads Delta tables under `/data/output/`. The Gold `dim_accounts` table was particularly useful because the streaming balance logic could use it as the starting snapshot for each account before applying stream transaction deltas.

The main friction was that the batch and streaming concerns were not designed together from the start. `run_all.py` was originally only a batch orchestrator, so Stage 3 required adding the stream stage into the same sequential entry point. Also, schema logic was still partly embedded inside the pipeline modules, which meant the new stream schemas for `current_balances` and `recent_transactions` had to be defined directly inside `stream_ingest.py`. Another issue was the Docker runtime itself: creating empty Spark DataFrames using `spark.createDataFrame()` triggered a pandas, pyarrow, and NumPy compatibility issue in the container. I changed the implementation to create empty Delta tables using Spark SQL instead, which avoided unnecessary pandas imports and made the Stage 3 run stable.

Most of the Stage 1 and Stage 2 code survived intact. The batch modules remained largely unchanged. The main modifications were extending `run_all.py` to call `pipeline/stream_ingest.py` and adding the new stream ingestion module. This means the Stage 3 implementation was mostly an extension of the original architecture rather than a rewrite.

---

## Decision 2: What design decisions in Stage 1 would you change in hindsight?

In hindsight, I would have centralised schema definitions and path handling earlier in the project. The Stage 2 and Stage 3 work both depended heavily on exact output schemas, and Stage 3 added two new output tables with strict field counts. If I had designed a single schema registry or configuration module from Stage 1, I could have defined the Bronze, Silver, Gold, and stream Gold schemas in one place and reused them across modules. Instead, the Stage 3 stream schemas had to be implemented directly in `stream_ingest.py`, which works but is less clean and increases the risk of schema drift.

I would also have designed the entry point with future pipeline modes in mind. `run_all.py` currently runs the stages sequentially, which is simple and reliable for the scorer, but it mixes batch and streaming orchestration in one linear flow. A better structure would have been to keep `run_all.py` as the required scoring entry point while moving orchestration into reusable functions such as `run_batch_pipeline()` and `run_stream_pipeline()`. That would make the pipeline easier to test because the stream stage could be tested independently while still allowing the full scorer command to run everything end-to-end.

A final change would be to avoid any code path that can trigger pandas for Spark operations. The Docker image exposed a pandas, pyarrow, and NumPy compatibility problem when empty Spark DataFrames were created using `createDataFrame()`. If I had known this earlier, I would have used Spark SQL or native Spark readers and writers consistently, even for empty tables. This would have reduced debugging time and made the implementation more robust under the challenge’s constrained Docker environment.

---

## Decision 3: How would you approach this differently if you had known Stage 3 was coming from the start?

If I had known Stage 3 was coming from the beginning, I would have designed the pipeline around shared batch and micro-batch interfaces. Instead of treating streaming as a late extension, I would have created a common ingestion pattern where each source has a reader, validator, and writer. For example, the batch input reader would read `/data/input/accounts.csv`, `/data/input/customers.csv`, and `/data/input/transactions.jsonl`, while the stream input reader would poll `/data/stream/` and read `stream_*.jsonl` files in filename order. Both paths would share Spark session creation, Delta writing utilities, schema definitions, and common validation helpers.

For state management, I would still use Delta tables rather than an external state store because the scoring environment expects Delta Parquet outputs and does not allow network services. However, I would design the stream state explicitly from Day 1. `current_balances` would be treated as a state table keyed by `account_id`, and `recent_transactions` would be treated as an account-windowed state table keyed by `(account_id, transaction_id)`. I would also track processed stream file names in a small checkpoint file under `/tmp` or a Delta metadata table so that reprocessing behaviour is clearer and easier to audit.

I would also structure the output layer more deliberately. Instead of thinking of Gold as only the batch reporting layer, I would define two output areas from the start: `gold/` for batch analytical tables and `stream_gold/` for low-latency account state tables. This separation is useful because the stream tables serve a different purpose from the batch fact and dimension tables. Finally, I would make testing part of the design earlier by including a small local stream test fixture and a Docker command that runs the full Bronze, Silver, Gold, and Stream pipeline under the same memory, CPU, and network constraints used by the scorer.