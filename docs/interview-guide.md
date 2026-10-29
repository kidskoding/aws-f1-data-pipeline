# Interview Guide

How to talk about this project in a data engineering, analytics engineering, or data platform interview.

## The 60-second version

"I built a batch pipeline on AWS that pulls Formula 1 results from a public API. A Lambda function lands the raw JSON in S3 unchanged. A Glue Spark job flattens it into a star schema, rejects bad rows with a reason, and runs Glue Data Quality rules before publishing Parquet. A second Lambda loads Redshift Serverless in a single transaction, then Step Functions counts the same table in the lake with Athena and fails the run if the numbers don't match. It's all Terraform, tested in GitHub Actions, and a full backfill costs a few cents."

## Points worth making

- **Raw is immutable, so everything is replayable.** If a cleaning rule is wrong, fix the code and rerun from `raw/`.
- **Reconciliation at three levels.** Rows (raw = clean + rejected), systems (lake count = warehouse count), and business numbers (our points = official standings, for every driver in every season).
- **Idempotent steps.** Every step can be rerun safely: replace a season, overwrite curated data, reload in one transaction.
- **The load is atomic.** `DELETE` plus `COPY` for every table runs as one Data API batch, which is one transaction. Readers see old data or new data, never half of each.
- **Cost guards are part of the design.** Minimum Glue and Redshift sizes, a Redshift daily cap that turns the warehouse off, an Athena scan limit, a job timeout, and a schedule that's off by default.
- **Least privilege.** Each component has its own IAM role, scoped to the prefixes and resources it touches.

## Likely questions

**Why Glue for the transform and Lambda for the extract?**
The extract is I/O bound and small: Lambda is cheap and starts instantly. The transform is a distributed join and reshape that grows with data: Spark on Glue scales by adding workers.

**Why both Athena and Redshift?**
Athena is pay-per-scan SQL on the lake, good for ad hoc questions and for checking the lake independently. Redshift is the warehouse for repeated, joined, BI-style queries, where loaded data with sort and distribution keys is faster.

**COPY vs Spectrum?**
COPY loads data into Redshift storage: fastest queries, best for data that's queried often. Spectrum queries Parquet in S3 in place: no load step, best for large or rarely used data. This project uses both (see `05_lake_vs_warehouse.sql`).

**Why Parquet?**
Columnar, compressed and typed. Queries read only the columns they need, and types survive between tools.

**How do you handle schema changes from the API?**
The Glue job reads with an explicit schema, so new fields are ignored and missing ones become nulls that the Data Quality rules catch. The raw crawler shows what changed in the raw data.

**How would this scale to 1,000 times the data?**
Partition curated tables by season, process only new raw partitions (Glue job bookmarks), stop coalescing to one file, add Glue workers, and load Redshift incrementally with a staging table and `MERGE` instead of a full replace.

**What does the distribution and sort key choice do?**
`results` is distributed by `driver_id` so a driver's rows sit together, and sorted by `(season, round)` so date-range filters skip blocks. Small dimensions use `DISTSTYLE ALL`, a copy on every node, so joins never move data.

**How do you deploy changes safely?**
CI runs lint, type checks, unit tests, Spark tests and `terraform validate`. `terraform plan` shows the exact AWS changes before `apply`. For a team: remote state in S3 with locking, and separate dev and prod workspaces.

**What happens when a run fails halfway?**
Step Functions catches the error and emails it. Each step is idempotent, so you rerun the whole pipeline. Because the warehouse load is one transaction, a failed load leaves the previous data intact.

## Same pipeline, other stacks

| Job | This project (AWS) | Databricks | Snowflake | On-prem (Teradata era) |
|---|---|---|---|---|
| Ingest from API | Lambda | Jobs (Python task) | Snowpark, external functions | Custom scripts |
| Data lake storage | S3 | S3 / ADLS + Delta Lake | Stages | SAN, file shares |
| ETL engine | Glue (Spark) | Spark, Lakeflow pipelines | Snowpark, tasks, dynamic tables | Ab Initio, SAP BODS, Informatica |
| Data quality | Glue Data Quality | Lakeflow expectations | Data metric functions | Hand-written checks |
| Catalog | Glue Data Catalog | Unity Catalog | Built in | Tool-specific metadata |
| Warehouse | Redshift Serverless | Databricks SQL warehouse | Virtual warehouse | Teradata |
| Bulk load | `COPY` | `COPY INTO`, Auto Loader | `COPY INTO`, Snowpipe | FastLoad, MultiLoad |
| Ad hoc lake SQL | Athena | Databricks SQL | External tables | n/a |
| Orchestration | Step Functions | Workflows | Tasks | Control-M, cron |
| Reporting | (QuickSight) | AI/BI dashboards | (Tableau, Sigma) | Business Objects |

## For teams migrating legacy finance data to AWS

Teams moving from Teradata and Ab Initio to S3, Glue and Redshift face the problems this project practices on a small scale:

- **Ab Initio graphs to Glue jobs:** business rules hidden in old ETL become explicit, tested Spark code (`clean_results` and its tests).
- **Teradata to Redshift:** both are parallel SQL warehouses. The SQL mostly carries over, but distribution and sort keys replace Teradata's primary index design.
- **Proving the numbers match:** finance migrations run old and new side by side until totals tie out. The points reconciliation here is the same idea: compute a total two ways and require zero differences.
- **Auditability:** immutable raw data, rejects with reasons, and per-run Data Quality results are the evidence auditors ask for.
