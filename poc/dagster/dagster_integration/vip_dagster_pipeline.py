import os, logging, smtplib, pandas as pd
import pathlib
import pytz
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dagster import (
    asset,
    AssetIn,
    StaticPartitionsDefinition,
    Definitions,
    Field,
    graph,
    graph_asset,
    op,
    fs_io_manager,
    in_process_executor,
    define_asset_job,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
    sensor,
    RunRequest,
    SkipReason,
    AssetKey,
    MetadataValue
)
from dagster._core.events import DagsterEventType
from dagster._core.storage.event_log.base import EventRecordsFilter
from dagster import RetryPolicy 
from dagster_pandas import DataFrame
from dotenv import load_dotenv
from scraping_pipelines.wv_scraping_pipeline import scrape_raw_wv, transform_and_load_wv, data_appended_wv
from scraping_pipelines.ok_scraping_pipeline import scrape_raw_ok, transform_and_load_ok, data_appended_ok
from scraping_pipelines.wa_scraping_pipeline import scrape_raw_wa, transform_and_load_wa, data_appended_wa
from scraping_pipelines.ga_scraping_pipeline import scrape_raw_ga, transform_and_load_ga, data_appended_ga
from scraping_pipelines.de_scraping_pipeline import scrape_raw_de, transform_and_load_de, data_appended_de
from scraping_pipelines.il_scraping_pipeline import scrape_raw_il, transform_and_load_il, data_appended_il
from scraping_pipelines.la_scraping_pipeline import scrape_raw_la, transform_and_load_la, data_appended_la
from scraping_pipelines.ny_scraping_pipeline import scrape_raw_ny, transform_and_load_ny, data_appended_ny
from scraping_pipelines.Iowa_dot_api_access import fetch_raw_ia, transform_ia_data
from scraping_pipelines.Wisconsin_dot_api_access import fetch_raw_ws, transform_ws_data
from scraping_pipelines.Nebraska_dot_api_access import fetch_raw_ne, transform_ne_data
from scraping_pipelines.Minnessota_dot_api_access import fetch_raw_mn, transform_mn_data
from mft_utils import MFTClient
from get_duckdb_data import export_duckdb_to_csv

load_dotenv()

EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECIPIENT = os.getenv("EMAIL_RECIPIENT")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.office365.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))

# ─────────────────── Asset layer (partitioned) ─────────────
STATES = ["wv", "ok", "ga", "wa", "de", "il","la","ny"]  # List of states to automate
partitions_def = StaticPartitionsDefinition(STATES)
RETRY_POLICY = RetryPolicy(
    max_retries=2,  # Retry up to 2 times
    delay=30,  # Wait 30 seconds between retries
)

@asset(partitions_def=partitions_def, key_prefix=["dot_data"],retry_policy=RETRY_POLICY)
def raw_data(context):
    state = context.partition_key
    if state not in STATES:
        raise NotImplementedError(f"Scraper for {state} not implemented")
    elif state == "ny" or state == "la":
        df, df_sub = globals()[f"scrape_raw_{state}"]()
        # Add metadata for preview
        preview_md_sub = df_sub.head(25).to_markdown()
        context.add_output_metadata(
            {
                "preview":  MetadataValue.md(preview_md_sub),   # main line for rendering preview
                "row_count": len(df_sub),                      # optional extras
            }
        )
    else:
        df = globals()[f"scrape_raw_{state}"]() # Dynamically calls functions according to state passed.
    # Add metadata for preview
    preview_md = df.head(25).to_markdown()
    context.add_output_metadata(
        {
            "preview":  MetadataValue.md(preview_md),   # main line for rendering preview
            "row_count": len(df),                      # optional extras
        }
    )
    if state in {"ny", "la"}:
        return (df, df_sub)     # tuple only for the two special cases
    else:
        return df               # DataFrame everywhere else

@asset(
    partitions_def=partitions_def,
    ins={"raw_data": AssetIn()},
    key_prefix=["dot_data"],
)
def combined_data(context, raw_data):
    state = context.partition_key
    if state not in STATES:
        raise NotImplementedError(f"Scraper for {state} not implemented")
    elif state in {"ny", "la"}:
        df_main, df_sub = raw_data
        if df_sub is None:
            raise ValueError("raw_data_sub must be provided for NY state")
        df_combined, df_combined_sub = globals()[f"transform_and_load_{state}"](df_main, df_sub)
        preview_md_sub = df_combined_sub.head(25).to_markdown()
        context.add_output_metadata({
            "preview": MetadataValue.md(preview_md_sub),
            "row_count": len(df_combined_sub),
        })
        if state == "ny" and df_sub is not None:
            # Save the NY amendment data to a CSV file right in this step since it is a special case witout duckdb table
            # Set the path for saving the NY amendment data
            ny_amendment_path = os.getenv("NY_AMENDMENT_PATH")
            EST = pytz.timezone('US/Eastern')
            now = datetime.now(EST)
            # Get current year and month of scraping pulls for writing files appropriately
            current_year = now.year
            current_month = now.strftime('%b') # e.g., 'Jan', 'Feb', etc.
            df_combined_sub.to_csv(rf"{ny_amendment_path}\ny_amendment_bulk_pipeline_vip_{current_month}_{current_year}.csv", index=False)
            ny_amendment_path_full = rf"{ny_amendment_path}\ny_amendment_bulk_pipeline_vip_{current_month}_{current_year}.csv"
            MFTClient.mft_file(ny_amendment_path_full, f"ny_amendment_bulk_pipeline_vip_{current_month}_{current_year}.csv")
    else:
        df_combined = globals()[f"transform_and_load_{state}"](raw_data)
    preview_md = df_combined.head(25).to_markdown()
    context.add_output_metadata({
            "preview": MetadataValue.md(preview_md),
            "row_count": len(df_combined),
        }
    )
    if state in {"ny","la"}:
        return df_combined, df_combined_sub
    else:
        return df_combined

@asset(
    partitions_def=partitions_def,
    ins={"combined_data": AssetIn()},
    key_prefix=["dot_data"],
)
def appended_data(context, combined_data):
    state = context.partition_key
    if state not in STATES:
        raise NotImplementedError
    elif state == "la":
        combined_data, combined_data_sub = combined_data
        if combined_data_sub is None:
            raise ValueError("combined_data_sub must be provided for LA state")
        appended_data, appended_data_sub = globals()[f"data_appended_{state}"](combined_data, combined_data_sub)
        preview_md_sub = appended_data_sub.head(25).to_markdown()
        context.add_output_metadata({
            "preview": MetadataValue.md(preview_md_sub),
            "row_count": len(appended_data_sub),
        })
    elif state == "ny":
        appended_data = globals()[f"data_appended_{state}"](combined_data[0])
    else:
        appended_data = globals()[f"data_appended_{state}"](combined_data)
    # Add metadata for preview
    preview_md = appended_data.to_markdown()
    context.add_output_metadata({
            "preview": MetadataValue.md(preview_md),
            "row_count": len(appended_data),
        }
    )
    if state == "la":
        return appended_data, appended_data_sub
    else:
        return appended_data

# ────────────── export + email ops  ─────────────
@op(config_schema={"state": Field(str)})
def export_csv(context) -> str:
    state = context.op_config["state"]
    state_upper   = state.upper() 
    folder = os.getenv(f"{state_upper}_FOLDER")
    duckdb_file = os.getenv(f"{state_upper}_DUCKDB_FILE")
    table_name  = os.getenv(f"{state_upper}_TABLE_NAME")
    path, file_name = export_duckdb_to_csv(folder, duckdb_file, table_name, f"{state}_bulk_pipeline_vip")
    MFTClient.mft_file(path, file_name)
    if state == "la":
        # For LA, we need to export the sub table as well
        sub_table_name = f"{table_name}_sub"
        sub_path, sub_file_name = export_duckdb_to_csv(folder, duckdb_file, sub_table_name, f"{state}_sub_bulk_pipeline_vip")
        MFTClient.mft_file(sub_path, sub_file_name)
    return path

@op(config_schema={"subject": Field(str), "body": Field(str)})
def notify(context, exported_path: str):
    cfg = context.op_config
    msg = MIMEMultipart()
    msg["From"], msg["To"], msg["Subject"] = EMAIL_SENDER, EMAIL_RECIPIENT, cfg["subject"]
    msg.attach(MIMEText(cfg["body"] + f"\nFile: {exported_path}", "plain"))
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
        s.starttls(); s.login(EMAIL_SENDER, EMAIL_PASSWORD); s.send_message(msg)
    logging.info("Email sent")

# ───────── graph: export → email ─────────
@graph
def post_process_graph():
    fp  = export_csv()
    notify(fp)

# ───────── jobs & schedules ─────────
jobs, schedules = [], []

# a single job to materialize both assets for every state
asset_job = define_asset_job(
    name="dot_assets_job",
    selection=[raw_data.key, combined_data.key, appended_data.key],
    partitions_def=partitions_def,
    executor_def=in_process_executor,
)
jobs.append(asset_job)
# schedules.append(
#     build_schedule_from_partitioned_job(
#         name="dot_assets_schedule",
#         job=asset_job,
#         cron_schedule="0 08 22,L * *",
#         execution_timezone="US/Eastern",
#     )
# ) # old common schedule for all states

# a job for each state, with its own schedule
# this is the preferred way to do it, as it allows for staggered execution
# helper → asset‑job for exactly ONE partition
# helper → schedule that always runs ONE partition
def make_state_schedule(state: str, cron: str):
    # will be invoked by Dagster when the cron fires
    def _execution_fn(context, key=state):
        # ask the job to build its RunRequest for that key
        return [asset_job.run_request_for_partition(context, partition_key=key)]
        # (asset_job is your all‑states job defined above)

    return ScheduleDefinition(
        name=f"{state}_assets_schedule",
        cron_schedule=cron,
        job=asset_job,                   # the job with all 8 partitions
        execution_timezone="US/Eastern",
        execution_fn=_execution_fn,     
    )

# staggered cron strings
CRONS = {
    "wv": "0 8 25 * *",
    "ok": "30 8 25 * *",
    "ga": "0 9 25 * *",
    "wa": "30 9 25 * *",
    "de": "0 10 25 * *",
    "il": "30 10 25 * *",
    "la": "0 5 25 * *",
    "ny": "30 5 25 * *",
}

# create schedules
for st in STATES:
    schedules.append(make_state_schedule(st, CRONS[st]))

POST_JOBS = {}
# per-state post-process jobs
for state in STATES:
    POST_JOBS[state] = post_process_graph.to_job(
        name=f"{state.lower()}_post_process",
        config={
            "ops": {
                "export_csv":      {"config": {"state": state}},
                "notify": {
                    "config": {
                        "subject": f"{state} DOT Pipeline Success",
                        "body":    f"{state} pipeline finished {datetime.now():%Y-%m-%d %H:%M}",
                    }
                },
            }
        },
        executor_def=in_process_executor,
    )

SENSORS = []

for state in STATES:
    job        = POST_JOBS[state]
    asset_key  = AssetKey(["dot_data", "appended_data"])

    @sensor(name=f"{state.lower()}_combined_sensor", job=job)
    def _sensor(context, state=state, asset_key=asset_key):
        raw_cursor = context.cursor
        last_id = int(raw_cursor) if raw_cursor else None
        filter_ = EventRecordsFilter(
        asset_key=asset_key,
        asset_partitions=[state],
        event_type=DagsterEventType.ASSET_MATERIALIZATION,
        after_cursor=last_id,
        )
        events = list(
            context.instance.get_event_records(
                limit=1,
                event_records_filter = filter_,
                ascending = False,
            )
        )
        if not events:
            yield SkipReason("no new materialization")
            return

        mat_id = events[0].storage_id
        context.update_cursor(str(mat_id)) 
        yield RunRequest(
            run_key=f"{state}-{mat_id}",
            tags = {"partition": state},
        )

    SENSORS.append(_sensor)  
# ─────────────────── API pipelines (separate job + schedules) ───────────────────

# Define which API partitions you want; keep it explicit (or load from env if you prefer)
API_SOURCES = ['ia','ws', 'ne', 'mn']
api_partitions_def = StaticPartitionsDefinition(API_SOURCES)

# (Optional) retry policy – reuse your existing one
API_RETRY_POLICY = RETRY_POLICY

@asset(partitions_def=api_partitions_def, key_prefix=["api_data"], retry_policy=API_RETRY_POLICY)
def api_raw_data(context):
    """Fetch raw data from an API for this partition (source)."""
    src = context.partition_key
    fn_name = f"fetch_raw_{src}"
    fetch_fn = globals().get(fn_name)
    if not fetch_fn:
        raise NotImplementedError(f"Missing API fetcher: {fn_name}()")
    df = fetch_fn()

    # Minimal preview
    if hasattr(df, "head"):
        context.add_output_metadata({
            "preview": MetadataValue.md(df.head(25).to_markdown()),
            "row_count": len(df),
        })
    return df

@asset(
    partitions_def=api_partitions_def,
    ins={"api_raw_data": AssetIn()},
    key_prefix=["api_data"],
    retry_policy=API_RETRY_POLICY,
)
def api_transformed_data(context, api_raw_data):
    """Transform + load API raw into storage (DuckDB/warehouse) and return the combined frame."""
    src = context.partition_key
    fn_name = f"transform_{src}_data"
    transform_fn = globals().get(fn_name)
    if not transform_fn:
        raise NotImplementedError(f"Missing API transformer: {fn_name}()")

    df_main, df_sub = transform_fn(api_raw_data)

    if hasattr(df_main, "head"):
        context.add_output_metadata({
            "preview": MetadataValue.md(df_main.head(25).to_markdown()),
            "row_count": len(df_main),
        })
    return df_main, df_sub

# Job for the API assets (completely separate from your web-scraping job)
api_jobs = []

# ─────────────────── API export + notify ops ───────────────────
@op
def _current_partition_key(context) -> str:
    pk = context.partition_key
    if not pk:
        raise ValueError("No partition key set for API asset run")
    return pk

@op
def export_api_csv(context, api_transformed_data, source: str) -> str:
    """
    Export API tables to CSV and push via MFT.
    """

    src   = context.partition_key or (context.op_config or {}).get("source")
    if not src:
        raise ValueError("Source partition key must be provided for API export")
    upper = source.upper()

    # Resolve output directory / format / basename
    outdir   = os.getenv(f"{upper}_API_BASE_PATH")
    basename = os.getenv(f"{upper}_API_BASENAME")
    basename_sub = os.getenv(f"{upper}_API_BASENAME_SUB")

    if not outdir or not basename:
        raise ValueError(f"Output directory missing for {src}. Set {upper}_API_BASE_PATH")
    pathlib.Path(outdir).mkdir(parents=True, exist_ok=True)  # Ensure output directory exists

    # Get current year and month of api pulls for writing files appropriately
    EST = pytz.timezone('US/Eastern')
    now = datetime.now(EST)
    current_year = now.year
    current_month = now.strftime('%b')
    fname   = f"{basename}_{current_year}_{current_month}.{'csv'}"
    fpath   = os.path.join(outdir, fname)
    fname_sub = f"{basename_sub}_{current_year}_{current_month}.{'csv'}" if basename_sub else None
    fpath_sub = os.path.join(outdir, fname_sub) if fname_sub else None
    df_out, df_out_sub = api_transformed_data

    df_out.to_csv(fpath, index=False)
    MFTClient.mft_file(fpath, fname)
    if df_out_sub is not None and fname_sub:   
        df_out_sub.to_csv(fpath_sub, index=False)
        MFTClient.mft_file(fpath_sub, fname_sub)

    return fpath  # notify receives this


@op
def notify_api(context, exported_path: str, source:str) -> str:
    subject = f"{source.upper()} API Pipeline Success"
    body    = f"{source} API pipeline finished {datetime.now():%Y-%m-%d %H:%M}"
    cfg = context.op_config
    msg = MIMEMultipart()
    msg["From"], msg["To"], msg["Subject"] = EMAIL_SENDER, EMAIL_RECIPIENT, subject
    msg.attach(MIMEText(cfg["body"] + f"\nFile: {exported_path}", "plain"))
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
        s.starttls(); s.login(EMAIL_SENDER, EMAIL_PASSWORD); s.send_message(msg)
    logging.info("[API] Email sent")
    return exported_path  # pass through for next op

# @op
# def _passthrough_path(exported_path: str) -> str:
#     return exported_path

@graph
def api_post_process_graph(api_transformed_data):
    src = _current_partition_key()
    p   = export_api_csv(api_transformed_data=api_transformed_data, source=src)
    out = notify_api(exported_path=p, source=src)  # <-- chained; not a leaf anymore
    return out

api_exported_file = graph_asset(
    name="api_exported_file",
    key_prefix=["api_data"],
    partitions_def=api_partitions_def,
    ins={"api_transformed_data": AssetIn(key=api_transformed_data.key)},
)(api_post_process_graph)

api_asset_job = define_asset_job(
    name="api_assets_job",
    selection=[api_raw_data.key, api_transformed_data.key, api_exported_file.key],
    partitions_def=api_partitions_def,
    executor_def=in_process_executor,
)
api_jobs.append(api_asset_job)

api_schedules = []

# Per-partition schedules (independent cron per API source)
# Fill these with your desired timings
API_CRONS = {
    "ia": "0 8 25 * *",
    "ws": "30 8 25 * *",
    "ne": "0 9 25 * *",
    "mn": "30 9 25 * *"
}

def make_partition_schedule_for_job(job, partition_key, cron, tz="US/Eastern", name_prefix="api"):
    def _execution_fn(context, key=partition_key):
        # Build exactly one RunRequest for this partition when the cron fires
        return [job.run_request_for_partition(context, partition_key=key)]
    return ScheduleDefinition(
        name=f"{name_prefix}_{partition_key}_schedule",
        cron_schedule=cron,
        job=job,
        execution_timezone=tz,
        execution_fn=_execution_fn,
    )
# Create schedules only for API sources that have a cron defined
for src in API_SOURCES:
    cron = API_CRONS.get(src)
    if not cron:
        logging.warning("[API] No cron provided for %s; skipping schedule.", src)
        continue
    api_schedules.append(make_partition_schedule_for_job(api_asset_job, src, cron))

# ───────── Definitions ─────────
defs = Definitions(
    assets=[raw_data, combined_data, appended_data, api_raw_data, api_transformed_data, api_exported_file],
    jobs=jobs+api_jobs,
    schedules=schedules+api_schedules,
    sensors= SENSORS,
    resources={"io_manager": fs_io_manager},
)


