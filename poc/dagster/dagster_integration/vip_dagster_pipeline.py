import os, logging, datetime as dt, smtplib, pandas as pd
import pytz, datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dagster import (
    asset,
    AssetIn,
    StaticPartitionsDefinition,
    Definitions,
    Field,
    graph,
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

@asset(partitions_def=partitions_def, key_prefix=["dot_data"])
def raw_data(context) -> pd.DataFrame:
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
    return df, df_sub if state in ["ny", "la"] else None

@asset(
    partitions_def=partitions_def,
    ins={"raw_data": AssetIn()},
    key_prefix=["dot_data"],
)
def combined_data(context, raw_data) -> pd.DataFrame:
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
    else:
        df_combined = globals()[f"transform_and_load_{state}"](raw_data)
    preview_md = df_combined.head(25).to_markdown()
    context.add_output_metadata({
            "preview": MetadataValue.md(preview_md),
            "row_count": len(df_combined),
        }
    )
    return df_combined, df_combined_sub if state in ["ny", "la"] else None

@asset(
    partitions_def=partitions_def,
    ins={"combined_data": AssetIn()},
    key_prefix=["dot_data"],
)
def appended_data(context, combined_data) -> pd.DataFrame:
    state = context.partition_key
    if state not in STATES:
        raise NotImplementedError
    elif state == "la":
        if combined_data_sub is None:
            raise ValueError("combined_data_sub must be provided for LA state")
        combined_data, combined_data_sub = combined_data
        appended_data, appended_data_sub = globals()[f"data_appended_{state}"](combined_data, combined_data_sub)
        preview_md_sub = appended_data_sub.head(25).to_markdown()
        context.add_output_metadata({
            "preview": MetadataValue.md(preview_md_sub),
            "row_count": len(appended_data_sub),
        })
    else:
        appended_data = globals()[f"data_appended_{state}"](combined_data)
    # Add metadata for preview
    preview_md = appended_data.to_markdown()
    context.add_output_metadata({
            "preview": MetadataValue.md(preview_md),
            "row_count": len(appended_data),
        }
    )
    return appended_data, appended_data_sub if state == "la" else None

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
    return path, sub_path if state == "la" else None

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
schedules.append(
    build_schedule_from_partitioned_job(
        name="dot_assets_schedule",
        job=asset_job,
        cron_schedule="0 08 22,L * *",
        execution_timezone="US/Eastern",
    )
)

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
                        "body":    f"{state} pipeline finished {dt.datetime.now():%Y-%m-%d %H:%M}",
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

# ───────── Definitions ─────────
defs = Definitions(
    assets=[raw_data, combined_data, appended_data],
    jobs=jobs,
    schedules=schedules,
    sensors=SENSORS,
    resources={"io_manager": fs_io_manager},
)


