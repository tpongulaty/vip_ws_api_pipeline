# import os
# import logging
# import smtplib
# from email.mime.text import MIMEText
# from email.mime.multipart import MIMEMultipart
# import pandas as pd
# from dagster import (
#     asset,
#     StaticPartitionsDefinition,
#     AssetIn,
#     graph,
#     op,
#     Definitions,
#     PartitionScheduleDefinition,
#     Field,
#     Out,
#     in_process_executor,
#     mem_io_manager,
# )
# from get_duckdb_data import export_duckdb_to_csv
# from mft_utils import MFTClient
# from wv_scraping_pipeline import scrape_raw_wv, transform_and_load_wv # replace with generic dispatch if needed
# from dagster_pandas import DataFrame
# from dotenv import load_dotenv

# # Load environment variables
# load_dotenv()

# EMAIL_SENDER    = os.getenv("EMAIL_SENDER")
# EMAIL_PASSWORD  = os.getenv("EMAIL_PASSWORD")
# EMAIL_RECIPIENT = os.getenv("EMAIL_RECIPIENT")
# SMTP_SERVER     = os.getenv("SMTP_SERVER", "smtp.office365.com")
# SMTP_PORT       = int(os.getenv("SMTP_PORT", 587))

# # List of states to automate
# STATES = ["WV", "CA", "TX", "NY"]

# @op(out=Out(dagster_type=DataFrame),config_schema={"state": Field(str, description="State code for scraping")})
# def scrape_data(context) -> pd.DataFrame:
#     state = context.op_config["state"]
#     logging.info(f"Starting scraping for {state}")
#     if state == "WV":
#         raw_df = scrape_raw_wv()
#         logging.info(f"Completed scraping for {state}")
#         return raw_df
#     else:
#         raise ValueError(f"No scraper implemented for state '{state}'")
    
# @op(out=Out(dagster_type=DataFrame),config_schema={"state": Field(str)})
# def transform_data(context, raw_df) -> pd.DataFrame:
#     state = context.op_config["state"]
#     if state == "WV":
#         combined_df = transform_and_load_wv(raw_df)
#         return combined_df
#     else:
#         raise NotImplementedError(f"Transform not implemented for {state}")

# @op(config_schema={"state": Field(str, description="State code for export and transfer")})
# def export_and_transfer_csv(context,combined_df):
#     state = context.op_config["state"]
#     logging.info(f"Exporting CSV for {state}")
#     folder      = os.getenv(f"{state}_FOLDER")
#     duckdb_file = os.getenv(f"{state}_DUCKDB_FILE")
#     table_name  = os.getenv(f"{state}_TABLE_NAME")
#     file_path, file_name = export_duckdb_to_csv(folder, duckdb_file, table_name, f"{state}_bulk_pipeline")
#     logging.info(f"CSV Exported to {file_path}")
#     # mft_client = MFTClient.from_env()  # assume MFTClient can load creds from env
#     # mft_client.mft_file(file_path, file_name, des_folder=f"{state}_analyzed")
#     # logging.info(f"MFT transfer completed for {state}")
#     return file_path

# @op(config_schema={
#     "subject": Field(str, description="Email subject"),
#     "body":    Field(str, description="Email body")
# })
# def send_status_email(context, exported_path):
#     cfg = context.op_config
#     msg = MIMEMultipart()
#     msg['From']    = EMAIL_SENDER
#     msg['To']      = EMAIL_RECIPIENT
#     msg['Subject'] = cfg['subject']
#     msg.attach(MIMEText(cfg['body'], 'plain'))

#     with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
#         server.starttls()
#         server.login(EMAIL_SENDER, EMAIL_PASSWORD)
#         server.send_message(msg)
#     logging.info("Email notification sent.")

# @graph
# def state_pipeline():
#     # wire up ops; literal values via config at job creation
#     raw = scrape_data()
#     combined = transform_data(raw_df = raw)
#     file_path = export_and_transfer_csv(combined_df = combined)
#     send_status_email(exported_path = file_path)

# # Dynamically create a job + schedule for each state
# jobs = []
# # schedules = []
# for state in STATES:
#     job = state_pipeline.to_job(
#         name=f"{state.lower()}_pipeline",
#         config={
#             "ops": {
#                 "scrape_data": {
#                     "config": {"state": state}
#                 },
#                 "transform_data": {
#                     "config": {"state": state}
#                 },
#                 "export_and_transfer_csv": {
#                     "config": {"state": state}
#                 },
#                 "send_status_email": {
#                     "config": {
#                         "subject": f"{state} DOT Scraping Success",
#                         "body":    f"{state} pipeline completed successfully."
#                     }
#                 }
#             }
#         },
#         executor_def=in_process_executor  # run in-process so mem_io_manager works
#     )
#     jobs.append(job)
#     # schedules.append(
#     #     ScheduleDefinition(
#     #         job=job,
#     #         cron_schedule="0 8 22,L * *",
#     #         execution_timezone="US/Eastern"
#     #     )
#     # )

# # if __name__ == "__main__":
# # Register all jobs and schedules in Dagster
# defs = Definitions(
#     jobs=jobs,
#     # schedules=schedules,
#     resources={
#         "io_manager": mem_io_manager,
#     }
    
# )


import os, logging, datetime as dt, smtplib, pandas as pd
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
from wv_scraping_pipeline import scrape_raw_wv, transform_and_load_wv, data_appended_wv
from get_duckdb_data import export_duckdb_to_csv

load_dotenv()

EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECIPIENT = os.getenv("EMAIL_RECIPIENT")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.office365.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))

# ─────────────────── Asset layer (partitioned) ─────────────
STATES = ["WV", "CA", "TX", "NY"]
partitions_def = StaticPartitionsDefinition(STATES)

@asset(partitions_def=partitions_def, key_prefix=["dot_data"])
def raw_data(context) -> pd.DataFrame:
    state = context.partition_key
    if state != "WV":
        raise NotImplementedError(f"Scraper for {state} not implemented")
    df = scrape_raw_wv()
    preview_md = df.head(25).to_markdown()
    context.add_output_metadata(
        {
            "preview":  MetadataValue.md(preview_md),   # main line for rendering preview
            "row_count": len(df),                      # optional extras
        }
    )
    return df

@asset(
    partitions_def=partitions_def,
    ins={"raw_data": AssetIn()},
    key_prefix=["dot_data"],
)
def combined_data(context, raw_data) -> pd.DataFrame:
    state = context.partition_key
    if state != "WV":
        raise NotImplementedError
    df_combined = transform_and_load_wv(raw_data)
    preview_md = df_combined.head(25).to_markdown()
    context.add_output_metadata({
            "preview": MetadataValue.md(preview_md),
            "row_count": len(df_combined),
        }
    )
    return df_combined

@asset(
    partitions_def=partitions_def,
    ins={"combined_data": AssetIn()},
    key_prefix=["dot_data"],
)
def appended_data(context, combined_data) -> pd.DataFrame:
    state = context.partition_key
    if state != "WV":
        raise NotImplementedError
    appended_data = data_appended_wv(combined_data)
    preview_md = appended_data.to_markdown()
    context.add_output_metadata({
            "preview": MetadataValue.md(preview_md),
            "row_count": len(appended_data),
        }
    )
    return appended_data

# ────────────── export + email ops  ─────────────
@op(config_schema={"state": Field(str)})
def export_csv(context) -> str:
    state = context.op_config["state"]
    folder = os.getenv(f"{state}_FOLDER")
    duckdb_file = os.getenv(f"{state}_DUCKDB_FILE")
    table_name  = os.getenv(f"{state}_TABLE_NAME")
    path, _ = export_duckdb_to_csv(folder, duckdb_file, table_name, f"{state}_bulk_pipeline")
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

# ───────── graph: load asset → export → email ─────────
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
#     PartitionScheduleDefinition(
#         name="dot_assets_schedule",
#         partitioned_job=asset_job,
#         cron_schedule="0 08 22,L * *",
#         execution_timezone="US/Eastern",
#     )
# )

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
    # schedules=schedules,
    sensors=SENSORS,
    resources={"io_manager": fs_io_manager},
)


