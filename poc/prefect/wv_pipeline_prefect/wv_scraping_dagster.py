# import logging
# from get_duckdb_data import export_duckdb_to_csv
# from mft_utils import MFTClient
# import os
# import dagster as dg
# import smtplib
# from email.mime.text import MIMEText
# from email.mime.multipart import MIMEMultipart

# # Import existing west_virginia_scraping() logic and isolate to an assest
# from wv_scraping_pipeline import west_virginia_scraping 

# # Fetch Credentials from .env for sending status emails
# EMAIL_SENDER = os.getenv("EMAIL_SENDER")
# EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
# EMAIL_RECIPIENT = os.getenv("EMAIL_RECIPIENT")
# SMTP_SERVER = "smtp.office365.com"
# SMTP_PORT = 587

# @dg.asset()
# def run_scraping_pipeline_wv():
#     try:
#         west_virginia_scraping()
#     except Exception as e:
#         logging.error(f"Scraping failed: {e}")
#         raise

# @dg.asset(deps=run_scraping_pipeline_wv)
# def export_and_transfer_csv_wv():
#     try:
#         file_path, file_name = export_duckdb_to_csv(os.getenv("WV_FOLDER"), os.getenv("WV_DUCKDB_FILE"), os.getenv("WV_TABLE_NAME"),"wv_bulk_pipeline")
#         logging.info(f"CSV Exported to {file_path}")
#         # MFTClient.mft_file(file_path, file_name, des_folder="rgc_analyzed")
#         # logging.info("MFT file transferred successfully.")
#     except Exception as e:
#         logging.error(f"MFT/CSV Export failed: {e}")
#         raise

# @dg.asset(deps=export_and_transfer_csv_wv)
# def send_status_email_wv():
#     try:
#         subject="WV DOT Scraping Success",
#         body="Pipeline completed successfully."
#         msg = MIMEMultipart()
#         msg['From'] = EMAIL_SENDER
#         msg['To'] = EMAIL_RECIPIENT
#         msg['Subject'] = subject
#         msg.attach(MIMEText(body, 'plain'))

#         with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
#             server.starttls()
#             server.login(EMAIL_SENDER, EMAIL_PASSWORD)
#             server.send_message(msg)

#         logging.info("Email notification sent.")
#     except Exception as e:
#         logging.error(f"Failed to send email: {e}")



# if __name__ == "__main__":
#     # This will register (or update) the deployments
#     defs = dg.Definitions(assets=[run_scraping_pipeline_wv,export_and_transfer_csv_wv,send_status_email_wv])

import os
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dagster import multi_asset, AssetOut, op, graph, Definitions, ScheduleDefinition, Field
from get_duckdb_data import export_duckdb_to_csv
from mft_utils import MFTClient
from wv_scraping_pipeline import scrape_raw_wv, transform_and_load_wv # replace with generic dispatch if needed
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

EMAIL_SENDER    = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD  = os.getenv("EMAIL_PASSWORD")
EMAIL_RECIPIENT = os.getenv("EMAIL_RECIPIENT")
SMTP_SERVER     = os.getenv("SMTP_SERVER", "smtp.office365.com")
SMTP_PORT       = int(os.getenv("SMTP_PORT", 587))

# List of states to automate
STATES = ["WV", "CA", "TX", "NY"]

@op(config_schema={"state": Field(str, description="State code for scraping")})
def scrape_data(context):
    state = context.op_config["state"]
    logging.info(f"Starting scraping for {state}")
    if state == "WV":
        scrape_raw_wv()
    else:
        raise ValueError(f"No scraper implemented for state '{state}'")
    logging.info(f"Completed scraping for {state}")

@op(config_schema={"state": Field(str)})
def transform_data(context, raw_df):
    state = context.op_config["state"]
    if state == "WV":
        return transform_and_load_wv(raw_df)
    else:
        raise NotImplementedError(f"Transform not implemented for {state}")

@op(config_schema={"state": Field(str, description="State code for export and transfer")})
def export_and_transfer_csv(context,combined_df):
    state = context.op_config["state"]
    logging.info(f"Exporting CSV for {state}")
    folder      = os.getenv(f"{state}_FOLDER")
    duckdb_file = os.getenv(f"{state}_DUCKDB_FILE")
    table_name  = os.getenv(f"{state}_TABLE_NAME")
    file_path, file_name = export_duckdb_to_csv(folder, duckdb_file, table_name, f"{state}_bulk_pipeline")
    logging.info(f"CSV Exported to {file_path}")
    # mft_client = MFTClient.from_env()  # assume MFTClient can load creds from env
    # mft_client.mft_file(file_path, file_name, des_folder=f"{state}_analyzed")
    # logging.info(f"MFT transfer completed for {state}")
    return file_path

@op(config_schema={
    "subject": Field(str, description="Email subject"),
    "body":    Field(str, description="Email body")
})
def send_status_email(context, exported_path):
    cfg = context.op_config
    msg = MIMEMultipart()
    msg['From']    = EMAIL_SENDER
    msg['To']      = EMAIL_RECIPIENT
    msg['Subject'] = cfg['subject']
    msg.attach(MIMEText(cfg['body'], 'plain'))

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)
    logging.info("Email notification sent.")

@graph
def state_pipeline():
    # wire up ops; literal values via config at job creation
    raw = scrape_data()
    combined = transform_data(raw_df = raw)
    file_path = export_and_transfer_csv(combined_df = combined)
    send_status_email(exported_path = file_path)

# Dynamically create a job + schedule for each state
jobs = []
# schedules = []
for state in STATES:
    job = state_pipeline.to_job(
        name=f"{state.lower()}_pipeline",
        config={
            "ops": {
                "scrape_data": {
                    "config": {"state": state}
                },
                "transform_data": {
                    "config": {"state": state}
                },
                "export_and_transfer_csv": {
                    "config": {"state": state}
                },
                "send_status_email": {
                    "config": {
                        "subject": f"{state} DOT Scraping Success",
                        "body":    f"{state} pipeline completed successfully."
                    }
                }
            }
        }
    )
    jobs.append(job)
    # schedules.append(
    #     ScheduleDefinition(
    #         job=job,
    #         cron_schedule="0 8 22,L * *",
    #         execution_timezone="US/Eastern"
    #     )
    # )

# if __name__ == "__main__":
# Register all jobs and schedules in Dagster
defs = Definitions(
    jobs=jobs
    # schedules=schedules
)
