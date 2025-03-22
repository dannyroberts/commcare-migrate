import os
import sys

import requests
from commcare_export.checkpoint import CheckpointManager
from commcare_export.cli import main as commcare_export
from sqlalchemy import select

DB = "postgresql://localhost:5432/det"


def call_commcare_export(username, apikey, query, commcare_hq, project):
    print(username, apikey, query)
    commcare_export([
        "--auth-mode", "apikey",
        "--username", username,
        "--password", apikey,
        "--query", query,
        "--output-format", "sql",
        "--output", DB,
        "--project", project,
        "--commcare-hq", commcare_hq,
    ])


def push_from_db_to_target(det_username, det_password, target_username, target_password, target_url, target_project):
    with get_db_context(DB) as sql_db:
        table = sql_db.get_table('Forms')
        result = sql_db.Session().execute(
            select(table).order_by(table.c.received_on)
        )
        for row in result:
            form_link = row['form_link']
            form_id = row['formid']
            form_received_on = row['received_on']
            if form_link is None:
                print(f"There is no form link for {form_id}")
                continue
            print(form_link)
            xform_response = requests.get(form_link,
                                          headers={'Authorization': f'ApiKey {det_username}:{det_password}'})
            if xform_response.status_code != 200:
                print(f"There was an error fetching the form {form_id} from {form_link}")
                continue
            xform = xform_response.text

            response = post_form(xform, f'{target_url}/a/{target_project}', target_username, target_password,
                                 spoofed_submit_time=form_received_on)
            if response.status_code == 500:
                print(f"There was an error on the target server processing form {form_id}")
                continue
            else:
                print(response.status_code)
                print(response.text)


def get_db_context(db_string):
    # This is kind of silly but I just want to be able to call .get_table and .Session
    # and know I'm getting exactly what commcare_export is using internally
    # I don't actually want any of the other CheckpointManager functionality, hence all the None values
    return CheckpointManager(DB, None, None, None, None)

def post_form(xform, target_project_url, target_username, target_password, spoofed_submit_time=None):
    url = f'{target_project_url}/receiver/api/'
    auth = (target_username, target_password)
    headers = {'Content-Type': 'text/html; charset=UTF-8'}
    if spoofed_submit_time:
        headers['X-SUBMIT-TIME'] = f'{spoofed_submit_time.isoformat()}Z'
        print(headers['X-SUBMIT-TIME'])
    return requests.post(url, xform.encode('utf-8'),
                             headers=headers, auth=auth)


if __name__ == "__main__":
    SOURCE_URL = os.getenv("SOURCE_URL") or "https://www.commcarehq.org"
    SOURCE_PROJECT = os.getenv("SOURCE_PROJECT")
    SOURCE_USERNAME = os.getenv("SOURCE_USERNAME")
    SOURCE_APIKEY = os.getenv("SOURCE_APIKEY")
    TARGET_URL = os.getenv("TARGET_URL")
    TARGET_PROJECT = os.getenv("TARGET_PROJECT")
    TARGET_USERNAME = os.getenv("TARGET_USERNAME")
    TARGET_PASSWORD = os.getenv("TARGET_PASSWORD")
    TARGET_APIKEY = os.getenv("TARGET_APIKEY")

    if sys.argv[1] == 'fetch':
        call_commcare_export(
            SOURCE_USERNAME,
            SOURCE_APIKEY,
            query='./commcare-migrate-source.xlsx',
            commcare_hq=SOURCE_URL,
            project=SOURCE_PROJECT,
        )
    elif sys.argv[1] == 'fetch-target':
        call_commcare_export(
            TARGET_USERNAME,
            TARGET_APIKEY,
            query='./commcare-migrate-target.xlsx',
            commcare_hq=TARGET_URL,
            project=TARGET_PROJECT,
        )
    elif sys.argv[1] == 'push':
        push_from_db_to_target(SOURCE_USERNAME, SOURCE_APIKEY, TARGET_USERNAME, TARGET_PASSWORD, TARGET_URL, TARGET_PROJECT)
