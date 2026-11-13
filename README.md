# Contact Activity Pipeline

Takes an Excel list of contact emails and builds one JSON file with each contact's full activity history from HubSpot and Sakari. The JSON is meant to be handed to an AI agent or used for analysis.

## What it pulls

| Stage | Source | What you get |
| --- | --- | --- |
| 1 | HubSpot | Calls, emails, notes, and HubSpot-logged SMS, with full content, all time |
| 2 | Sakari | Every text sent or received, matched to contacts by phone number |

## Setup (one time)

From the project folder:

```
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

If there is no `requirements.txt` yet:

```
pip install requests pandas openpyxl python-dotenv
pip freeze > requirements.txt
```

## Credentials

Create a file named `.env` next to the script. No quotes, no spaces around the `=`.

```
HUBSPOT_TOKEN=
SAKARI_CLIENT_ID=
SAKARI_CLIENT_SECRET=
SAKARI_ACCOUNT_ID=
```

- **HubSpot:** private app token with these scopes: `crm.objects.contacts.read`, `sales-email-read`, `crm.objects.owners.read`, `crm.schemas.custom.read`. Without `sales-email-read`, email bodies come back redacted.
- **Sakari:** hub.sakari.io, cog icon, API Credentials.

Never commit `.env`. The `.gitignore` should contain:

```
.env
.venv/
```

## Input

An Excel file named `contacts.xlsx` in the project folder, with the emails in a column called `E-mail Address`. Both can be changed in the CONFIG block or with `--input` and `--column`.

## Run

Activate the venv first (`.venv\Scripts\activate`), then:

| Command | What it does |
| --- | --- |
| `python contact_activity_pipeline.py` | Both stages |
| `python contact_activity_pipeline.py --stage hubspot` | HubSpot only |
| `python contact_activity_pipeline.py --stage sakari` | Sakari only, added into the existing JSON |
| `python contact_activity_pipeline.py --limit 5` | Test run on the first 5 contacts |
| `python contact_activity_pipeline.py --discover` | Lists the object types attached to the first contact, then exits |
| `python contact_activity_pipeline.py --sakari-sample` | Prints one raw Sakari message, then exits |
| `python contact_activity_pipeline.py --test-transcript` | Shows HubSpot's raw reply for one transcript, then exits |


`contact_activities.json`, saved as the script runs, so it can be opened mid-run.
