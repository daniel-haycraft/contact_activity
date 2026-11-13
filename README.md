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

Do not rename or move the folder after creating `.venv`. If you do, delete `.venv` and run the setup again.

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

## Output

`contact_activities.json`, saved as the script runs, so it can be opened mid-run.

- **Top level:** `generated_at`, `status` (`in_progress` or `complete`), `stages_completed`, `contact_count`, `contacts`
- **Each contact:** email, HubSpot contact ID, name, phones, lifecycle stage, owner, activity counts, and an `activities` list sorted oldest to newest
- **Each activity:** `source` (`hubspot` or `sakari`), `type` (`call`, `email`, `note`, `sms`), `timestamp`, plus the content for that type
- Emails that are not in HubSpot are listed with `"found": false`

## Good to know

- **Rerunning the HubSpot stage starts the JSON over**, which removes Sakari texts already added. Run Sakari again after it.
- **The Sakari stage is safe to rerun.** It skips texts already in the file.
- **Call transcripts are usually empty.** HubSpot's transcript API does not return transcripts HubSpot generates itself. The script tries, then stops after 25 failures in a row and moves on.
- **CF1's own phone lines are ignored for Sakari matching.** Any number that shows up on calls for 3 or more contacts is treated as a company line.
- **Each Sakari text includes a `raw` field** with Sakari's original payload. Set `KEEP_SAKARI_RAW = False` in the CONFIG block for a smaller file.
- **Tasks are not included.**

## Troubleshooting

| Problem | Fix |
| --- | --- |
| `KeyError: 'HUBSPOT_TOKEN'` (or a Sakari key) | That value is missing from `.env`, or `.env` is not next to the script |
| `No module named ...` | The venv is not active, or run `pip install -r requirements.txt` |
| `Fatal error in launcher` when using pip | The folder was renamed. Delete `.venv` and redo the setup |
| `MISSING_SCOPES` from HubSpot | Add the scope named in the error to the private app |
| `Sakari auth failed` | Check the three Sakari values in `.env` |
| `contact_activities.json doesn't exist yet` | Run the HubSpot stage before the Sakari stage |
