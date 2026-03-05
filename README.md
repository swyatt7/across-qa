# across-qa
qa scripts for the NASA-ACROSS project

## Daily Slack ingestion-status notification

A GitHub Actions workflow (`.github/workflows/slack-ingestion-check.yml`)
runs automatically every day at **08:00 MST** and posts the current telescope
schedule ingestion health — along with a PNG timeline — to a Slack channel.

### One-time configuration

Follow these steps once to enable the workflow in your fork / repository:

#### 1. Create a Slack app and bot

1. Go to <https://api.slack.com/apps> and click **Create New App → From scratch**.
2. Give the app a name (e.g. *across-qa-bot*) and select the target workspace.
3. Under **OAuth & Permissions → Scopes → Bot Token Scopes**, add:
   - `chat:write` — post messages
   - `files:write` — upload the PNG attachment
4. Click **Install to Workspace** and copy the **Bot User OAuth Token**
   (starts with `xoxb-`).
5. Invite the bot to the target Slack channel:
   ```
   /invite @across-qa-bot
   ```
6. Note the **Channel ID** (right-click the channel in Slack → *View channel
   details* → copy the ID at the bottom, e.g. `C01234ABCDE`).

#### 2. Add repository secrets

In your GitHub repository go to **Settings → Secrets and variables → Actions**
and add:

| Secret name        | Value                                                  |
|--------------------|--------------------------------------------------------|
| `SLACK_BOT_TOKEN`  | Bot User OAuth Token from step 4 above (`xoxb-…`)     |
| `SLACK_CHANNEL_ID` | Slack channel ID from step 6 above (e.g. `C01234ABCDE`) |

#### 3. Trigger a test run

Navigate to **Actions → Telescope Ingestion Status → Slack** and click
**Run workflow** to verify the bot posts correctly before the first scheduled
run.

---

## Quick start

Install in editable mode:

```bash
pip install -e .
```

Run the CLI:

```bash
across-qa
```

## Example

Filter to a telescope name and a schedule status:

```bash
across-qa --telescope swift --status planned
```

Example output:

```text
telescope_name telescope_id schedule_status      cron             last_ingested             next_expected status                                        message
	Swift         t-01       planned 0 * * * * 2026-03-04 10:00:00+00:00 2026-03-04 11:00:00+00:00     OK Schedule is up-to-date. Next expected by 2026-03-04T11:00:00Z.
```

Fail CI when any result is `LATE` or `MISSING`:

```bash
across-qa --exit-code
```

## Python example

Use the checker directly in Python:

```python
from across.client import Client
from across_qa.checker import check_all_telescopes

client = Client()
df = check_all_telescopes(client=client)

# Keep only problematic rows
issues = df[df["status"].isin(["LATE", "MISSING"])]

if issues.empty:
	print("All telescope cadence checks are OK")
else:
	print(issues[["telescope_name", "schedule_status", "status", "message"]].to_string(index=False))
```
