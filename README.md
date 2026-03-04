# across-qa
qa scripts for the NASA-ACROSS project

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
