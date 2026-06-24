---
title: "Workflow Status API"
order: 6
---

# Workflow Status API

Zipline exposes a workflow status endpoint for scripts and external systems that need to monitor a workflow run after it has been created.

Use this API when you need to poll workflow state from automation. The Zipline Hub UI uses the same workflow status data on workflow detail pages.

## Endpoint

```http
GET /workflow/v2/{workflowId}/status
```

Replace `{workflowId}` with the workflow ID returned by the workflow start API or shown in the Zipline Hub UI.

If your deployment enables authentication, include a bearer token. The `zipline auth get-access-token` command prints a short-lived token that can be used from scripts:

```bash
curl -H "Authorization: Bearer $(zipline auth get-access-token)" \
  https://zipline.example.com/workflow/v2/<workflowId>/status
```

The endpoint requires viewer access when role-based authorization is enabled.

## Response

The response contains the workflow-level status and the node execution timeline for that workflow run.

```json
{
  "workflowId": "6f5c8e2d-2f0d-4d5e-8f6b-9e7e4f5c2d1a",
  "confName": "team.user_features.v1",
  "mode": "backfill",
  "branch": "main",
  "user": "user@example.com",
  "status": "2",
  "startPartition": "2026-04-01",
  "endPartition": "2026-04-05",
  "terminalNodes": ["team.user_features.v1"],
  "nodeExecutions": [
    {
      "nodeName": "team.user_features.v1",
      "nodeHash": "abc123",
      "startPartition": "2026-04-01",
      "endPartition": "2026-04-05",
      "dependencies": [],
      "stepRuns": [
        {
          "runId": "step-run-id",
          "workflowId": "6f5c8e2d-2f0d-4d5e-8f6b-9e7e4f5c2d1a",
          "startPartition": "2026-04-01",
          "endPartition": "2026-04-05",
          "status": "2",
          "attempt": "1",
          "jobTrackingInfo": {
            "jobUrl": "https://..."
          }
        }
      ]
    }
  ]
}
```

Workflow and step status fields are returned as enum wire values. Common workflow status values are:

| Value | Status           | Meaning                                                                         |
| ----- | ---------------- | ------------------------------------------------------------------------------- |
| `1`   | `WAITING`        | The workflow has been accepted but has not started running yet.                 |
| `2`   | `RUNNING`        | At least one step is actively running or the workflow is still being processed. |
| `3`   | `SUCCEEDED`      | The workflow completed successfully.                                            |
| `4`   | `FAILED`         | The workflow reached a terminal failure.                                        |
| `5`   | `CANCELLED`      | The workflow was cancelled.                                                     |
| `6`   | `CANCEL_PENDING` | A cancellation request has been accepted but is not complete yet.               |

## Polling

Poll the status endpoint until the workflow reaches a terminal status:

- `SUCCEEDED`
- `FAILED`
- `CANCELLED`

In JSON responses, these terminal statuses correspond to enum values `3`, `4`, and `5`.

A typical polling loop checks every 15 to 60 seconds. Use a longer interval for long-running backfills or scheduled workflows.

For detailed progress, inspect `nodeExecutions[].stepRuns[]`. Step runs include status, partition range, attempt number, dependency links, and job tracking URLs when available.

## Notes

- The workflow status endpoint is the canonical API for programmatic workflow monitoring.
- Zipline does not currently return a `statusUrl` from workflow creation responses.
- Zipline does not currently send workflow webhooks. External systems should poll this endpoint when they need workflow completion or failure notifications.
