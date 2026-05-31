---
title: "Fetcher API"
order: 6
---

# Fetcher API

The Chronon fetcher service exposes HTTP endpoints for online feature serving and schema inspection. These endpoints read from the online KV store and metadata uploaded during deploy, so they are intended for online `GroupBy`s and `Join`s.

Names in path parameters are Chronon metadata names. URL-encode names that contain path separators.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/ping` | Health check endpoint. |
| `GET` | `/config` | Fetcher service configuration. |
| `GET` | `/v1/joins` | Lists Chronon joins marked `online=True`. |
| `GET` | `/v1/join/:name/schema` | Returns the online fetch schema for a join. |
| `GET` | `/v1/groupby/:name/schema` | Returns the online fetch schema for a GroupBy. |
| `GET` | `/v1/groupby/:name/status` | Returns online GroupBy upload status. |
| `GET` | `/v1/stats/:tableName` | Returns enhanced statistics for a table when stats are available. |
| `POST` | `/v1/fetch/groupby/:name` | Fetches features for one GroupBy. |
| `POST` | `/v1/fetch/join/:name` | Fetches features for one join. |
| `POST` | `/v1/fetch/modeltransforms/:name` | Fetches model transform outputs. |
| `POST` | `/v2/fetch/join/:name` | Fetches join features with the feature payload encoded as a base64 Avro string. |

## Fetch Requests

The v1 fetch endpoints accept a JSON array of entity key maps. Each map is one lookup. The response preserves request order.

```bash
curl -X POST "http://localhost:9000/v1/fetch/join/quickstart.demo_v1" \
  -H "Content-Type: application/json" \
  -d '[{"user_id": "5"}, {"user_id": "7"}]'
```

Successful v1 fetch responses use this shape:

```json
{
  "results": [
    {
      "status": "Success",
      "entityKeys": {"user_id": "5"},
      "features": {"purchase_count_30d": 12}
    }
  ]
}
```

Individual lookup failures are returned as result entries with `"status": "Failure"` and an `"error"` field. Request-level failures, such as invalid JSON, return an `errors` array.

## Join Schema

`GET /v1/join/:name/schema` returns the Avro schemas used by online join fetching and logging.

```bash
curl "http://localhost:9000/v1/join/quickstart.demo_v1/schema" | jq
```

Response fields:

| Field | Description |
|-------|-------------|
| `joinName` | Join metadata name. |
| `keySchema` | Avro schema for entity keys. |
| `valueSchema` | Avro schema for fetched feature values after join derivations. |
| `schemaHash` | Hash of the join logging schema payload. |
| `valueInfos` | Per-feature metadata, including feature name, source group, prefix, lookup keys, and type string. |

## GroupBy Schema

`GET /v1/groupby/:name/schema` returns the Avro schemas used by online GroupBy fetching.

```bash
curl "http://localhost:9000/v1/groupby/quickstart.user_activity_v1/schema" | jq
```

Response fields:

| Field | Description |
|-------|-------------|
| `groupByName` | GroupBy metadata name. |
| `keySchema` | Avro schema for entity keys. |
| `valueSchema` | Avro schema for values returned by online fetches, including derivations when configured. |
| `inputSchema` | Avro schema before applying source select expressions. |
| `selectedSchema` | Avro schema after applying source select expressions. |

GroupBy schema fetching is available only for online GroupBys. If a GroupBy is not online, use the Iceberg catalog schema through eval for the offline table schema, or enable `online=True` and upload the GroupBy.

## GroupBy Status

`GET /v1/groupby/:name/status` returns the latest uploaded batch watermark for an online GroupBy.

```bash
curl "http://localhost:9000/v1/groupby/quickstart.user_activity_v1/status" | jq
```

Response fields:

| Field | Description |
|-------|-------------|
| `groupByName` | GroupBy metadata name. |
| `batchEndDate` | Date through which batch upload data is available in the online KV store. |

## CLI

The Zipline CLI can call schema endpoints through the fetcher service:

```bash
zipline hub fetch compiled/joins/quickstart/demo_v1 --schema --fetcher-url http://localhost:9000
zipline hub fetch compiled/group_bys/quickstart/user_activity_v1 --schema --fetcher-url http://localhost:9000
```
