# quasar-edge PoC

Auth-scoped WebSocket query gateway with cache + invalidation refetch.

## Scope

- Accept WS clients, authenticate per connection, and scope cache by user.
- Serve query snapshots from cache or fetch-through from Core.
- Consume ordered invalidation events and refetch affected subscriptions.
- On ordering gaps, refetch all currently subscribed keys.

Out of scope: durable storage, eviction policies beyond subscription lifecycle, distributed coordination, and diff/patch updates.

## Setup

```bash
cp .env.example .env
```

Edit `.env` to set `AUTH_JWT_SECRET` and any other overrides.

## Run

```bash
cargo run
```

Routes:

- `GET /healthz`
- `GET /ws`

## WS Protocol

First client message must be init:

```json
{"token":"<jwt>"}
```

Then operation messages:

```json
{"Subscribe":{"query_id":"feed","args":{"limit":20}}}
{"Unsubscribe":{"query_id":"feed","args":{"limit":20}}}
```

Server messages:

```json
{"Snapshot":{"query_id":"feed","payload":{"items":[]}}}
{"Error":{"message":"..."}}
```

## Core Contract

- Query fetch: `POST {CORE_BASE_URL}/query/{query_id}` with header `X-Auth-Context`.
- Query response:
```json
{"version":123,"result":{}}
```
- Invalidation stream: NDJSON from `CORE_INVALIDATION_STREAM_URL`.
```json
{"version":124,"affected_query_ids":["feed"]}
```
