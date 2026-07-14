# grt-delay-project

Self collected Grand River Transit (GRT) GTFS Realtime archive. A
scheduled GitHub Actions workflow polls the public bus and ION light
rail TripUpdates feeds every 10 minutes during service hours and
commits the raw protobuf snapshots to this repository.

This repository is the shared data layer for two University of
Waterloo course projects (MSE 446 and CS 486, Spring 2026, disclosed
in both proposals). It contains data collection only. All analysis
lives in separate repositories, one per course.

## Why collect at all

GRT publishes its realtime feeds as live snapshots that are
overwritten continuously. No public historical archive exists. The
feeds publish predicted arrival times without delay fields, so delay
labels are derived later by joining each prediction against the static
GTFS schedule (stored under `schedule/`).

## Layout

```
logger.py                     poller: fetch feeds, archive raw protobufs
.github/workflows/collect.yml schedule: every 10 min during service hours
requirements.txt              requests, gtfs-realtime-bindings, tzdata
data/raw/<date>/*.pb.gz       gzipped raw feed snapshots (committed)
data/status.txt               heartbeat: last success time and row count
data/csv/                     parsed daily CSVs (local only, not committed)
schedule/                     dated static GTFS zips, bus and LRT separately
```

## Feeds (verified live 2026-07-07)

Publishing tool: `https://webapps.regionofwaterloo.ca/api/grt-routes/`

| Feed | URL suffix |
| --- | --- |
| Bus TripUpdates | `/api/tripupdates/1` |
| LRT TripUpdates | `/api/tripupdates/2` |
| Bus static GTFS | `/api/staticfeeds/1` |
| LRT static GTFS | `/api/staticfeeds/2` |

## Local use

```
pip install -r requirements.txt
python logger.py
```

Each run archives one snapshot per feed and appends parsed rows to
`data/csv/tripupdates_<date>.csv`. Set `GRT_WRITE_CSV=0` to skip the
CSV (the Actions workflow does this and commits raw only).

## Operational notes

The label for a (trip, stop) pair is the last prediction observed
before the vehicle passes the stop, so occasional missed polls thin
the data without biasing it. The Actions cron is best effort and some
runs will be skipped or late. That is acceptable by design.
