# grt-delay-project

Archive of Grand River Transit (GRT) GTFS Realtime snapshots. A
scheduled GitHub Actions workflow polls the public bus and ION light
rail TripUpdates feeds about every 2 minutes during rush-clock windows
every day and about every 10 minutes otherwise during service hours. It
commits the raw protobuf snapshots to this repository.

Collected for University of Waterloo coursework (Spring 2026). This
repository contains data collection only.

## Layout

```
logger.py                     poller: fetch feeds, archive raw protobufs
.github/workflows/collect.yml adaptive realtime collection schedule
.github/workflows/schedule-watch.yml validate and archive static GTFS changes
requirements.txt              requests, gtfs-realtime-bindings, tzdata
data/raw/<date>/*.pb.gz       gzipped raw feed snapshots (committed)
data/status.txt               heartbeat: last success time and row count
schedule/                     dated static GTFS zips, bus and LRT separately
```

GitHub scheduled workflows can start late or omit a scheduled event.
The archive therefore records achieved coverage rather than assuming
the target cadence was met.

Raw responses are archived even when validation fails. A parsed realtime
feed is usable only when its header timestamp is no more than ten minutes
old and no more than two minutes in the future. The heartbeat separately
records whether each mode contained scheduled arrival rows. A poll is
successful for workflow accounting only when both modes contain at least one
usable scheduled arrival; a structurally valid zero-row response is archived
but cannot make the run green.

## Local use

```
pip install -r requirements.txt
python logger.py
```

Set `GRT_WRITE_CSV=0` to skip the local parsed CSV (the Actions
workflow does this and commits raw snapshots only).
