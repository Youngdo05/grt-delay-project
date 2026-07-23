# grt-delay-project

Archive of Grand River Transit (GRT) GTFS Realtime snapshots. A
scheduled GitHub Actions workflow polls the public bus and ION light
rail TripUpdates feeds about every 2 minutes during weekday rush
windows and about every 10 minutes otherwise during service hours. It
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

## Local use

```
pip install -r requirements.txt
python logger.py
```

Set `GRT_WRITE_CSV=0` to skip the local parsed CSV (the Actions
workflow does this and commits raw snapshots only).
