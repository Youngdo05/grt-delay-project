# grt-delay-project

Archive of Grand River Transit (GRT) GTFS Realtime snapshots. A
scheduled GitHub Actions workflow polls the public bus and ION light
rail TripUpdates feeds every 10 minutes during service hours and
commits the raw protobuf snapshots to this repository.

Collected for University of Waterloo coursework (Spring 2026). This
repository contains data collection only.

## Layout

```
logger.py                     poller: fetch feeds, archive raw protobufs
.github/workflows/collect.yml schedule: every 10 min during service hours
requirements.txt              requests, gtfs-realtime-bindings, tzdata
data/raw/<date>/*.pb.gz       gzipped raw feed snapshots (committed)
data/status.txt               heartbeat: last success time and row count
schedule/                     dated static GTFS zips, bus and LRT separately
```

## Local use

```
pip install -r requirements.txt
python logger.py
```

Set `GRT_WRITE_CSV=0` to skip the local parsed CSV (the Actions
workflow does this and commits raw snapshots only).
