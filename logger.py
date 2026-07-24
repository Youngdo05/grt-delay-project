"""GRT GTFS Realtime logger.

Fetches TripUpdates from GRT's public feeds, saves the raw protobuf
(gzipped) for reproducibility, and optionally appends parsed per stop
rows to a daily CSV for local inspection.

Designed to be run every ~10 minutes by GitHub Actions or cron.
A missed run does not corrupt saved snapshots, but it reduces temporal
resolution and can change label freshness or hide short-lived dynamics.

Note: this feed publishes predicted arrival/departure times but NOT
delay fields (verified 2026-07-07: 0 of 10,219 bus stop updates had
arrival.delay). Delay labels are derived downstream by joining
arrival_time_utc against the static GTFS schedule. The delay columns
are kept in the CSV schema for completeness but will be blank.

Env vars:
  GRT_WRITE_CSV=0  skip the parsed daily CSV (used in Actions, where
                   only raw protobufs are committed; CSVs are rebuilt
                   locally at analysis time)
"""

import csv
import gzip
import io
import os
import ssl
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from google.transit import gtfs_realtime_pb2

# ---------------------------------------------------------------------------
# CONFIG - URLs extracted and verified live on 2026-07-07 (Task A)
# ---------------------------------------------------------------------------
BASE = "https://webapps.regionofwaterloo.ca/api/grt-routes/api"
FEEDS = {
    "bus_tripupdates": f"{BASE}/tripupdates/1",
    "lrt_tripupdates": f"{BASE}/tripupdates/2",
    # Vehicle positions are optional; delays come from TripUpdates.
    # "bus_vehiclepositions": f"{BASE}/vehiclepositions/1",
    # "lrt_vehiclepositions": f"{BASE}/vehiclepositions/2",
}

LOCAL_TZ = ZoneInfo("America/Toronto")
DATA_DIR = Path(__file__).parent / "data"
RAW_DIR = DATA_DIR / "raw"
CSV_DIR = DATA_DIR / "csv"
WRITE_CSV = os.environ.get("GRT_WRITE_CSV", "1") != "0"
MAX_FEED_AGE_S = 600
MAX_FUTURE_FEED_S = 120

CSV_FIELDS = [
    "poll_utc",          # when we fetched the feed
    "feed",              # bus_tripupdates / lrt_tripupdates
    "feed_ts_utc",       # FeedHeader.timestamp (feed generation time)
    "trip_id",
    "route_id",
    "start_date",
    "start_time",
    "schedule_relationship",
    "stop_schedule_relationship",
    "vehicle_id",
    "stop_sequence",
    "stop_id",
    "arrival_delay_s",   # blank on this feed; kept for schema stability
    "arrival_time_utc",  # predicted arrival epoch; the label source
    "departure_delay_s",
    "departure_time_utc",
]


class LegacyTLSAdapter(HTTPAdapter):
    """The Region of Waterloo server negotiates a small DH key that
    OpenSSL 3 rejects at its default security level (DH_KEY_TOO_SMALL,
    observed 2026-07-14). Lower the cipher security level for this
    session only. Certificate verification stays fully enabled."""

    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)


SESSION = requests.Session()
SESSION.mount("https://", LegacyTLSAdapter())


def fetch(url: str, retries: int = 3) -> bytes:
    last_err = None
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=15)
            r.raise_for_status()
            return r.content
        except Exception as e:
            last_err = e
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"fetch failed after {retries} tries: {url}: {last_err}")


def parse_tripupdates(blob: bytes, feed_name: str, poll_utc: str) -> list[dict]:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(blob)
    # protobuf accepts b"" without raising and returns an uninitialized proto2
    # message. Treat that as a parse failure: otherwise an empty HTTP-200 body
    # is reported as a successful zero-row feed and can mask a source outage.
    if not msg.IsInitialized():
        missing = ", ".join(msg.FindInitializationErrors()) or "required fields"
        raise ValueError(f"uninitialized GTFS-Realtime message (missing {missing})")
    if not msg.header.HasField("timestamp") or msg.header.timestamp <= 0:
        raise ValueError("GTFS-Realtime feed is missing a valid header timestamp")
    poll_epoch = int(
        datetime.strptime(poll_utc, "%Y-%m-%dT%H:%M:%SZ")
        .replace(tzinfo=timezone.utc)
        .timestamp()
    )
    feed_ts = int(msg.header.timestamp)
    feed_age = poll_epoch - feed_ts
    if feed_age > MAX_FEED_AGE_S:
        raise ValueError(
            f"stale GTFS-Realtime feed: header is {feed_age}s old "
            f"(maximum {MAX_FEED_AGE_S}s)"
        )
    if feed_age < -MAX_FUTURE_FEED_S:
        raise ValueError(
            f"future GTFS-Realtime timestamp: header is {-feed_age}s ahead "
            f"(maximum {MAX_FUTURE_FEED_S}s)"
        )
    rows = []
    for ent in msg.entity:
        if not ent.HasField("trip_update"):
            continue
        tu = ent.trip_update
        trip = tu.trip
        base = {
            "poll_utc": poll_utc,
            "feed": feed_name,
            "feed_ts_utc": feed_ts,
            "trip_id": trip.trip_id,
            "route_id": trip.route_id,
            "start_date": trip.start_date,
            "start_time": trip.start_time,
            "schedule_relationship": trip.schedule_relationship,
            "vehicle_id": tu.vehicle.id if tu.HasField("vehicle") else "",
        }
        for stu in tu.stop_time_update:
            row = dict(base)
            row["stop_sequence"] = stu.stop_sequence
            row["stop_id"] = stu.stop_id
            row["stop_schedule_relationship"] = stu.schedule_relationship
            row["arrival_delay_s"] = (
                stu.arrival.delay if stu.HasField("arrival") and stu.arrival.HasField("delay") else ""
            )
            row["arrival_time_utc"] = (
                stu.arrival.time if stu.HasField("arrival") and stu.arrival.HasField("time") else ""
            )
            row["departure_delay_s"] = (
                stu.departure.delay if stu.HasField("departure") and stu.departure.HasField("delay") else ""
            )
            row["departure_time_utc"] = (
                stu.departure.time if stu.HasField("departure") and stu.departure.HasField("time") else ""
            )
            rows.append(row)
    return rows


def validate_static_gtfs(blob: bytes) -> None:
    """Reject error pages and incomplete archives before schedule archival."""
    required = {"stop_times.txt", "trips.txt", "calendar_dates.txt"}
    try:
        with zipfile.ZipFile(io.BytesIO(blob)) as archive:
            bad_member = archive.testzip()
            if bad_member is not None:
                raise ValueError(f"CRC failure in {bad_member}")
            missing = required - set(archive.namelist())
            if missing:
                raise ValueError(f"missing required files: {sorted(missing)}")
    except zipfile.BadZipFile as exc:
        raise ValueError("static GTFS response is not a valid ZIP archive") from exc


def is_usable_scheduled_arrival(row: dict) -> bool:
    """Whether one parsed row can contribute to downstream arrival analysis."""
    return (
        row["schedule_relationship"] == 0
        and row["stop_schedule_relationship"] == 0
        and row["arrival_time_utc"] != ""
    )


def collection_exit_code(feed_rows: dict, feed_usable_rows: dict) -> int:
    """Return 0 only when every configured feed has usable arrival rows."""
    all_usable = all(
        feed_name in feed_rows and feed_usable_rows.get(feed_name, 0) > 0
        for feed_name in FEEDS
    )
    if all_usable:
        return 0
    return 1 if any(feed_usable_rows.values()) else 2


def main() -> int:
    now_utc = datetime.now(timezone.utc)
    poll_utc = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    local_day = now_utc.astimezone(LOCAL_TZ).strftime("%Y-%m-%d")
    stamp = now_utc.strftime("%Y%m%dT%H%M%SZ")

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    if WRITE_CSV:
        CSV_DIR.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    failures = []
    feed_rows = {}
    feed_usable_rows = {}
    for feed_name, url in FEEDS.items():
        try:
            blob = fetch(url)
        except RuntimeError as e:
            print(f"[error] {e}")
            failures.append(feed_name)
            continue

        # 1) archive raw protobuf, gzipped, BEFORE parsing - a schema
        #    surprise must never cost us the raw bytes
        raw_path = RAW_DIR / local_day / f"{feed_name}_{stamp}.pb.gz"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(raw_path, "wb") as f:
            f.write(blob)

        # 2) parse and verify feed freshness, guarded per feed so one bad feed
        #    cannot stop the other. Raw bytes remain archived for auditability.
        try:
            rows = parse_tripupdates(blob, feed_name, poll_utc)
        except Exception as e:
            print(f"[error] {feed_name}: parse failed (raw archived): {e}")
            failures.append(f"{feed_name}:parse")
            continue
        if WRITE_CSV:
            csv_path = CSV_DIR / f"tripupdates_{local_day}.csv"
            new_file = not csv_path.exists()
            with open(csv_path, "a", newline="") as f:
                w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                if new_file:
                    w.writeheader()
                w.writerows(rows)
        feed_rows[feed_name] = len(rows)
        feed_usable_rows[feed_name] = sum(
            is_usable_scheduled_arrival(row) for row in rows)
        total_rows += len(rows)
        if feed_usable_rows[feed_name] == 0:
            print(
                f"[warn] {feed_name}: parsed successfully but has no "
                "scheduled arrival rows; archived but unusable for analysis"
            )
        else:
            print(
                f"[ok] {feed_name}: {len(rows)} stop-time rows, "
                f"{feed_usable_rows[feed_name]} scheduled arrivals"
            )

    # Heartbeat for the morning glance. last_success_utc advances only when at
    # least one feed contains a usable scheduled arrival, so an empty or
    # status-only response cannot masquerade as usable collection. Per-feed
    # success and usability remain separate below.
    status_path = DATA_DIR / "status.txt"
    last_success = ""
    if status_path.exists():
        for line in status_path.read_text().splitlines():
            if line.startswith("last_success_utc="):
                last_success = line.split("=", 1)[1]
                break
    if sum(feed_usable_rows.values()) > 0:
        last_success = poll_utc
    per_feed = ""
    for feed_name in FEEDS:
        short = feed_name.split("_")[0]
        got = feed_name in feed_rows
        per_feed += f"{short}_rows={feed_rows.get(feed_name, '')}\n"
        per_feed += f"{short}_success={'true' if got else 'false'}\n"
        per_feed += (
            f"{short}_usable="
            f"{'true' if feed_usable_rows.get(feed_name, 0) > 0 else 'false'}\n"
        )
    with open(status_path, "w") as f:
        f.write(
            f"last_attempt_utc={poll_utc}\n"
            f"last_success_utc={last_success}\n"
            f"rows_this_poll={total_rows}\n"
            f"{per_feed}"
            f"failures={','.join(failures) or 'none'}\n"
        )

    # Exit codes consumed by the workflow's failure accounting:
    #   0 = every feed has usable scheduled arrivals
    #   1 = at least one feed has usable scheduled arrivals
    #   2 = no feed has usable scheduled arrivals
    return collection_exit_code(feed_rows, feed_usable_rows)


if __name__ == "__main__":
    sys.exit(main())
