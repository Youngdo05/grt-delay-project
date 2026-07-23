import io
import unittest
import zipfile

from google.transit import gtfs_realtime_pb2

import logger


class ParseTripUpdatesTests(unittest.TestCase):
    def test_empty_payload_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "uninitialized GTFS-Realtime"):
            logger.parse_tripupdates(b"", "bus_tripupdates", "2026-07-22T00:00:00Z")

    def test_initialized_zero_entity_feed_is_valid(self):
        msg = gtfs_realtime_pb2.FeedMessage()
        msg.header.gtfs_realtime_version = "2.0"
        msg.header.timestamp = 1_784_678_400
        rows = logger.parse_tripupdates(
            msg.SerializeToString(), "bus_tripupdates", "2026-07-22T00:00:00Z")
        self.assertEqual(rows, [])

    def test_valid_arrival_is_parsed(self):
        msg = gtfs_realtime_pb2.FeedMessage()
        msg.header.gtfs_realtime_version = "2.0"
        msg.header.timestamp = 1_784_678_400
        entity = msg.entity.add()
        entity.id = "e1"
        trip = entity.trip_update.trip
        trip.trip_id = "T1"
        trip.route_id = "R1"
        trip.start_date = "20260722"
        update = entity.trip_update.stop_time_update.add()
        update.stop_sequence = 3
        update.stop_id = "S3"
        update.arrival.time = 1_784_682_000

        rows = logger.parse_tripupdates(
            msg.SerializeToString(), "bus_tripupdates", "2026-07-22T00:00:00Z")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["trip_id"], "T1")
        self.assertEqual(rows[0]["stop_id"], "S3")
        self.assertEqual(rows[0]["stop_schedule_relationship"], 0)
        self.assertEqual(rows[0]["arrival_time_utc"], 1_784_682_000)

    def test_missing_header_timestamp_is_rejected(self):
        msg = gtfs_realtime_pb2.FeedMessage()
        msg.header.gtfs_realtime_version = "2.0"
        with self.assertRaisesRegex(ValueError, "missing a valid header timestamp"):
            logger.parse_tripupdates(
                msg.SerializeToString(), "bus_tripupdates",
                "2026-07-22T00:00:00Z")

    def test_feed_older_than_ten_minutes_is_rejected(self):
        msg = gtfs_realtime_pb2.FeedMessage()
        msg.header.gtfs_realtime_version = "2.0"
        msg.header.timestamp = 1_784_677_799
        with self.assertRaisesRegex(ValueError, "stale GTFS-Realtime feed"):
            logger.parse_tripupdates(
                msg.SerializeToString(), "bus_tripupdates",
                "2026-07-22T00:00:00Z")

    def test_feed_at_ten_minute_boundary_is_accepted(self):
        msg = gtfs_realtime_pb2.FeedMessage()
        msg.header.gtfs_realtime_version = "2.0"
        msg.header.timestamp = 1_784_677_800
        self.assertEqual(
            logger.parse_tripupdates(
                msg.SerializeToString(), "bus_tripupdates",
                "2026-07-22T00:00:00Z"),
            [],
        )

    def test_far_future_header_timestamp_is_rejected(self):
        msg = gtfs_realtime_pb2.FeedMessage()
        msg.header.gtfs_realtime_version = "2.0"
        msg.header.timestamp = 1_784_678_521
        with self.assertRaisesRegex(ValueError, "future GTFS-Realtime timestamp"):
            logger.parse_tripupdates(
                msg.SerializeToString(), "bus_tripupdates",
                "2026-07-22T00:00:00Z")


class StaticGtfsValidationTests(unittest.TestCase):
    @staticmethod
    def archive(*names):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as output:
            for name in names:
                output.writestr(name, "header\n")
        return buffer.getvalue()

    def test_non_zip_response_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "not a valid ZIP"):
            logger.validate_static_gtfs(b"<html>temporary error</html>")

    def test_incomplete_static_archive_is_rejected(self):
        payload = self.archive("trips.txt", "stop_times.txt")
        with self.assertRaisesRegex(ValueError, "calendar_dates.txt"):
            logger.validate_static_gtfs(payload)

    def test_required_static_archive_is_accepted(self):
        payload = self.archive(
            "trips.txt", "stop_times.txt", "calendar_dates.txt")
        logger.validate_static_gtfs(payload)


class UsableArrivalTests(unittest.TestCase):
    def test_only_scheduled_trip_and_stop_with_arrival_is_usable(self):
        row = {
            "schedule_relationship": 0,
            "stop_schedule_relationship": 0,
            "arrival_time_utc": 1_784_678_400,
        }
        self.assertTrue(logger.is_usable_scheduled_arrival(row))
        for field, value in (
            ("schedule_relationship", 1),
            ("stop_schedule_relationship", 1),
            ("arrival_time_utc", ""),
        ):
            changed = dict(row)
            changed[field] = value
            self.assertFalse(logger.is_usable_scheduled_arrival(changed))


if __name__ == "__main__":
    unittest.main()
