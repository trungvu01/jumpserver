import gzip
import json
import tempfile
import unittest
import uuid
from pathlib import Path

from terminal.replay_duration import (
    read_ssh_duration,
    read_rdp_duration,
)


class ReplayDurationTests(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def make_cast(self, timestamps, header_duration=None):
        path = self.root / "test.cast.gz"

        header = {
            "version": 2,
            "width": 80,
            "height": 24,
        }

        if header_duration is not None:
            header["duration"] = header_duration

        with gzip.open(path, "wt", encoding="utf-8") as stream:
            stream.write(json.dumps(header) + "\n")

            for stamp in timestamps:
                stream.write(
                    json.dumps([stamp, "o", "Test"]) + "\n"
                )

        return path

    def test_ssh_duration(self):
        path = self.make_cast([0, 15, 312])
        self.assertEqual(read_ssh_duration(path), 312000)

    def test_ssh_header_duration(self):
        path = self.make_cast([0, 10], header_duration=20)
        self.assertEqual(read_ssh_duration(path), 20000)

    def test_ssh_subsecond(self):
        path = self.make_cast([0, 0.45])
        self.assertEqual(read_ssh_duration(path), 450)

    def test_ssh_over_24_hours(self):
        path = self.make_cast([0, 30 * 3600])
        self.assertEqual(read_ssh_duration(path), 108000000)

    def test_ssh_invalid_timeline(self):
        path = self.make_cast([0, 20, 10])

        with self.assertRaises(ValueError):
            read_ssh_duration(path)

    def test_ssh_empty_recording(self):
        path = self.make_cast([])

        with self.assertRaises(ValueError):
            read_ssh_duration(path)

    def make_manifest(self, timestamps):
        session_id = str(uuid.uuid4())
        path = self.root / "test.replay.json"

        files = []

        for index, (start, end) in enumerate(timestamps):
            files.append({
                "name": f"{session_id}.{index}.part.gz",
                "start": start,
                "end": end,
                "duration": end - start,
                "size": 1024,
            })

        path.write_text(json.dumps({
            "id": session_id,
            "protocol": "rdp",
            "files": files,
        }))

        return path, session_id

    def test_rdp_single_part(self):
        path, sid = self.make_manifest([(0, 60000)])
        self.assertEqual(read_rdp_duration(path, sid), 60000)

    def test_rdp_multiple_parts(self):
        path, sid = self.make_manifest([
            (0, 60000),
            (60000, 120000),
            (120000, 180000),
        ])

        self.assertEqual(read_rdp_duration(path, sid), 180000)

    def test_rdp_preserves_gap(self):
        path, sid = self.make_manifest([
            (0, 60000),
            (65000, 125000),
        ])

        self.assertEqual(read_rdp_duration(path, sid), 125000)

    def test_rdp_over_24_hours(self):
        path, sid = self.make_manifest([
            (0, 30 * 3600 * 1000),
        ])

        self.assertEqual(read_rdp_duration(path, sid), 108000000)

    def test_rdp_invalid_timeline(self):
        path, sid = self.make_manifest([
            (0, 60000),
            (30000, 90000),
        ])

        with self.assertRaises(ValueError):
            read_rdp_duration(path, sid)
