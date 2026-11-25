import json
import tempfile
import uuid
from pathlib import Path

from django.test import SimpleTestCase

from terminal.replay_mp4 import (
    MAX_DURATION,
    ConversionError,
    validate_cast,
)
from terminal.replay_rdp import parse_rdp_manifest


class ReplayMp4DurationLimitTests(SimpleTestCase):

    def test_configured_duration_limit(self):
        self.assertEqual(MAX_DURATION, 24 * 3600)

    def make_cast(self, directory, seconds):
        path = Path(directory) / 'test.cast'

        header = {
            'version': 2,
            'width': 80,
            'height': 24,
        }

        events = [
            [0, 'o', 'PAM replay test'],
            [seconds, 'o', 'Finished'],
        ]

        lines = [json.dumps(header)]
        lines.extend(json.dumps(event) for event in events)

        path.write_text(
            '\n'.join(lines) + '\n',
            encoding='utf-8',
        )
        return path

    def test_ssh_long_recordings(self):
        with tempfile.TemporaryDirectory() as directory:
            for hours in (8, 12, 24):
                with self.subTest(hours=hours):
                    seconds = hours * 3600
                    path = self.make_cast(directory, seconds)

                    duration, _, _ = validate_cast(path)

                    self.assertEqual(duration, seconds)

    def test_ssh_over_duration_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.make_cast(
                directory,
                MAX_DURATION + 1,
            )

            with self.assertRaises(ConversionError) as ctx:
                validate_cast(path)

            self.assertEqual(ctx.exception.code, 'duration_limit')

    def make_rdp_manifest(self, seconds):
        session_id = str(uuid.uuid4())
        files = []
        remaining = int(seconds * 1000)
        start = 0

        while remaining > 0:
            duration = min(600000, remaining)

            files.append({
                'name': f'{session_id}.{len(files)}.part.gz',
                'start': start,
                'end': start + duration,
                'duration': duration,
                'size': 1024,
            })

            start += duration
            remaining -= duration

        payload = {
            'id': session_id,
            'protocol': 'rdp',
            'files': files,
        }

        return json.dumps(payload), session_id

    def test_rdp_long_recordings(self):
        for hours in (8, 12, 24):
            with self.subTest(hours=hours):
                seconds = hours * 3600
                payload, session_id = self.make_rdp_manifest(seconds)

                _, duration = parse_rdp_manifest(
                    payload,
                    session_id,
                )

                self.assertEqual(duration, seconds)

    def test_rdp_over_duration_limit(self):
        payload, session_id = self.make_rdp_manifest(
            MAX_DURATION + 1
        )

        with self.assertRaises(ConversionError) as ctx:
            parse_rdp_manifest(payload, session_id)

        self.assertEqual(ctx.exception.code, 'duration_limit')
