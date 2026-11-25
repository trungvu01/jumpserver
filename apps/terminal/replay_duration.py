"""PAM V15 - Read audit replay duration without rendering video."""

import gzip
import json
import math


# Metadata reading safety limits, independent of MP4 export limits.
MAX_MANIFEST_BYTES = 4 * 1024 * 1024
MAX_CAST_SCAN_BYTES = 512 * 1024 * 1024
MAX_CAST_LINE_BYTES = 8 * 1024 * 1024
MAX_SAFE_MS = (1 << 53) - 1


def _seconds(value):
    if type(value) not in (int, float):
        raise ValueError("Invalid replay timestamp")

    if not 0 <= value <= MAX_SAFE_MS / 1000:
        raise ValueError("Invalid replay timestamp")

    if not math.isfinite(value):
        raise ValueError("Invalid replay timestamp")

    return float(value)


def read_ssh_duration(path):
    """Return duration in milliseconds from a gzip Asciicast v2 file."""

    scanned = 0

    with gzip.open(path, "rb") as stream:

        def read_line():
            nonlocal scanned

            line = stream.readline(MAX_CAST_LINE_BYTES + 1)
            scanned += len(line)

            if (
                len(line) > MAX_CAST_LINE_BYTES
                or scanned > MAX_CAST_SCAN_BYTES
            ):
                raise ValueError("Replay metadata scan limit exceeded")

            return line

        header = json.loads(read_line())

        if not isinstance(header, dict) or header.get("version") != 2:
            raise ValueError("Unsupported Asciicast format")

        last = 0.0
        event_count = 0

        while True:
            line = read_line()

            if not line:
                break

            if not line.strip():
                continue

            event = json.loads(line)

            if not isinstance(event, list) or len(event) < 3:
                raise ValueError("Invalid Asciicast event")

            stamp = _seconds(event[0])

            if stamp < last:
                raise ValueError("Invalid Asciicast timeline")

            last = stamp
            event_count += 1

        if not event_count:
            raise ValueError("Empty Asciicast recording")

        if header.get("duration") is not None:
            last = max(last, _seconds(header["duration"]))

        return round(last * 1000)


def read_rdp_duration(path, session_id):
    """Return the complete Guacamole manifest duration in milliseconds."""

    with open(path, "rb") as stream:
        payload = stream.read(MAX_MANIFEST_BYTES + 1)

    if len(payload) > MAX_MANIFEST_BYTES:
        raise ValueError("Manifest size limit exceeded")

    manifest = json.loads(payload)

    if (
        not isinstance(manifest, dict)
        or str(manifest.get("id")) != str(session_id)
        or manifest.get("protocol") != "rdp"
        or manifest.get("type") == "mp4"
    ):
        raise ValueError("Unsupported RDP manifest")

    parts = manifest.get("files")

    if not isinstance(parts, list) or not parts:
        raise ValueError("Empty RDP manifest")

    duration_ms = 0
    previous_end = None

    for index, part in enumerate(parts):
        if not isinstance(part, dict):
            raise ValueError("Invalid RDP part")

        expected_name = f"{session_id}.{index}.part.gz"

        if part.get("name") != expected_name:
            raise ValueError("Invalid RDP part name")

        start = part.get("start")
        end = part.get("end")
        duration = part.get("duration")

        if any(type(v) is not int for v in (start, end, duration)):
            raise ValueError("Invalid RDP timestamps")

        if not 0 <= start < end or duration <= 0:
            raise ValueError("Invalid RDP timeline")

        if abs(end - start - duration) > 1000:
            raise ValueError("Invalid RDP part duration")

        if previous_end is not None:
            if start < previous_end - 1000:
                raise ValueError("Invalid RDP part order")

            duration_ms += max(0, start - previous_end)

        duration_ms += duration
        previous_end = end

        if duration_ms > MAX_SAFE_MS:
            raise ValueError("Invalid RDP duration")

    return duration_ms


def read_session_duration(session):
    """Resolve a completed recording through JumpServer storage."""

    import stat
    from pathlib import Path

    from django.core.files.storage import default_storage
    from common.storage.replay import (
        ReplayStorageHandler,
        SessionPartReplayStorageHandler,
    )

    if not session.is_finished or not session.has_replay:
        return None

    if session.protocol == "ssh":
        handler = ReplayStorageHandler(session)
        local_path, _ = handler.get_file_path_url()

        if not local_path or not local_path.endswith(".cast.gz"):
            return None

    elif session.protocol == "rdp":
        handler = SessionPartReplayStorageHandler(session)
        manifest_name = f"{session.id}.replay.json"
        local_path, _ = handler.get_part_file_path_url(manifest_name)

        if not local_path:
            return None

    else:
        return None

    path = Path(default_storage.path(local_path))

    if not stat.S_ISREG(path.lstat().st_mode):
        raise ValueError("Replay source is not a regular file")

    if session.protocol == "ssh":
        return read_ssh_duration(path)

    return read_rdp_duration(path, session.id)
