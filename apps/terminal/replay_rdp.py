"""On-demand RDP Guacamole recording -> MP4.

Uses the authenticated Session part-storage abstraction for local/remote parts;
does not mutate source recordings or expose files through public media URLs.
Guacenc is run only in core-task, never in Core's API process. Every video
process is cancellable through the same V8 cache lease and subprocess watchdog.
"""
import gzip
import json
import logging
import math
import os
import re
import shutil
import stat
import time
import uuid
import zlib
from pathlib import Path

from django.core.cache import cache
from django.core.files.storage import default_storage

from .replay_mp4 import (
    JOB_TTL, FILE_TTL, JOB_BUDGET_SECONDS, MAX_DURATION, MAX_MP4,
    ConversionError, _assert_size, _probe_video, _run,
    cancel_key, ensure_client_active, get_job, lock_key, output_path,
    release_owner_lock, update_job,
)

logger = logging.getLogger(__name__)

RDP_FPS = 25
# Last-resort fallback only when a recording does not advertise its screen size.
RDP_FALLBACK_WIDTH = 1280
RDP_FALLBACK_HEIGHT = 720
# A larger frame may exceed memory budgets on the four-vCPU PAM lab. Reject
# rather than silently crop the desktop. Fine to tune after benchmarking.
RDP_MAX_WIDTH = 2560
RDP_MAX_HEIGHT = 1600
RDP_MAX_PIXELS = 2560 * 1600
RDP_SIZE_SCAN_BYTES = 16 * 1024 * 1024
RDP_GUAC_BITRATE_MIN = 6_000_000
RDP_GUAC_BITRATE_MAX = 12_000_000
RDP_X264_CRF = 17
RDP_X264_PRESET = 'superfast'  # lower CPU use than V9's 'veryfast'

# PAM MP4 V14 - Extended RDP recording limits.
MAX_PARTS = 720
MAX_MANIFEST = 4 * 1024 * 1024
MAX_PART_COMPRESSED = 256 * 1024 * 1024
MAX_PART_RAW = 384 * 1024 * 1024
MAX_ALL_RAW = 8 * 1024 * 1024 * 1024
MAX_INTERMEDIATE = 2 * 1024 * 1024 * 1024
PART_NAME = re.compile(r'^[0-9a-f-]{36}\.([0-9]+)\.part\.gz$', re.IGNORECASE)


def _bounded_regular_file(path, limit):
    """Open a locally retrieved part/manifest without following symlinks."""
    path = Path(path)
    stat_before = path.lstat()
    if not stat.S_ISREG(stat_before.st_mode) or not 0 < stat_before.st_size <= limit:
        raise ConversionError('resource_limit')
    with path.open('rb') as stream:
        actual = os.fstat(stream.fileno())
        if (not stat.S_ISREG(actual.st_mode) or actual.st_ino != stat_before.st_ino
                or actual.st_dev != stat_before.st_dev or actual.st_size > limit):
            raise ConversionError('invalid_recording')
        return stream.read(limit + 1)


def parse_rdp_manifest(payload, session_id):
    """Validate recorded part chronology before downloading and rendering."""
    try:
        data = json.loads(payload)
    except (TypeError, ValueError, UnicodeError):
        raise ConversionError('invalid_recording') from None
    if not isinstance(data, dict) or str(data.get('id')) != str(session_id):
        raise ConversionError('invalid_recording')
    if data.get('protocol') != 'rdp' or data.get('type') == 'mp4':
        raise ConversionError('unsupported_format')
    files = data.get('files')
    if not isinstance(files, list) or not 1 <= len(files) <= MAX_PARTS:
        raise ConversionError('invalid_recording')
    parts = []
    accumulated = 0.0
    previous_end = None
    total_uncompressed = 0
    for index, item in enumerate(files):
        if not isinstance(item, dict):
            raise ConversionError('invalid_recording')
        name = item.get('name')
        if (not isinstance(name, str)
                or name != f'{session_id}.{index}.part.gz'
                or not PART_NAME.fullmatch(name)):
            raise ConversionError('invalid_recording')
        start, end, duration, size = (item.get(k) for k in ('start', 'end', 'duration', 'size'))
        if any(type(v) is not int for v in (start, end, duration, size)):
            raise ConversionError('invalid_recording')
        if not (0 <= start < end and duration > 0 and size > 0):
            raise ConversionError('invalid_recording')
        if abs(end - start - duration) > 1000:
            raise ConversionError('invalid_timeline')
        if size > MAX_PART_RAW:
            raise ConversionError('resource_limit')
        total_uncompressed += size
        if total_uncompressed > MAX_ALL_RAW:
            raise ConversionError('resource_limit')
        if previous_end is not None:
            # A small negative difference can be caused by timestamp rounding.
            if start < previous_end - 1000:
                raise ConversionError('invalid_timeline')
            gap = max(0., (start - previous_end) / 1000.)
            parts[-1]['hold_after'] = gap
            accumulated += gap
        accumulated += duration / 1000.
        if accumulated > MAX_DURATION:
            raise ConversionError('duration_limit')
        parts.append({'name': name, 'duration': duration / 1000.,
                      'uncompressed_size': size, 'hold_after': 0.})
        previous_end = end
    return parts, accumulated


def _read_rdp_manifest(handler, session_id):
    manifest_name = f'{session_id}.replay.json'
    local_path, _ = handler.get_part_file_path_url(manifest_name)
    if not local_path:
        raise ConversionError('not_available')
    try:
        payload = _bounded_regular_file(default_storage.path(local_path), MAX_MANIFEST)
    except (FileNotFoundError, OSError):
        raise ConversionError('not_available') from None
    return parse_rdp_manifest(payload, session_id)


def _inflate_part(handler, part, dest, job_id, remaining_limit):
    """Stream gzip to disk under strict limits; never extract a TAR."""
    ensure_client_active(job_id)
    try:
        local_path, _ = handler.get_part_file_path_url(part['name'])
    except (FileNotFoundError, OSError):
        raise ConversionError('missing_part') from None
    if not local_path:
        raise ConversionError('missing_part')
    try:
        src_path = Path(default_storage.path(local_path))
        source_stat = src_path.lstat()
        if not stat.S_ISREG(source_stat.st_mode):
            raise ConversionError('invalid_recording')
        if source_stat.st_size > MAX_PART_COMPRESSED:
            raise ConversionError('resource_limit')
        count = 0
        with gzip.open(src_path, 'rb') as stream, open(dest, 'xb') as output:
            while True:
                ensure_client_active(job_id)
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                count += len(chunk)
                if count > MAX_PART_RAW or count > remaining_limit:
                    raise ConversionError('resource_limit')
                output.write(chunk)
    except ConversionError:
        raise
    except (gzip.BadGzipFile, EOFError, zlib.error, OSError):
        raise ConversionError('invalid_recording') from None
    if count != part['uncompressed_size']:
        raise ConversionError('invalid_recording')
    return count



def detect_guac_screen_size(path, *, scan_limit=RDP_SIZE_SCAN_BYTES):
    """Read primary-layer Guacamole `size` instructions without decoding images.

    Guacamole is a length-prefixed instruction protocol. Non-size element
    payloads are skipped rather than being searched as text, so text inside a
    JPEG/PNG/base64 blob cannot be confused with a display resize.

    Only the first scan_limit bytes are examined. If no primary-layer size was
    observed, None is returned and the older fallback dimensions remain in use.
    Larger-than-supported dimensions are rejected, never silently cropped.
    """
    chosen = None
    def finish():
        if chosen is None:
            return None
        # H.264/yuv420p needs even dimensions; pad at most one pixel.
        return (chosen[0] + (chosen[0] % 2), chosen[1] + (chosen[1] % 2))

    with open(path, 'rb') as stream:
        for _instruction in range(100_000):
            if stream.tell() >= scan_limit:
                break
            elements = []
            complete = False
            for element_index in range(256):
                digits = bytearray()
                for _digit in range(11):
                    ch = stream.read(1)
                    if ch == b'.':
                        break
                    if not ch or not (b'0' <= ch <= b'9'):
                        return finish()
                    digits.extend(ch)
                else:
                    return finish()
                if not digits:
                    return finish()
                length = int(digits)
                if length > 64 * 1024 * 1024 or stream.tell() + length + 1 > scan_limit:
                    return finish()
                # Only read opcode and the first size arguments. All other
                # potentially large payloads are skipped using seek().
                if element_index < 5 and length <= 32:
                    value = stream.read(length)
                else:
                    stream.seek(length, os.SEEK_CUR)
                    value = b''
                separator = stream.read(1)
                if separator not in (b',', b';'):
                    return finish()
                if element_index < 5:
                    elements.append(value)
                if separator == b';':
                    complete = True
                    break
            if not complete:
                break
            if len(elements) >= 4 and elements[0] == b'size' and elements[1] == b'0':
                try:
                    width = int(elements[2])
                    height = int(elements[3])
                except ValueError:
                    continue
                if width <= 0 or height <= 0:
                    continue
                if width > RDP_MAX_WIDTH or height > RDP_MAX_HEIGHT or width * height > RDP_MAX_PIXELS:
                    raise ConversionError('unsupported_size')
                # Ignore likely placeholder-size events, not desktop screens.
                if width < 320 or height < 200:
                    continue
                if chosen is None or width * height > chosen[0] * chosen[1]:
                    chosen = (width, height)
    return finish()


def rdp_intermediate_bitrate(width, height):
    """Provide enough first-pass bitrate for readable GUI text and icons."""
    return min(RDP_GUAC_BITRATE_MAX,
               max(RDP_GUAC_BITRATE_MIN, width * height * 4))


def render_rdp_parts(handler, parts, directory, mp4, job_id, deadline):
    """Render each part then align with its metadata timeline before concat.

    Per-part encode prevents guacenc's small extra-tail frame duration from
    accumulating across a long multi-part session. A gap between two part
    timestamps is represented by holding the preceding final frame.
    """
    total = len(parts) * 2
    update_job(job_id, stage='rendering', current=0, total=total)
    clips = []
    expected_frames = 0
    remaining_raw = MAX_ALL_RAW
    render_size = None
    for index, part in enumerate(parts):
        ensure_client_active(job_id)
        update_job(job_id, stage='loading', current=2 * index, total=total)
        raw = directory / f'part-{index:04d}.guac'
        raw_bytes = _inflate_part(handler, part, raw, job_id, remaining_raw)
        remaining_raw -= raw_bytes
        update_job(job_id, stage='rendering', current=2 * index, total=total)
        observed_size = detect_guac_screen_size(raw)
        if render_size is None:
            render_size = observed_size or (RDP_FALLBACK_WIDTH, RDP_FALLBACK_HEIGHT)
            if observed_size is None:
                logger.warning('Replay RDP size not found; using fallback %dx%d for job=%s',
                               render_size[0], render_size[1], job_id)
            else:
                logger.info('Replay RDP detected desktop %dx%d job=%s',
                            render_size[0], render_size[1], job_id)
        elif (observed_size and
              (observed_size[0] > render_size[0] or observed_size[1] > render_size[1])):
            # A multi-part session can resize. V10 does not silently crop a
            # larger later screen or concatenate clips of different sizes.
            raise ConversionError('unsupported_size')
        intermediate = Path(str(raw) + '.m4v')
        bitrate = rdp_intermediate_bitrate(*render_size)
        _run(['guacenc', '-s', f'{render_size[0]}x{render_size[1]}',
              '-r', str(bitrate), str(raw)],
             timeout=1800, failure_code='renderer_failed', deadline=deadline, job_id=job_id)
        _assert_size(intermediate, MAX_INTERMEDIATE, 'renderer_failed')
        raw.unlink(missing_ok=True)
        measured_duration, _ = _probe_video(intermediate, deadline, job_id=job_id)
        if measured_duration < max(.04, part['duration'] - max(5., part['duration'] * .03)):
            raise ConversionError('incomplete_video')
        if measured_duration > part['duration'] + max(8., part['duration'] * .03):
            raise ConversionError('invalid_timeline')
        ensure_client_active(job_id)
        update_job(job_id, stage='encoding', current=2 * index + 1, total=total)
        frames = max(1, math.ceil((part['duration'] + part['hold_after']) * RDP_FPS - 1e-9))
        expected_frames += frames
        clip = directory / f'clip-{index:04d}.mp4'
        # Guacenc can append ~2 seconds to a part's container timeline.
        # Trim to the *manifest* part duration before adding the pause between
        # parts, otherwise that encoder tail is mistaken for real session time.
        base_frames = max(1, math.ceil(part['duration'] * RDP_FPS - 1e-9))
        correction_pad = math.ceil(max(5., part['duration'] * .03) + 1)
        gap_pad = math.ceil(part['hold_after'] + 1)
        video_filter = (
            f'fps={RDP_FPS},'
            f'tpad=stop_mode=clone:stop_duration={correction_pad},'
            f'trim=end_frame={base_frames},'
            f'tpad=stop_mode=clone:stop_duration={gap_pad},'
            f'trim=end_frame={frames},setpts=PTS-STARTPTS'
        )
        _run(['ffmpeg', '-hide_banner', '-loglevel', 'error', '-y',
              '-i', str(intermediate), '-vf', video_filter,
              '-c:v', 'libx264', '-preset', RDP_X264_PRESET, '-crf', str(RDP_X264_CRF),
              '-threads', '2', '-filter_threads', '2',
              '-bf', '0', '-pix_fmt', 'yuv420p', '-r', str(RDP_FPS),
              '-video_track_timescale', str(RDP_FPS * 1024),
              '-frames:v', str(frames), '-movflags', '+faststart', str(clip)],
             timeout=900, failure_code='encoder_failed', deadline=deadline, job_id=job_id)
        _assert_size(clip, MAX_MP4, 'encoder_failed')
        intermediate.unlink(missing_ok=True)
        clips.append(clip)
        if sum(p.stat().st_size for p in clips) > MAX_MP4:
            raise ConversionError('resource_limit')
        update_job(job_id, stage='encoding', current=2 * index + 2, total=total)
        logger.info('Replay RDP progress job=%s part=%d/%d', job_id, index + 1, len(parts))

    ensure_client_active(job_id)
    update_job(job_id, stage='finalizing', current=total, total=total)
    manifest = directory / 'clips.txt'
    if len(clips) == 1:
        # One recording part (the common RDP case) needs no concat/remux.
        # MP4 moov metadata was already moved to the front while encoding.
        os.replace(clips[0], mp4)
    else:
        with open(manifest, 'w', encoding='utf-8') as out:
            for clip in clips:
                out.write("file '%s'\n" % clip)
        _run(['ffmpeg', '-hide_banner', '-loglevel', 'error', '-y',
              '-f', 'concat', '-safe', '0', '-i', str(manifest), '-c', 'copy',
              '-movflags', '+faststart', str(mp4)],
             timeout=900, failure_code='encoder_failed', deadline=deadline, job_id=job_id)
    _assert_size(mp4, MAX_MP4, 'encoder_failed')
    duration, packets = _probe_video(mp4, deadline, job_id=job_id)
    expected_duration = expected_frames / RDP_FPS
    if packets != expected_frames or abs(duration - expected_duration) > .5:
        logger.warning('Replay RDP validation failed job=%s packets=%d/%d duration=%.3f/%.3f',
                       job_id, packets, expected_frames, duration, expected_duration)
        raise ConversionError('incomplete_video')
    for clip in clips:
        clip.unlink(missing_ok=True)
    manifest.unlink(missing_ok=True)
    logger.info('Replay RDP validated job=%s parts=%d resolution=%dx%d duration=%.3f',
                job_id, len(parts), render_size[0], render_size[1], duration)


def convert_rdp_job(session_id, job_id):
    from orgs.utils import tmp_to_org, tmp_to_root_org
    from terminal.models import Session
    from common.storage.replay import SessionPartReplayStorageHandler

    entry = get_job(job_id)
    if not entry or str(entry.get('session_id')) != str(session_id):
        return
    if entry.get('status') == 'cancelled' or cache.get(cancel_key(job_id)):
        return
    deadline = time.monotonic() + JOB_BUDGET_SECONDS
    directory = output_path(job_id).parent
    update_job(job_id, status='processing', stage='loading', current=0, total=0)
    try:
        ensure_client_active(job_id)
        with tmp_to_root_org():
            session = Session.objects.get(pk=session_id)
        if (str(session.org_id) != entry['org_id'] or not session.is_finished
                or session.protocol != 'rdp' or not session.has_replay):
            raise ConversionError('not_available')
        with tmp_to_org(session.org):
            handler = SessionPartReplayStorageHandler(session)
            parts, declared_duration = _read_rdp_manifest(handler, str(session.id))
            logger.info('Replay RDP start job=%s parts=%d duration=%.1f',
                        job_id, len(parts), declared_duration)
            directory.mkdir(mode=0o700, parents=True, exist_ok=True)
            mp4 = output_path(job_id)
            render_rdp_parts(handler, parts, directory, mp4, job_id, deadline)
        ensure_client_active(job_id)
        os.chmod(mp4, 0o600)
        os.utime(directory, None)
        update_job(job_id, status='ready', stage='ready',
                   expires_at=time.time() + FILE_TTL)
        logger.info('Replay RDP completed job=%s mp4_bytes=%d', job_id, mp4.stat().st_size)
    except ConversionError as exc:
        shutil.rmtree(directory, ignore_errors=True)
        if exc.code == 'cancelled':
            cache.set(cancel_key(job_id), True, JOB_TTL)
            update_job(job_id, status='cancelled', stage='cancelled', error_code='cancelled')
            lock = lock_key(session_id, entry['user_id'])
            if cache.get(lock) == str(job_id):
                cache.delete(lock)
        else:
            logger.warning('Replay RDP failed job=%s code=%s', job_id, exc.code)
            update_job(job_id, status='failed', stage='failed', error_code=exc.code,
                       error='Unable to prepare this MP4 recording')
    except Exception:
        logger.exception('Replay RDP failed unexpectedly job=%s', job_id)
        shutil.rmtree(directory, ignore_errors=True)
        update_job(job_id, status='failed', stage='failed', error_code='unexpected_error',
                   error='Unable to prepare this MP4 recording')
    finally:
        release_owner_lock(job_id, entry['user_id'])
