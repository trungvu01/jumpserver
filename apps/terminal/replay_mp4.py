"""On-demand SSH asciicast -> MP4 export with bounded per-segment rendering.

`agg` 1.9.0 --select renders an excerpt while reconstructing terminal state
from the original recording. Rendering short excerpts avoids one enormous GIF.
Original recordings are never changed. Temporary files live in shared MEDIA_ROOT.
"""
import gzip
import json
import logging
import math
import re
import unicodedata
import os
import signal
import shutil
import subprocess
import time
import uuid
from pathlib import Path

from django.core.cache import cache
from django.core.files.storage import default_storage

logger = logging.getLogger(__name__)

# PAM MP4 V14 - Long Recording Support
# Up to 24h of recording, 8h processing budget and 8 GiB MP4.
JOB_TTL = 24 * 3600
# The browser renews this lease through authenticated status polling.
# A closed/crashed tab can no longer renew it; background browser throttling
# is tolerated for up to three minutes. Explicit cancel is usually faster.
CLIENT_LEASE_SECONDS = 180
FILE_TTL = 1800
MAX_COMPRESSED = 40 * 1024 * 1024
MAX_CAST = 64 * 1024 * 1024
MAX_DURATION = 24 * 3600.0
MAX_GIF = 128 * 1024 * 1024
MAX_MP4 = 8 * 1024 * 1024 * 1024
SEGMENT_SECONDS = 60
FPS = 12
JOB_BUDGET_SECONDS = 8 * 3600
EXPORT_DIR = 'replay_mp4_exports'


class ConversionError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


def job_key(job_id):
    return 'pam:replay-mp4:job:' + str(uuid.UUID(str(job_id)))


def lock_key(session_id, user_id):
    return 'pam:replay-mp4:lock:%s:%s' % (session_id, user_id)


def owner_lock_key(user_id):
    # Only one background video conversion per authenticated user across tabs.
    return 'pam:replay-mp4:owner:%s' % str(user_id)


def _safe_filename_part(value, max_length=72):
    text = unicodedata.normalize('NFKD', str(value or 'unknown'))
    text = ''.join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r'[\\/:*?"<>|\x00-\x1f\x7f]+', '-', text)
    text = re.sub(r'\s+', '-', text.strip())
    text = re.sub(r'_+', '-', text)
    text = re.sub(r'-+', '-', text).strip(' .-')
    return (text[:max_length].strip(' .-') or 'unknown')


def _account_label(value):
    # JumpServer saves snapshots as display(username); use actual login name.
    match = re.fullmatch(r'[^()]+\(([^()]*)\)', str(value or '').strip())
    return match.group(1).strip() if match else str(value or '').strip()


def export_filename(session):
    """Build download filename from trusted session DB metadata, never URL input."""
    from django.utils import timezone
    stamp = timezone.localtime(session.date_start).strftime('%y%m%d-%H%M%S')
    user = _safe_filename_part(_account_label(session.user), max_length=48)
    asset = _safe_filename_part(session.asset, max_length=85)
    account = _safe_filename_part(_account_label(session.account), max_length=48)
    protocol = _safe_filename_part(session.protocol, max_length=12)
    return f'{user}_{asset}_{account}_{protocol}_{stamp}.mp4'


def release_owner_lock(job_id, user_id):
    lock = owner_lock_key(user_id)
    if cache.get(lock) == str(job_id):
        cache.delete(lock)


def client_lease_key(job_id):
    return 'pam:replay-mp4:client:' + str(uuid.UUID(str(job_id)))


def cancel_key(job_id):
    return 'pam:replay-mp4:cancel:' + str(uuid.UUID(str(job_id)))


def touch_job(job_id):
    """Renew the worker lease only for an authenticated, active consumer."""
    job = get_job(job_id)
    if job and job.get('status') in ('queued', 'processing') and not cache.get(cancel_key(job_id)):
        cache.set(client_lease_key(job_id), True, CLIENT_LEASE_SECONDS)


def client_active(job_id):
    return bool(cache.get(client_lease_key(job_id)))


def ensure_client_active(job_id):
    """Called by worker during render/encode; does not trust browser claims."""
    job = get_job(job_id)
    if (not job or job.get('status') == 'cancelled' or
            cache.get(cancel_key(job_id)) or not client_active(job_id)):
        raise ConversionError('cancelled')


def cancel_job(job_id, session_id, user_id):
    """Ask worker to stop; do not release owner lock until it actually stops."""
    job = get_job(job_id)
    if not job:
        return 'not_found'
    if job.get('status') not in ('queued', 'processing'):
        return job.get('status', 'not_found')
    cache.set(cancel_key(job_id), True, JOB_TTL)
    cache.delete(client_lease_key(job_id))
    if job['status'] == 'queued':
        update_job(job_id, status='cancelled', stage='cancelled', error_code='cancelled')
        # queued: there is no converter process yet.
        lock = lock_key(session_id, user_id)
        if cache.get(lock) == str(job_id):
            cache.delete(lock)
        release_owner_lock(job_id, user_id)
        return 'cancelled'
    # The worker observes the cancellation marker, terminates its subprocess
    # group, marks the job cancelled and only then releases the global lock.
    return 'cancelling'


def output_path(job_id):
    job_id = uuid.UUID(str(job_id)).hex
    return Path(default_storage.base_location) / EXPORT_DIR / job_id / 'recording.mp4'


def get_job(job_id):
    try:
        return cache.get(job_key(job_id))
    except (TypeError, ValueError, AttributeError):
        return None


def update_job(job_id, **changes):
    key = job_key(job_id)
    job = cache.get(key)
    if job is not None:
        if changes.get('status') != 'cancelled' and (
                job.get('status') == 'cancelled' or cache.get(cancel_key(job_id))):
            return
        cache.set(key, {**job, **changes}, JOB_TTL)


def validate_cast(src, job_id=None):
    """Return the full playback duration without exposing terminal contents."""
    try:
        with open(src, encoding='utf-8') as f:
            header = json.loads(f.readline())
            if not isinstance(header, dict) or header.get('version') != 2:
                raise ConversionError('unsupported_format')
            cols, rows = header.get('width'), header.get('height')
            if not (type(cols) is int and 1 <= cols <= 250 and
                    type(rows) is int and 1 <= rows <= 100):
                raise ConversionError('unsupported_size')
            last_time = 0.0
            for row_number, line in enumerate(f):
                if job_id and row_number % 1024 == 0:
                    ensure_client_active(job_id)
                if not line.strip():
                    continue
                event = json.loads(line)
                if not isinstance(event, list) or len(event) < 3:
                    raise ConversionError('invalid_recording')
                stamp = float(event[0])
                if not math.isfinite(stamp) or stamp < last_time or stamp < 0:
                    raise ConversionError('invalid_recording')
                if stamp > MAX_DURATION:
                    raise ConversionError('duration_limit')
                last_time = stamp
            header_duration = header.get('duration')
            if header_duration is not None:
                header_duration = float(header_duration)
                if not math.isfinite(header_duration) or header_duration < 0:
                    raise ConversionError('invalid_recording')
                last_time = max(last_time, header_duration)
            if last_time > MAX_DURATION:
                raise ConversionError('duration_limit')
            return max(0.1, last_time), cols, rows
    except ConversionError:
        raise
    except (OSError, UnicodeError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        raise ConversionError('invalid_recording') from None


def _terminate_process_group(proc):
    """Stop FFmpeg/agg and any subprocesses when a tab is closed."""
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.communicate(timeout=3)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.communicate()


def _run(cmd, *, timeout, failure_code, deadline, job_id=None):
    """Bounded subprocess with cooperative cancellation even mid-segment."""
    if job_id:
        ensure_client_active(job_id)
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ConversionError('processing_timeout')
    subdeadline = time.monotonic() + min(timeout, remaining)
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL, start_new_session=True)
    except FileNotFoundError as exc:
        logger.error('MP4 converter executable missing: %s', cmd[0])
        raise ConversionError('converter_missing') from exc
    try:
        while True:
            if job_id:
                ensure_client_active(job_id)
            left = min(subdeadline, deadline) - time.monotonic()
            if left <= 0:
                raise ConversionError('processing_timeout')
            try:
                proc.communicate(timeout=min(1.0, left))
                break
            except subprocess.TimeoutExpired:
                continue
        if job_id:
            ensure_client_active(job_id)
        if proc.returncode:
            logger.warning('MP4 subprocess failed: executable=%s returncode=%s',
                           cmd[0], proc.returncode)
            raise ConversionError(failure_code)
    except BaseException:
        _terminate_process_group(proc)
        raise


def _assert_size(path, limit, error_code):
    if not path.is_file() or path.stat().st_size == 0:
        raise ConversionError(error_code)
    if path.stat().st_size > limit:
        raise ConversionError('resource_limit')


def _probe_video(mp4, deadline, job_id=None):
    """Inspect only the encoded video stream, not the session wall clock."""
    if job_id:
        ensure_client_active(job_id)
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ConversionError('processing_timeout')
    try:
        probe = subprocess.run(
            ['ffprobe', '-v', 'error', '-select_streams', 'v:0',
             '-count_packets', '-show_entries',
             'stream=nb_read_packets:format=duration', '-of', 'json', str(mp4)],
            check=True, capture_output=True, text=True, timeout=min(60, remaining))
        if job_id:
            ensure_client_active(job_id)
        info = json.loads(probe.stdout)
        stream = info['streams'][0]
        frame_packets = int(stream['nb_read_packets'])
        duration = float(info['format']['duration'])
        if frame_packets <= 0 or not math.isfinite(duration) or duration <= 0:
            raise ConversionError('incomplete_video')
        return duration, frame_packets
    except subprocess.TimeoutExpired as exc:
        raise ConversionError('processing_timeout') from exc
    except ConversionError:
        raise
    except (ValueError, TypeError, KeyError, IndexError, subprocess.SubprocessError,
            OSError, json.JSONDecodeError) as exc:
        logger.warning('Replay MP4 ffprobe validation failed: %s', type(exc).__name__)
        raise ConversionError('incomplete_video') from None


def _render_in_segments(cast, directory, mp4, duration, job_id, deadline, cols=None, rows=None):
    total = max(1, math.ceil(duration / SEGMENT_SECONDS))
    logger.info('Replay MP4 render start: job=%s duration=%.1fs segments=%s', job_id, duration, total)
    clips = []
    expected_frames = 0
    # Do not change the original recording's idle timing. The segmented
    # renderer reconstructs historical state from the original cast.
    for index in range(total):
        ensure_client_active(job_id)
        if time.monotonic() >= deadline:
            raise ConversionError('processing_timeout')
        start = index * SEGMENT_SECONDS
        end = min((index + 1) * SEGMENT_SECONDS, duration)
        length = end - start
        gif = directory / 'segment.gif'
        clip = directory / ('clip-%04d.mp4' % index)
        update_job(job_id, stage='rendering', current=index, total=total)
        _run(['agg', '--no-loop', '--fps-cap', str(FPS),
              *( ['--cols', str(cols), '--rows', str(rows)] if cols and rows else [] ),
              '--idle-time-limit', str(int(MAX_DURATION)),
              '--last-frame-duration', '0.1',
              '--select', '%s..%s' % (start, end),
              str(cast), str(gif)],
             timeout=900, failure_code='renderer_failed', deadline=deadline, job_id=job_id)
        _assert_size(gif, MAX_GIF, 'renderer_failed')
        update_job(job_id, stage='encoding', current=index, total=total)
        # Pad idle tails using the last terminal frame, then trim to the exact
        # original timeline interval. All clips have identical H.264 settings.
        # Each segment has an exact number of frames, independent of GIF delay
        # quantization or the FFmpeg version. With a fixed FPS this prevents
        # small per-segment rounding errors accumulating across long sessions.
        target_frames = max(1, math.ceil(length * FPS - 1e-9))
        expected_frames += target_frames
        vf = ('scale=trunc(iw/2)*2:trunc(ih/2)*2,'
              'fps=%d,tpad=stop_mode=clone:stop_duration=%d,'
              'trim=end_frame=%d,setpts=PTS-STARTPTS' %
              (FPS, SEGMENT_SECONDS + 5, target_frames))
        _run(['ffmpeg', '-hide_banner', '-loglevel', 'error', '-y',
              '-i', str(gif), '-vf', vf,
              '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '23',
              '-threads', '2', '-bf', '0', '-pix_fmt', 'yuv420p',
              '-r', str(FPS), '-video_track_timescale', str(FPS * 1024),
              '-frames:v', str(target_frames), str(clip)],
             timeout=300, failure_code='encoder_failed', deadline=deadline, job_id=job_id)
        _assert_size(clip, MAX_MP4, 'encoder_failed')
        gif.unlink(missing_ok=True)
        clips.append(clip)
        if sum(p.stat().st_size for p in clips) > MAX_MP4:
            raise ConversionError('resource_limit')
        update_job(job_id, stage='rendering', current=index + 1, total=total)
        logger.info('Replay MP4 progress: job=%s segment=%d/%d', job_id, index + 1, total)

    ensure_client_active(job_id)
    update_job(job_id, stage='finalizing', current=total, total=total)
    # Internal paths are UUID-derived and never sourced from a user filename.
    manifest = directory / 'segments.txt'
    with open(manifest, 'w', encoding='utf-8') as stream:
        for clip in clips:
            stream.write("file '%s'\n" % clip)
    _run(['ffmpeg', '-hide_banner', '-loglevel', 'error', '-y',
          '-f', 'concat', '-safe', '0', '-i', str(manifest), '-c', 'copy',
          '-movflags', '+faststart', str(mp4)],
         timeout=600, failure_code='encoder_failed', deadline=deadline, job_id=job_id)
    _assert_size(mp4, MAX_MP4, 'encoder_failed')
    # The Session model's wall-clock duration is NOT the asciicast timeline.
    # Validate the actual encoded frames first. This catches truncated exports
    # while avoiding spurious failures caused by container duration rounding.
    encoded_duration, encoded_frames = _probe_video(mp4, deadline, job_id=job_id)
    expected_duration = expected_frames / FPS
    if encoded_frames != expected_frames:
        logger.warning('Replay MP4 frame mismatch job=%s expected=%d actual=%d',
                       job_id, expected_frames, encoded_frames)
        raise ConversionError('incomplete_video')
    # A few frames of timestamp rounding are harmless, but grossly distorted
    # video presentation duration must still be rejected.
    allowed_drift = max(0.5, 3 / FPS)
    if abs(encoded_duration - expected_duration) > allowed_drift:
        logger.warning('Replay MP4 timeline mismatch job=%s expected=%.3f actual=%.3f',
                       job_id, expected_duration, encoded_duration)
        raise ConversionError('incomplete_video')
    logger.info('Replay MP4 validated: job=%s frames=%d timeline=%.3fs cast=%.3fs',
                job_id, encoded_frames, encoded_duration, duration)
    ensure_client_active(job_id)
    for clip in clips:
        clip.unlink(missing_ok=True)
    manifest.unlink(missing_ok=True)


def convert_ssh_job(session_id, job_id):
    """Called only by Celery; not from an HTTP request."""
    from orgs.utils import tmp_to_org, tmp_to_root_org
    from terminal.models import Session
    from common.storage.replay import ReplayStorageHandler

    entry = get_job(job_id)
    if not entry or str(entry['session_id']) != str(session_id):
        return
    if entry.get('status') == 'cancelled' or cache.get(cancel_key(job_id)):
        return
    deadline = time.monotonic() + JOB_BUDGET_SECONDS
    update_job(job_id, status='processing', stage='loading', current=0, total=0)
    directory = output_path(job_id).parent
    try:
        ensure_client_active(job_id)
        with tmp_to_root_org():
            session = Session.objects.get(pk=session_id)
        if str(session.org_id) != entry['org_id'] or not session.is_finished or session.protocol != 'ssh':
            raise ConversionError('not_available')
        with tmp_to_org(session.org):
            local_path, _url = ReplayStorageHandler(session).get_file_path_url()
        if not local_path or not local_path.endswith('.cast.gz'):
            raise ConversionError('not_available')
        raw_path = Path(default_storage.path(local_path))
        if raw_path.stat().st_size > MAX_COMPRESSED:
            raise ConversionError('resource_limit')

        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        cast = directory / 'source.cast'
        mp4 = output_path(job_id)
        with gzip.open(raw_path, 'rb') as src, open(cast, 'wb') as dst:
            size = 0
            while True:
                ensure_client_active(job_id)
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_CAST:
                    raise ConversionError('resource_limit')
                dst.write(chunk)
        duration, cols, rows = validate_cast(cast, job_id=job_id)
        update_job(job_id, stage='rendering', current=0,
                   total=max(1, math.ceil(duration / SEGMENT_SECONDS)))
        _render_in_segments(cast, directory, mp4, duration, job_id, deadline, cols, rows)
        ensure_client_active(job_id)
        os.chmod(mp4, 0o600)
        cast.unlink(missing_ok=True)
        os.utime(directory, None)
        update_job(job_id, status='ready', stage='ready',
                   expires_at=time.time() + FILE_TTL)
        logger.info('Replay MP4 completed: job=%s output_bytes=%d', job_id, mp4.stat().st_size)
    except ConversionError as exc:
        shutil.rmtree(directory, ignore_errors=True)
        if exc.code == 'cancelled':
            logger.info('Replay MP4 canceled: job=%s (browser closed or client lease expired)', job_id)
            cache.set(cancel_key(job_id), True, JOB_TTL)
            update_job(job_id, status='cancelled', stage='cancelled', error_code='cancelled')
            lock = lock_key(session_id, entry['user_id'])
            if cache.get(lock) == str(job_id):
                cache.delete(lock)
        else:
            logger.warning('Replay MP4 job failed: job=%s code=%s', job_id, exc.code)
            update_job(job_id, status='failed', stage='failed', error_code=exc.code,
                       error='Unable to prepare this MP4 recording')
    except Exception:
        logger.exception('Replay MP4 job failed unexpectedly: %s', job_id)
        shutil.rmtree(directory, ignore_errors=True)
        update_job(job_id, status='failed', stage='failed', error_code='unexpected_error',
                   error='Unable to prepare this MP4 recording')
    finally:
        release_owner_lock(job_id, entry['user_id'])


def cleanup_export_files():
    root = Path(default_storage.base_location) / EXPORT_DIR
    if not root.is_dir():
        return
    for directory in root.iterdir():
        if not directory.is_dir():
            continue
        try:
            job_id = str(uuid.UUID(hex=directory.name))
            job = get_job(job_id)
            # Never erase an active render even when it takes longer than TTL.
            if job and job.get('status') in ('queued', 'processing') and client_active(job_id):
                continue
            if job and job.get('status') == 'ready' and job.get('expires_at', 0) > time.time():
                continue
            if time.time() - directory.stat().st_mtime > FILE_TTL + 900:
                shutil.rmtree(directory)
        except (ValueError, OSError):
            continue
