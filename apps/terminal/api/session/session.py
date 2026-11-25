# -*- coding: utf-8 -*-
#
import os
import tarfile

from django.conf import settings
from django.core.cache import cache
from django.core.files.storage import default_storage
from django.db.models import F
from django.http import FileResponse, HttpResponse
from django.shortcuts import get_object_or_404, reverse
from django.utils import translation
from django.utils.encoding import escape_uri_path
from django.utils.translation import gettext, gettext as _, gettext_lazy, gettext_noop
from django_filters import rest_framework as filters
from rest_framework import generics
from rest_framework import viewsets, views
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED, HTTP_400_BAD_REQUEST

from audits.const import ActionChoices
from audits.handler import create_or_update_operate_log
from audits.utils import record_operate_log_and_activity_log
from common.api import AsyncApiMixin, ReportExportMixin
from common.const.http import GET, POST
from common.drf.filters import BaseFilterSet
from common.drf.filters import DatetimeRangeFilterBackend
from common.drf.renders import PassthroughRenderer
from common.drf.throttling import FileTransferThrottle
from common.permissions import IsServiceAccount
from common.storage.replay import ReplayStorageHandler, SessionPartReplayStorageHandler
from common.utils import data_to_json, is_uuid, i18n_fmt
from common.utils import get_logger, get_object_or_none
from orgs.mixins.api import OrgBulkModelViewSet
from orgs.utils import tmp_to_root_org, tmp_to_org
from rbac.permissions import RBACPermission
from terminal import serializers
from terminal.const import TerminalType
from terminal.models import Session
from terminal.permissions import IsSessionAssignee
from terminal.reporting import SessionReportExporter
from terminal.session_lifecycle import lifecycle_events_map, reasons_map
from terminal.utils import is_session_approver
from users.models import User

__all__ = [
    'SessionViewSet', 'SessionReplayViewSet',
    'SessionJoinValidateAPI', 'MySessionAPIView'
]

logger = get_logger(__name__)

REPLAY_OP = gettext_noop('User %s %s session %s replay')


class MySessionAPIView(generics.ListAPIView):
    permission_classes = (IsAuthenticated,)
    serializer_class = serializers.SessionSerializer

    def get_queryset(self):
        user = self.request.user
        qs = Session.objects.filter(user_id=user.id)
        return qs


class SessionFilterSet(BaseFilterSet):
    user = filters.CharFilter(
        lookup_expr='iexact', label=gettext_lazy('User name')
    )
    asset = filters.CharFilter(
        lookup_expr='iexact', label=gettext_lazy('Asset name')
    )
    account = filters.CharFilter(
        lookup_expr='iexact', label=gettext_lazy('Account name')
    )
    terminal = filters.CharFilter(
        method='filter_terminal', label=gettext_lazy('Terminal name or ID')
    )

    class Meta:
        model = Session
        fields = [
            "user", "user_id", "asset", "asset_id", "account", "remote_addr",
            "protocol", "is_finished", 'login_from', 'terminal'
        ]

    @staticmethod
    def filter_terminal(queryset, name, value):
        if is_uuid(value):
            return queryset.filter(terminal__id=value)
        else:
            return queryset.filter(terminal__name=value)


# PAM_MP4_V7: separate bounded buckets for export, status polling, and cancel.
# Other file-download endpoints retain JumpServer's original FileTransferThrottle.
from rest_framework.throttling import UserRateThrottle

# PAM_MP4_V8: burst protection keeps any short lockout within a minute;
# a separate per-user sustained bucket still protects Core and the job queue.
class ReplayMp4CreateBurstThrottle(UserRateThrottle):
    scope = 'pam_mp4_create_burst'
    rate = '30/min'

class ReplayMp4CreateThrottle(UserRateThrottle):
    scope = 'pam_mp4_create'
    rate = '600/hour'

class ReplayMp4StatusThrottle(UserRateThrottle):
    scope = 'pam_mp4_status'
    rate = '2400/hour'

class ReplayMp4CancelThrottle(UserRateThrottle):
    scope = 'pam_mp4_cancel'
    rate = '120/hour'


class SessionViewSet(ReportExportMixin, OrgBulkModelViewSet):
    model = Session
    report_exporter_class = SessionReportExporter
    serializer_classes = {
        'default': serializers.SessionSerializer,
        'display': serializers.SessionDisplaySerializer,
        'lifecycle_log': serializers.SessionLifecycleLogSerializer,
    }
    search_fields = [
        "user", "asset", "account", "remote_addr",
        "protocol", "is_finished", 'login_from',
    ]
    filterset_class = SessionFilterSet
    date_range_filter_fields = [
        ('date_start', ('date_from', 'date_to'))
    ]
    extra_filter_backends = [DatetimeRangeFilterBackend]
    rbac_perms = {
        'download': ['terminal.download_sessionreplay'],
        'mp4_create': ['terminal.download_sessionreplay'],
        'mp4_status': ['terminal.download_sessionreplay'],
        'mp4_download': ['terminal.download_sessionreplay'],
        'mp4_cancel': ['terminal.download_sessionreplay'],
    }
    permission_classes = [RBACPermission]

    def get_permissions(self):
        if self.action == 'retrieve':
            self.permission_classes = [RBACPermission | IsSessionAssignee]
        return super().get_permissions()

    def retrieve(self, request, *args, **kwargs):
        instance = self.get_object()
        serializer = self.get_serializer(instance)
        with translation.override('en'):
            create_or_update_operate_log(
                ActionChoices.view, self.model._meta.verbose_name,
                force=True, resource=instance,
            )
        return Response(serializer.data)

    @staticmethod
    def prepare_offline_file(session, local_path):
        replay_path = default_storage.path(local_path)
        dir_path = os.path.dirname(replay_path)
        replay_filename = os.path.basename(replay_path)
        meta_filename = '{}.json'.format(session.id)
        offline_filename = '{}.tar'.format(session.id)
        offline_path = os.path.join(dir_path, offline_filename)

        meta_path = os.path.join(dir_path, meta_filename)
        with open(meta_path, 'wt') as f:
            serializer = serializers.SessionDisplaySerializer(session)
            data = data_to_json(serializer.data)
            f.write(data)

        with tarfile.open(offline_path, 'w') as f:
            f.add(replay_path, arcname=replay_filename)
            f.add(meta_path, arcname=meta_filename)
        return offline_path

    @action(methods=[POST], detail=True, url_path='replay/mp4',
            throttle_classes=[ReplayMp4CreateBurstThrottle, ReplayMp4CreateThrottle])
    def mp4_create(self, request, *args, **kwargs):
        """Start an SSH or RDP MP4 conversion; legacy TAR endpoint is unchanged."""
        import uuid
        import time
        from terminal.replay_mp4 import JOB_TTL, job_key, lock_key, get_job, touch_job, owner_lock_key
        session = self.get_object()  # org scope + object permissions
        if session.protocol not in ('ssh', 'rdp') or not session.is_finished:
            return Response({'detail': 'MP4 export requires completed SSH or RDP session'}, status=415)
        if session.protocol == 'rdp' and not session.has_replay:
            return Response({'detail': 'RDP replay is not available'}, status=404)
        # Gate all HTTP export requests, including cached files, across tabs.
        owner_lock = owner_lock_key(request.user.id)
        owner_job_id = cache.get(owner_lock)
        if owner_job_id:
            owner_job = get_job(owner_job_id)
            if owner_job and owner_job.get('status') in ('queued', 'processing'):
                if owner_job.get('session_id') != str(session.id):
                    return Response({'detail': 'Another recording export is in progress'}, status=409)
            else:
                cache.delete(owner_lock)
        lock = lock_key(session.id, request.user.id)
        existing_id = cache.get(lock)
        if existing_id:
            existing = get_job(existing_id)
            # Cached RDP output from V9 is still playable but has the older
            # quality profile. Re-encode it ONCE on the first V10 request.
            stale_rdp_profile = (session.protocol == 'rdp' and existing
                                 and existing.get('status') == 'ready'
                                 and existing.get('rdp_profile', 9) != 10)
            if (existing and not stale_rdp_profile
                    and existing.get('status') in ('queued', 'processing', 'ready')
                    and (existing.get('status') != 'ready' or
                         existing.get('expires_at', 0) > time.time())):
                touch_job(existing_id)
                return Response({'job_id': existing_id, 'status': existing['status']}, status=202)
            cache.delete(lock)
        job_id = str(uuid.uuid4())
        if not cache.add(owner_lock, job_id, JOB_TTL):
            return Response({'detail': 'Another recording export is in progress'}, status=409)
        if not cache.add(lock, job_id, JOB_TTL):
            if cache.get(owner_lock) == job_id:
                cache.delete(owner_lock)
            return Response({'detail': 'An export for this session is already in progress'}, status=409)
        cache.set(job_key(job_id), {
            'session_id': str(session.id),
            'org_id': str(session.org_id),
            'user_id': str(request.user.id),
            'status': 'queued',
            'rdp_profile': 10 if session.protocol == 'rdp' else None,
        }, JOB_TTL)
        touch_job(job_id)
        try:
            if session.protocol == 'rdp':
                from terminal.tasks import export_rdp_replay_mp4
                export_rdp_replay_mp4.delay(str(session.id), job_id)
            else:
                from terminal.tasks import export_ssh_replay_mp4
                export_ssh_replay_mp4.delay(str(session.id), job_id)
        except Exception:
            cache.delete(job_key(job_id))
            cache.delete(lock)
            if cache.get(owner_lock) == job_id:
                cache.delete(owner_lock)
            logger.exception('Unable to queue MP4 export')
            return Response({'detail': 'Video export worker is unavailable'}, status=503)
        return Response({'job_id': job_id, 'status': 'queued'}, status=202)

    def _mp4_job_for_request(self, request, session):
        from terminal.replay_mp4 import get_job
        job_id = request.query_params.get('job_id')
        job = get_job(job_id)
        if (not job or job['session_id'] != str(session.id) or
                job['org_id'] != str(session.org_id) or
                job['user_id'] != str(request.user.id)):
            return None
        return job

    @action(methods=[GET], detail=True, url_path='replay/mp4/status', throttle_classes=[ReplayMp4StatusThrottle])
    def mp4_status(self, request, *args, **kwargs):
        import time
        from terminal.replay_mp4 import export_filename
        session = self.get_object()
        job = self._mp4_job_for_request(request, session)
        if not job:
            return Response({'detail': 'Export job not found'}, status=404)
        if job.get('status') == 'ready' and job.get('expires_at', 0) <= time.time():
            return Response({'detail': 'Prepared MP4 has expired. Please download again.'}, status=410)
        if job.get('status') in ('queued', 'processing'):
            from terminal.replay_mp4 import touch_job
            touch_job(request.query_params.get('job_id'))
        return Response({
            'status': job['status'],
            'stage': job.get('stage', job['status']),
            'current': job.get('current', 0),
            'total': job.get('total', 0),
            'error_code': job.get('error_code', ''),
            'error': job.get('error', ''),
            'filename': export_filename(session),
        })

    @action(methods=[POST], detail=True, url_path='replay/mp4/cancel',
            throttle_classes=[ReplayMp4CancelThrottle])
    def mp4_cancel(self, request, *args, **kwargs):
        """Cancel a running export for the same authenticated user and org."""
        from terminal.replay_mp4 import cancel_job
        session = self.get_object()
        job = self._mp4_job_for_request(request, session)
        if not job:
            return Response({'detail': 'Export job not found'}, status=404)
        status = cancel_job(request.query_params.get('job_id'),
                            session.id, request.user.id)
        return Response({'status': status})

    @action(methods=[GET], detail=True, url_path='replay/mp4/download',
            throttle_classes=[FileTransferThrottle], renderer_classes=(PassthroughRenderer,))
    def mp4_download(self, request, *args, **kwargs):
        import time
        from terminal.replay_mp4 import output_path, export_filename
        session = self.get_object()
        job = self._mp4_job_for_request(request, session)
        if not job:
            return Response({'detail': 'Export job not found'}, status=404)
        if job['status'] != 'ready':
            return Response({'detail': 'MP4 is not ready'}, status=409)
        if job.get('expires_at', 0) <= time.time():
            return Response({'detail': 'Prepared MP4 expired'}, status=410)
        path = output_path(request.query_params.get('job_id'))
        if not path.is_file():
            return Response({'detail': 'Prepared MP4 no longer exists'}, status=404)
        # Stream only through the authenticated API. No public media URL is exposed.
        # For a production-scale deployment, X-Accel can replace this after
        # verifying the Nginx private alias, volume, and file permissions.
        response = FileResponse(open(path, 'rb'), as_attachment=True,
                                filename=export_filename(session), content_type='video/mp4')
        response['Cache-Control'] = 'private, no-store'
        detail = i18n_fmt(REPLAY_OP, request.user, _('Download'), str(session))
        record_operate_log_and_activity_log(
            [session.asset_id], ActionChoices.download, detail, Session,
            resource=session, resource_type=_('Session replay'))
        return response

    @action(methods=[GET], detail=True, renderer_classes=(PassthroughRenderer,), url_path='replay/download',
            throttle_classes=[FileTransferThrottle],
            url_name='replay-download')
    def download(self, request, *args, **kwargs):
        session = self.get_object()
        return self.build_replay_download_response(session, request)

    @classmethod
    def build_replay_download_response(cls, session, request, before_send=None):
        storage = ReplayStorageHandler(session)
        local_path, url = storage.get_file_path_url()
        if local_path is None:
            # url => error message
            return Response({'error': url}, status=404)

        # 如果获取的录像文件类型是 .replay.json 则使用 part 的方式下载
        if url.endswith('.replay.json'):
            # part 的方式录像存储, 通过 part_storage 的方式下载
            part_storage = SessionPartReplayStorageHandler(session)
            offline_abs_path = part_storage.prepare_offline_tar_file()
        else:
            offline_abs_path = cls.prepare_offline_file(session, local_path)
        if before_send:
            before_send()
        if settings.DEBUG_DEV:
            response = FileResponse(open(offline_abs_path, 'rb'), as_attachment=True, filename=f'{session.id}.tar')
        else:
            media_root = default_storage.base_location
            relative_path = os.path.relpath(offline_abs_path, media_root)
            internal_url = os.path.join(settings.PRIVATE_STORAGE_INTERNAL_URL, relative_path)
            internal_url = escape_uri_path(internal_url)
            response = HttpResponse()
            response['X-Accel-Redirect'] = internal_url
        response['Content-Type'] = 'application/octet-stream'
        filename = escape_uri_path('{}.tar'.format(session.id))
        disposition = "attachment; filename*=UTF-8''{}".format(filename)
        response["Content-Disposition"] = disposition
        response['Cache-Control'] = 'no-store'
        detail = i18n_fmt(
            REPLAY_OP, request.user, _('Download'), str(session)
        )
        record_operate_log_and_activity_log(
            [session.asset_id], ActionChoices.download, detail, Session,
            resource=session, resource_type=_('Session replay')
        )
        return response

    @action(methods=[GET], detail=False, permission_classes=[IsAuthenticated], url_path='online-info', )
    def online_info(self, request, *args, **kwargs):
        if not settings.VIEW_ASSET_ONLINE_SESSION_INFO:
            return self.permission_denied(request, "view asset online session info disabled")
        asset = self.request.query_params.get('asset_id')
        account = self.request.query_params.get('account')
        if asset is None or account is None:
            return Response({'count': None})

        queryset = Session.objects.filter(is_finished=False) \
            .filter(asset_id=asset) \
            .filter(protocol='rdp')  # 当前只统计 rdp 协议的会话
        if '(' in account and ')' in account:
            queryset = queryset.filter(account=account)
        else:
            queryset = queryset.filter(account__endswith='({})'.format(account))
        count = queryset.count()
        return Response({'count': count})

    @action(methods=[POST], detail=True, permission_classes=[IsServiceAccount], url_path='lifecycle_log',
            url_name='lifecycle_log')
    def lifecycle_log(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        validated_data = serializer.validated_data
        event = validated_data.pop('event', None)
        event_class = lifecycle_events_map.get(event, None)
        if not event_class:
            return Response({'msg': f'event_name {event} invalid'}, status=400)
        session = self.get_object()
        reason = validated_data.pop('reason', None)
        reason = reasons_map.get(reason, reason)
        event_obj = event_class(session, reason, **validated_data)
        activity_log = event_obj.create_activity_log()
        return Response({'msg': 'ok', 'id': activity_log.id})

    def get_queryset(self):
        queryset = super().get_queryset()
        if self.request.method in ('GET',):
            queryset = queryset.prefetch_related('terminal').annotate(terminal_display=F('terminal__name'))
        elif self.request.method in ('PATCH',):
            # postgres reports an error for statements that use select_for_update for out join
            # so we need to use select_for_update only for have not prefetch_related and annotate
            queryset = queryset.select_for_update()
        return queryset

    def perform_create(self, serializer):
        if hasattr(self.request.user, 'terminal'):
            serializer.validated_data["terminal"] = self.request.user.terminal
        return super().perform_create(serializer)


class SessionReplayViewSet(AsyncApiMixin, viewsets.ViewSet):
    serializer_class = serializers.ReplaySerializer
    view_replay_cache_key = "SESSION_REPLAY_VIEW_{}_{}"
    session = None
    rbac_perms = {
        'create': 'terminal.upload_sessionreplay',
        'retrieve': 'terminal.view_sessionreplay',
        'index': 'terminal.view_sessionreplay',
    }

    def create(self, request, *args, **kwargs):
        session_id = kwargs.get('pk')
        session = get_object_or_404(Session, id=session_id)
        serializer = self.serializer_class(data=request.data)

        serializer.is_valid(raise_exception=True)
        file = serializer.validated_data['file']
        # 兼容旧版本 API 未指定 version 为 2 的情况
        version = serializer.validated_data.get('version', 2)
        name, err = session.save_replay_to_storage_with_version(file, version)
        if not name:
            msg = "Failed save replay `{}`: {}".format(session_id, err)
            logger.error(msg)
            return Response({'msg': str(err)}, status=HTTP_400_BAD_REQUEST)
        url = default_storage.url(name)
        return Response({'url': url}, status=HTTP_201_CREATED)

    @staticmethod
    def get_replay_data(session, url):
        all_guacamole_types = (
            TerminalType.lion, TerminalType.guacamole,
            TerminalType.razor, TerminalType.xrdp
        )

        if url.endswith('.cast.gz'):
            tp = 'asciicast'
        elif url.endswith('.mp4'):
            ## .replay.mp4 .part.mp4
            tp = 'mp4'
        elif url.endswith('replay.json'):
            # 新版本将返回元数据信息
            tp = 'parts'
        elif (getattr(session.terminal, 'type', None) in all_guacamole_types) or \
                (session.protocol in ('rdp', 'vnc')):
            tp = 'guacamole'
        else:
            tp = 'json'

        download_url = reverse('api-terminal:session-replay-download', kwargs={'pk': session.id})
        data = {
            'type': tp, 'src': url,
            'user': session.user, 'asset': session.asset,
            'account': session.account,
            'date_start': session.date_start,
            'date_end': session.date_end,
            'download_url': download_url,
        }
        return data

    def is_need_async(self):
        if self.action != 'retrieve':
            return False
        return True

    def async_callback(self, *args, **kwargs):
        session_id = kwargs.get('pk')
        cache_data = cache.get(self.async_cache_key) or {}
        response = cache_data.get('resp') or {}
        response_data = response.get('data') or {}
        if (
                cache_data.get('status') != 'ok'
                or not 200 <= response.get('status', 0) < 300
        ):
            return
        if not response_data.get('type'):
            return

        session = get_object_or_404(Session, id=session_id)
        self._record_replay_view(session)

    def _record_replay_view(self, session):
        detail = i18n_fmt(
            REPLAY_OP, self.request.user, _('View'), str(session)
        )
        key = self.view_replay_cache_key.format(self.request.user.id, session.id)
        if cache.get(key):
            return
        record_operate_log_and_activity_log(
            [session.asset_id], ActionChoices.view, detail, Session,
            resource=session, resource_type=_('Session replay')
        )
        cache.set(key, 1, 10)

    def retrieve(self, request, *args, **kwargs):
        session_id = kwargs.get('pk')
        session = get_object_or_404(Session, id=session_id)
        part_filename = request.query_params.get('part_filename')
        if part_filename:
            storage = SessionPartReplayStorageHandler(session)
            local_path, url = storage.get_part_file_path_url(part_filename)
        else:
            storage = ReplayStorageHandler(session)
            local_path, url = storage.get_file_path_url()

        if local_path is None:
            # url => error message
            return Response({"error": url}, status=404)
        data = self.get_replay_data(session, url)
        return Response(data)

    def index(self, request, *args, **kwargs):
        session = get_object_or_404(Session, id=kwargs.get('pk'))
        storage = SessionPartReplayStorageHandler(session)
        try:
            content = storage.get_verified_index_bytes()
        except FileNotFoundError:
            return Response({'error': gettext('Replay index not found.')}, status=404)
        except ValueError as exc:
            logger.warning('Invalid replay index for session %s: %s', session.id, exc)
            return Response({'error': gettext('Replay index is invalid.')}, status=422)
        except OSError as exc:
            logger.warning('Could not read replay index for session %s: %s', session.id, exc)
            return Response({'error': gettext('Replay index is unavailable.')}, status=503)
        self._record_replay_view(session)
        response = HttpResponse(
            content, content_type=storage.INDEX_MEDIA_TYPE
        )
        response['Cache-Control'] = 'private, no-store'
        return response


class SessionJoinValidateAPI(views.APIView):
    """
    监控用
    """
    serializer_class = serializers.SessionJoinValidateSerializer
    rbac_perms = {
        'POST': 'terminal.validate_sessionactionperm'
    }

    def post(self, request, *args, **kwargs):
        serializer = self.serializer_class(data=request.data)
        if not serializer.is_valid():
            msg = str(serializer.errors)
            return Response({'ok': False, 'msg': msg}, status=401)
        user_id = serializer.validated_data['user_id']
        session_id = serializer.validated_data['session_id']

        with tmp_to_root_org():
            session = get_object_or_none(Session, pk=session_id)
        if not session:
            msg = _('Session does not exist: {}'.format(session_id))
            return Response({'ok': False, 'msg': msg}, status=401)
        if not session.can_join:
            msg = _('Session is finished or the protocol not supported')
            return Response({'ok': False, 'msg': msg}, status=401)

        user = get_object_or_none(User, pk=user_id)
        if not user:
            msg = _('User does not exist: {}'.format(user_id))
            return Response({'ok': False, 'msg': msg}, status=401)

        with tmp_to_org(session.org):
            if is_session_approver(session_id, user_id):
                return Response({'ok': True, 'msg': ''}, status=200)

            if not user.has_perm('terminal.monitor_session'):
                msg = _('User does not have permission')
                return Response({'ok': False, 'msg': msg}, status=401)

        return Response({'ok': True, 'msg': ''}, status=200)
