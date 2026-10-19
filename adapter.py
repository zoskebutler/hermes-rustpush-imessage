"""Hermes adapter for the private local RustPush sidecar.

The plugin only translates Hermes's generic platform contract to a local
line-delimited JSON protocol. Apple protocol state and APNs/IDS connections
remain in the supervised sidecar.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import stat
import sys
import time
import uuid
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional

from gateway.config import Platform
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_media_bytes,
)

LOGGER = logging.getLogger("hermes.platforms.imessage")
PLATFORM_NAME = "imessage_cli"
PROTOCOL_VERSION = 1
MAX_FRAME_BYTES = 1_048_576
DEFAULT_SOCKET = Path.home() / ".hermes" / "rustpush" / "run" / "bridge.sock"
NETWORK_KEY_PARTS = (
    "endpoint",
    "host",
    "listen",
    "network",
    "port",
    "proxy",
    "relay",
    "remote",
    "tcp",
    "telemetry",
    "url",
)


class SidecarUnavailable(RuntimeError):
    pass


class SidecarRejected(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class SidecarClient:
    """One persistent authenticated-by-kernel UDS connection."""

    def __init__(
        self,
        socket_path: Path,
        on_event: Callable[[Dict[str, Any]], Awaitable[None]],
    ) -> None:
        self.socket_path = socket_path
        self.on_event = on_event
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.capabilities: set[str] = set()
        self._reader_task: Optional[asyncio.Task] = None
        self._write_lock = asyncio.Lock()
        self._pending: Dict[str, asyncio.Future] = {}
        self._event_tasks: set[asyncio.Task] = set()
        self._closed = asyncio.Event()

    def _verify_socket(self) -> None:
        st = self.socket_path.lstat()
        if not stat.S_ISSOCK(st.st_mode):
            raise SidecarUnavailable("unavailable")
        if st.st_uid != os.getuid() or stat.S_IMODE(st.st_mode) & 0o077:
            raise SidecarUnavailable("unavailable")

    async def connect(self) -> Dict[str, Any]:
        self._verify_socket()
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_unix_connection(str(self.socket_path)), timeout=5.0
            )
        except Exception:
            raise SidecarUnavailable("unavailable") from None
        self.reader, self.writer = reader, writer
        request_id = uuid.uuid4().hex
        await self._write(
            {"request_id": request_id, "op": "hello", "protocol": PROTOCOL_VERSION}
        )
        response = await self._read_frame()
        if (
            response.get("request_id") != request_id
            or response.get("ok") is not True
            or not isinstance(response.get("result"), dict)
        ):
            await self.close()
            raise SidecarUnavailable("protocol_error")
        result = response["result"]
        if result.get("protocol") != PROTOCOL_VERSION:
            await self.close()
            raise SidecarUnavailable("protocol_error")
        capabilities = result.get("capabilities", [])
        self.capabilities = {
            value for value in capabilities if isinstance(value, str)
        }
        self._reader_task = asyncio.create_task(
            self._reader_loop(), name="rustpush-plugin-reader"
        )
        return result

    async def _read_frame(self) -> Dict[str, Any]:
        if self.reader is None:
            raise SidecarUnavailable("unavailable")
        try:
            line = await self.reader.readline()
        except Exception:
            raise SidecarUnavailable("unavailable") from None
        if not line or len(line) > MAX_FRAME_BYTES:
            raise SidecarUnavailable("protocol_error")
        try:
            value = json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise SidecarUnavailable("protocol_error") from None
        if not isinstance(value, dict):
            raise SidecarUnavailable("protocol_error")
        return value

    async def _write(self, value: Dict[str, Any]) -> None:
        writer = self.writer
        if writer is None:
            raise SidecarUnavailable("unavailable")
        encoded = json.dumps(
            value, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8") + b"\n"
        if len(encoded) > MAX_FRAME_BYTES:
            raise SidecarRejected("request_too_large")
        async with self._write_lock:
            try:
                writer.write(encoded)
                await writer.drain()
            except Exception:
                raise SidecarUnavailable("unavailable") from None

    async def request(self, op: str, **payload: Any) -> Dict[str, Any]:
        if self._closed.is_set():
            raise SidecarUnavailable("unavailable")
        request_id = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pending[request_id] = future
        try:
            await self._write({"request_id": request_id, "op": op, **payload})
            response = await asyncio.wait_for(future, timeout=120.0)
        except asyncio.TimeoutError:
            raise SidecarUnavailable("timeout") from None
        finally:
            self._pending.pop(request_id, None)
        if response.get("ok") is not True:
            code = response.get("error", "transport_error")
            raise SidecarRejected(code if isinstance(code, str) else "transport_error")
        result = response.get("result", {})
        if not isinstance(result, dict):
            raise SidecarUnavailable("protocol_error")
        return result

    async def _reader_loop(self) -> None:
        try:
            while True:
                frame = await self._read_frame()
                event = frame.get("event")
                if isinstance(event, dict):
                    task = asyncio.create_task(
                        self.on_event(event), name="rustpush-plugin-event"
                    )
                    self._event_tasks.add(task)
                    task.add_done_callback(self._event_done)
                    continue
                request_id = frame.get("request_id")
                future = self._pending.get(request_id)
                if future is not None and not future.done():
                    future.set_result(frame)
        except (SidecarUnavailable, asyncio.CancelledError):
            pass
        finally:
            for future in list(self._pending.values()):
                if not future.done():
                    future.set_exception(SidecarUnavailable("unavailable"))
            self._closed.set()

    def _event_done(self, task: asyncio.Task) -> None:
        self._event_tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            # The sidecar event stays unacked and is replayed after reconnect.
            LOGGER.warning("iMessage inbound event deferred category=adapter_failure")

    async def wait_closed(self) -> None:
        await self._closed.wait()

    async def close(self) -> None:
        task = self._reader_task
        self._reader_task = None
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        for event_task in list(self._event_tasks):
            event_task.cancel()
        if self._event_tasks:
            await asyncio.gather(*self._event_tasks, return_exceptions=True)
        self._event_tasks.clear()
        writer = self.writer
        self.writer = None
        self.reader = None
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
        self._closed.set()


class RustPushIMessageAdapter(BasePlatformAdapter):
    """Thin Hermes translation layer over the local RustPush process."""

    supports_async_delivery = True
    MAX_MESSAGE_LENGTH = 500_000

    def __init__(self, config, *, _platform: Optional[Platform] = None) -> None:
        extra = getattr(config, "extra", {}) or {}
        if _has_network_configuration(extra):
            raise ValueError("network configuration is not supported")
        platform = _platform if _platform is not None else Platform(PLATFORM_NAME)
        super().__init__(config=config, platform=platform)
        raw_socket = str(extra.get("socket_path") or DEFAULT_SOCKET)
        self.socket_path = Path(raw_socket).expanduser()
        if not self.socket_path.is_absolute():
            raise ValueError("socket_path must be absolute")
        self._client: Optional[SidecarClient] = None
        self._supervisor: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._ready = asyncio.Event()
        self._last_inbound: OrderedDict[str, str] = OrderedDict()
        self._sent: OrderedDict[str, float] = OrderedDict()
        self._processed_events: OrderedDict[str, float] = OrderedDict()
        self._processed_messages: OrderedDict[tuple[str, str], float] = OrderedDict()
        self._inflight_events: dict[tuple[str, ...], asyncio.Future] = {}
        # Inbound burst coalescing: iMessage delivers a photo and its caption
        # (or several quick texts) as separate events. Without a short quiet
        # period the first event starts a turn and the next one interrupts it.
        # The base window is short (photo + caption land within ~1s); it only
        # stretches while the sender's typing indicator says more is coming.
        self._burst_quiet_s = _bounded_float(extra.get("burst_quiet_seconds"), 1.0, 0.0, 15.0)
        self._burst_max_s = max(
            self._burst_quiet_s,
            _bounded_float(extra.get("burst_max_seconds"), 30.0, 0.0, 120.0),
        )
        self._typing_grace_s = _bounded_float(extra.get("typing_grace_seconds"), 15.0, 0.0, 60.0)
        self._bursts: dict[tuple[str, str], _InboundBurst] = {}
        self._typing_until: dict[tuple[str, str], float] = {}

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        # The gateway supplies this keyword on both cold-start and recovery
        # paths. RustPush owns its own persistent replay queue, so reconnects
        # use the same sidecar handshake without resetting remote state.
        del is_reconnect
        self._stop.clear()
        if self._supervisor is None or self._supervisor.done():
            self._supervisor = asyncio.create_task(
                self._connection_supervisor(), name="rustpush-plugin-supervisor"
            )
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=20.0)
        except asyncio.TimeoutError:
            return False
        return True

    async def _connection_supervisor(self) -> None:
        backoff = 0.25
        while not self._stop.is_set():
            client = SidecarClient(self.socket_path, self._on_event)
            try:
                await client.connect()
            except (SidecarUnavailable, OSError):
                await client.close()
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    backoff = min(backoff * 2, 5.0)
                continue
            self._client = client
            self._ready.set()
            self._mark_connected()
            backoff = 0.25
            LOGGER.info(
                "iMessage local sidecar connected capabilities=%s",
                len(client.capabilities),
            )
            await client.wait_closed()
            if self._client is client:
                self._client = None
            self._ready.clear()
            self._mark_disconnected()
            await client.close()
        self._ready.clear()
        self._mark_disconnected()

    async def disconnect(self) -> None:
        self._stop.set()
        self._ready.clear()
        client = self._client
        self._client = None
        if client is not None:
            await client.close()
        task = self._supervisor
        self._supervisor = None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._mark_disconnected()

    async def _request(self, op: str, **payload: Any) -> Dict[str, Any]:
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=10.0)
        except asyncio.TimeoutError:
            raise SidecarUnavailable("unavailable") from None
        client = self._client
        if client is None:
            raise SidecarUnavailable("unavailable")
        if op not in client.capabilities:
            raise SidecarRejected("unsupported")
        return await client.request(op, **payload)

    def _remember(
        self, mapping: OrderedDict[Any, Any], key: Any, value: Any, limit: int
    ) -> None:
        mapping[key] = value
        mapping.move_to_end(key)
        while len(mapping) > limit:
            mapping.popitem(last=False)

    async def _on_event(self, event: Dict[str, Any]) -> None:
        event_id = event.get("event_id")
        if not isinstance(event_id, str) or not event_id:
            return
        kind = event.get("kind")
        if kind == "typing":
            self._note_typing(event, active=True)
            await self._forward_platform_event(event)
            return
        message_key = None
        if kind == "message" and event.get("from_me") is not True:
            chat_id, native_id = event.get("chat_id"), event.get("native_id")
            if (
                isinstance(chat_id, str) and chat_id
                and isinstance(native_id, str) and native_id
                and isinstance(event.get("sender"), str) and event["sender"]
            ):
                message_key = (chat_id, native_id)
        keys: list[tuple[str, ...]] = [("event", event_id)]
        if message_key is not None:
            keys.append(("message", *message_key))
        while True:
            if event_id in self._processed_events or (
                message_key is not None and message_key in self._processed_messages
            ):
                await self._ack_processed(event_id)
                return
            pending = next((self._inflight_events[key] for key in keys if key in self._inflight_events), None)
            if pending is None:
                break
            # Do not ack until admission succeeds; a cancelled waiter must not
            # cancel the owner's claim. Failed owners leave replay retryable.
            if await asyncio.shield(pending):
                await self._ack_processed(event_id)
                return

        # No await between checking and claiming either identity: reader tasks
        # share one event loop. In-flight claims are never retention-evicted.
        claim = asyncio.get_running_loop().create_future()
        for key in keys:
            self._inflight_events[key] = claim
        completed = False
        accepted_message = False

        def remember_completed() -> None:
            nonlocal completed
            if completed:
                return
            if message_key is not None:
                self._remember(self._processed_messages, message_key, time.monotonic(), 4096)
            self._remember(self._processed_events, event_id, time.monotonic(), 4096)
            completed = True

        try:
            if event.get("from_me") is True:
                native_id = event.get("native_id")
                if isinstance(native_id, str) and native_id:
                    self._remember(self._sent, native_id, time.monotonic(), 2000)
            elif kind == "message":
                accepted_message = await self._accept_message(event, on_admitted=remember_completed)
            elif kind == "reaction":
                await self._accept_reaction(event)
            else:
                await self._forward_platform_event(event)
            remember_completed()
        finally:
            for key in keys:
                del self._inflight_events[key]
            claim.set_result(completed)
        # Transport failure/cancellation after admission must not roll it back.
        if accepted_message:
            try:
                await self._request(
                    "mark_read", chat_id=event["chat_id"], native_id=event["native_id"]
                )
            except Exception:
                LOGGER.warning("iMessage automatic read receipt failed category=transport")
        await self._request("ack", event_id=event_id)

    async def _ack_processed(self, event_id: str) -> None:
        self._remember(self._processed_events, event_id, time.monotonic(), 4096)
        await self._request("ack", event_id=event_id)

    def _source_for(self, event: Dict[str, Any]):
        chat_id = str(event.get("chat_id") or "")
        participants = event.get("participants")
        count = len(participants) if isinstance(participants, list) else 0
        chat_type = "group" if count > 1 or event.get("group_name") else "dm"
        sender = event.get("sender")
        sender_id = sender if isinstance(sender, str) else None
        name = event.get("group_name")
        return self.build_source(
            chat_id=chat_id,
            chat_name=name if isinstance(name, str) else None,
            chat_type=chat_type,
            user_id=sender_id,
            user_name=None,
        )

    async def _forward_platform_event(self, event: Dict[str, Any]) -> None:
        handler = getattr(self, "_platform_event_handler", None)
        if handler is None:
            return
        kind = str(event.get("kind") or "unknown")
        stable = {
            "platform": PLATFORM_NAME,
            "event_name": f"imessage:{kind}",
            "event_id": event.get("event_id"),
            "chat_id": event.get("chat_id"),
            "sender": event.get("sender"),
            "native_id": event.get("native_id"),
            "timestamp_ms": event.get("timestamp_ms"),
            "target_id": event.get("target_id"),
            "target_part": event.get("target_part"),
            "text": event.get("text"),
            "reaction": event.get("emoji"),
            "removed": event.get("remove"),
            "typing": event.get("typing"),
            "participants": event.get("participants"),
            "group_name": event.get("group_name"),
        }
        await handler(stable, self._source_for(event))

    async def _accept_message(
        self, event: Dict[str, Any], *, on_admitted: Callable[[], None] | None = None
    ) -> bool:
        chat_id = event.get("chat_id")
        native_id = event.get("native_id")
        sender = event.get("sender")
        if not all(isinstance(value, str) and value for value in (chat_id, native_id, sender)):
            return False
        media_urls: list[str] = []
        media_types: list[str] = []
        attachment_meta = event.get("attachments")
        declared_attachment_count = 0
        unavailable_attachment_count = 0
        if isinstance(attachment_meta, list):
            for item in attachment_meta:
                if not isinstance(item, dict):
                    continue
                declared_attachment_count += 1
                if item.get("available") is not True:
                    unavailable_attachment_count += 1
                    continue
                # Accept the pre-contract-fix names while replaying any event
                # persisted by an older sidecar, but emit only the canonical
                # attachment_id/filename shape from the sidecar going forward.
                attachment_id = item.get("attachment_id") or item.get("id")
                if not isinstance(attachment_id, str):
                    unavailable_attachment_count += 1
                    continue
                materialized: Optional[Path] = None
                try:
                    result = await self._request(
                        "materialize_attachment", attachment_id=attachment_id
                    )
                    raw_path = result.get("path")
                    if not isinstance(raw_path, str):
                        continue
                    materialized = Path(raw_path)
                    data = materialized.read_bytes()
                    cached = cache_media_bytes(
                        data,
                        filename=str(item.get("filename") or item.get("name") or "attachment.bin"),
                        mime_type=str(item.get("mime") or "application/octet-stream"),
                    )
                    if cached is not None:
                        # Hermes' CachedMedia exposes ``media_type``; older
                        # builds used ``mime_type``. Reading the wrong one
                        # raised after the path was appended, so photos lost
                        # their type and reached the agent as documents.
                        cached_type = getattr(cached, "media_type", None) or getattr(
                            cached, "mime_type", None
                        ) or str(item.get("mime") or "application/octet-stream")
                        media_urls.append(cached.path)
                        media_types.append(cached_type)
                    else:
                        unavailable_attachment_count += 1
                except Exception:
                    unavailable_attachment_count += 1
                finally:
                    try:
                        await self._request(
                            "release_attachment", attachment_id=attachment_id
                        )
                    except Exception:
                        pass
        text = event.get("text")
        if not isinstance(text, str):
            text = ""
        # RustPush exposes an iMessage attachment balloon as U+FFFC in the
        # flattened text. The actual payload is carried in attachments.
        text = text.replace("\ufffc", "").strip()
        if not text and media_urls:
            text = "[Attachment]"
        elif not text and declared_attachment_count:
            text = "[Attachment unavailable]"
        if not text:
            # RustPush surfaces "stopped typing" (Typing(false)) as an empty
            # message event: release any burst that was waiting on it.
            self._note_typing(event, active=False)
            return False
        message_type = _message_type(media_types)
        timestamp_ms = event.get("timestamp_ms")
        try:
            timestamp = datetime.fromtimestamp(
                int(timestamp_ms) / 1000, tz=timezone.utc
            )
        except (TypeError, ValueError, OSError):
            timestamp = datetime.now(timezone.utc)
        self._remember(self._last_inbound, chat_id, native_id, 500)
        message_event = MessageEvent(
            text=text,
            user_id=sender,
            user_name=None,
            source=self._source_for(event),
            message_type=message_type,
            message_id=native_id,
            timestamp=timestamp,
            raw_message={
                "event_kind": "message",
                "attachment_count": len(media_urls),
                "declared_attachment_count": declared_attachment_count,
                "unavailable_attachment_count": unavailable_attachment_count,
                "has_reply": bool(event.get("reply_to")),
            },
            media_urls=media_urls,
            media_types=media_types,
            reply_to_message_id=(
                event.get("reply_to") if isinstance(event.get("reply_to"), str) else None
            ),
            metadata={
                "imessage_group_name": event.get("group_name"),
                "imessage_received_monotonic_ns": event.get("received_monotonic_ns"),
            },
        )
        try:
            await self._dispatch_inbound(message_event)
        finally:
            # Newer Hermes records admission before later awaits. Preserve it
            # even if cancellation prevents handle_message from returning.
            if on_admitted is not None and getattr(message_event, "_gateway_accepted", False) is True:
                on_admitted()
        return True

    def _note_typing(self, event: Dict[str, Any], *, active: bool) -> None:
        """Track the sender's typing indicator so a pending burst waits for them."""
        if event.get("from_me") is True:
            return
        chat_id, sender = event.get("chat_id"), event.get("sender")
        if not isinstance(chat_id, str) or not isinstance(sender, str):
            return
        key = (chat_id, sender)
        if active and self._typing_grace_s > 0:
            self._typing_until[key] = time.monotonic() + self._typing_grace_s
        else:
            self._typing_until.pop(key, None)
        burst = self._bursts.get(key)
        if burst is not None:
            burst.wake()

    async def _dispatch_inbound(self, message_event: MessageEvent) -> None:
        """Hand an inbound message to the gateway, coalescing quick bursts.

        Events from the same sender in the same chat that arrive within
        ``burst_quiet_seconds`` of each other (capped at ``burst_max_seconds``
        from the first) become ONE gateway message: texts are joined, media
        lists are concatenated. The first event of a burst owns the dispatch;
        later events wait for it and inherit its admission verdict, so every
        sidecar event is acked only after the merged message was admitted (and
        stays unacked/replayable if it was not).
        """
        if self._burst_quiet_s <= 0:
            await self.handle_message(message_event)
            return
        key = (message_event.source.chat_id, message_event.user_id or "")
        # Sending a message ends that typing session; a new indicator will
        # arrive if they keep typing.
        self._typing_until.pop(key, None)
        burst = self._bursts.get(key)
        if burst is not None:
            burst.absorb(message_event)
            accepted = await asyncio.shield(burst.result)
            message_event._gateway_accepted = accepted  # type: ignore[attr-defined]
            return
        burst = _InboundBurst(message_event)
        self._bursts[key] = burst
        try:
            try:
                await burst.wait_quiet(
                    self._burst_quiet_s,
                    self._burst_max_s,
                    lambda: self._typing_until.get(key, 0.0),
                )
            finally:
                # Close the window synchronously so a later event starts a
                # new burst instead of joining one that is being dispatched.
                if self._bursts.get(key) is burst:
                    del self._bursts[key]
            await self.handle_message(burst.event)
        except BaseException:
            if not burst.result.done():
                burst.result.set_exception(SidecarUnavailable("burst_dispatch_failed"))
                burst.result.exception()  # mark retrieved; members re-raise it
            raise
        if not burst.result.done():
            burst.result.set_result(getattr(burst.event, "_gateway_accepted", False) is True)

    async def _accept_reaction(self, event: Dict[str, Any]) -> None:
        await self._forward_platform_event(event)
        target = event.get("target_id")
        if not isinstance(target, str) or (
            target not in self._sent and not bool(event.get("target_from_me"))
        ):
            return
        sender = event.get("sender")
        chat_id = event.get("chat_id")
        if not isinstance(sender, str) or not isinstance(chat_id, str):
            return
        action = "removed" if event.get("remove") is True else "added"
        emoji = str(event.get("emoji") or "")
        await self.handle_message(
            MessageEvent(
                text=f"reaction:{action}:{emoji}",
                user_id=sender,
                source=self._source_for(event),
                message_id=str(event.get("native_id") or event.get("event_id")),
                reply_to_message_id=target,
                reply_to_is_own_message=True,
                raw_message={"event_kind": "reaction"},
            )
        )

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> SendResult:
        del metadata, kwargs
        try:
            await self.stop_typing(chat_id)
            result = await self._request(
                "reply" if reply_to else "send",
                chat_id=chat_id,
                text=content,
                **({"reply_to": reply_to} if reply_to else {}),
            )
        except (SidecarUnavailable, SidecarRejected) as exc:
            return SendResult(success=False, error=_safe_code(exc))
        native_id = result.get("native_id")
        if isinstance(native_id, str):
            self._remember(self._sent, native_id, time.monotonic(), 2000)
        return SendResult(success=True, message_id=native_id)

    async def _send_file(
        self,
        chat_id: str,
        file_path: str,
        *,
        caption: Optional[str],
        file_name: Optional[str],
        reply_to: Optional[str],
    ) -> SendResult:
        try:
            await self.stop_typing(chat_id)
            result = await self._request(
                "reply_file" if reply_to else "send_file",
                chat_id=chat_id,
                path=str(Path(file_path).expanduser()),
                caption=caption or "",
                **({"filename": file_name} if file_name else {}),
                **({"reply_to": reply_to} if reply_to else {}),
            )
        except (SidecarUnavailable, SidecarRejected) as exc:
            return SendResult(success=False, error=_safe_code(exc))
        native_id = result.get("native_id")
        if isinstance(native_id, str):
            self._remember(self._sent, native_id, time.monotonic(), 2000)
        return SendResult(success=True, message_id=native_id)

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> SendResult:
        del metadata, kwargs
        return await self._send_file(
            chat_id, file_path, caption=caption, file_name=file_name, reply_to=reply_to
        )

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> SendResult:
        del metadata, kwargs
        return await self._send_file(
            chat_id, image_path, caption=caption, file_name=None, reply_to=reply_to
        )

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> SendResult:
        del metadata, kwargs
        return await self._send_file(
            chat_id, video_path, caption=caption, file_name=None, reply_to=reply_to
        )

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> SendResult:
        del metadata, kwargs
        return await self._send_file(
            chat_id, audio_path, caption=caption, file_name=None, reply_to=reply_to
        )

    async def send_typing(
        self, chat_id: str, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        del metadata
        try:
            await self._request("typing", chat_id=chat_id, active=True)
        except Exception:
            return

    async def stop_typing(self, chat_id: str) -> None:
        try:
            await self._request("typing", chat_id=chat_id, active=False)
        except Exception:
            return

    async def mark_read(self, chat_id: str) -> bool:
        try:
            await self._request(
                "mark_read",
                chat_id=chat_id,
                native_id=self._last_inbound.get(chat_id),
            )
            return True
        except Exception:
            return False

    async def add_reaction(
        self,
        chat_id: str,
        reaction: str,
        message_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        target = message_id or self._last_inbound.get(chat_id)
        if not target:
            return {"success": False, "error": "missing_message_id"}
        try:
            result = await self._request(
                "react", chat_id=chat_id, native_id=target, reaction=reaction
            )
            return {"success": True, "message_id": result.get("native_id")}
        except (SidecarUnavailable, SidecarRejected) as exc:
            return {"success": False, "error": _safe_code(exc)}

    async def remove_reaction(
        self,
        chat_id: str,
        message_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        target = message_id or self._last_inbound.get(chat_id)
        if not target:
            return {"success": False, "error": "missing_message_id"}
        try:
            result = await self._request(
                "unreact", chat_id=chat_id, native_id=target
            )
            return {"success": True, "message_id": result.get("native_id")}
        except (SidecarUnavailable, SidecarRejected) as exc:
            return {"success": False, "error": _safe_code(exc)}

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        del finalize
        try:
            result = await self._request(
                "edit", chat_id=chat_id, native_id=message_id, text=content
            )
            return SendResult(success=True, message_id=result.get("native_id"))
        except (SidecarUnavailable, SidecarRejected) as exc:
            return SendResult(success=False, error=_safe_code(exc))

    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        try:
            await self._request(
                "undo_send", chat_id=chat_id, native_id=message_id
            )
            return True
        except Exception:
            return False

    async def create_conversation(
        self, participants: list[str], group_name: Optional[str] = None
    ) -> Dict[str, Any]:
        return await self._request(
            "create_conversation", participants=participants, group_name=group_name
        )

    async def rename_group(self, chat_id: str, name: str) -> Dict[str, Any]:
        return await self._request("rename_group", chat_id=chat_id, name=name)

    async def change_participants(
        self, chat_id: str, participants: list[str]
    ) -> Dict[str, Any]:
        return await self._request(
            "change_participants", chat_id=chat_id, participants=participants
        )

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {
            "id": chat_id,
            "name": chat_id,
            "type": "group" if chat_id.startswith("imessage:") else "dm",
        }


def _message_type(mimes: list[str]) -> MessageType:
    if not mimes:
        return MessageType.TEXT
    if all(value.startswith("image/") for value in mimes):
        return MessageType.PHOTO
    if all(value.startswith("video/") for value in mimes):
        return MessageType.VIDEO
    if all(value.startswith("audio/") for value in mimes):
        return MessageType.AUDIO
    return MessageType.DOCUMENT


def _safe_code(exc: BaseException) -> str:
    if isinstance(exc, SidecarRejected):
        return exc.code
    if isinstance(exc, SidecarUnavailable):
        return str(exc)
    return "transport_error"


def _has_network_configuration(value: Any, path: tuple[str, ...] = ()) -> bool:
    if isinstance(value, dict):
        for key, child in value.items():
            lowered = str(key).lower()
            if any(part in lowered for part in NETWORK_KEY_PARTS):
                return True
            if _has_network_configuration(child, path + (lowered,)):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_has_network_configuration(child, path) for child in value)
    return False


def validate_config(config: Dict[str, Any]) -> tuple[bool, str]:
    extra = config.get("extra", config) if isinstance(config, dict) else {}
    if not isinstance(extra, dict):
        return False, "extra must be a mapping"
    if _has_network_configuration(extra):
        return False, "network configuration is not supported"
    raw = str(extra.get("socket_path") or DEFAULT_SOCKET)
    if not Path(raw).expanduser().is_absolute():
        return False, "socket_path must be absolute"
    return True, ""


def is_connected(adapter: Any) -> bool:
    return isinstance(adapter, RustPushIMessageAdapter) and adapter.is_connected


def register(ctx) -> None:
    ctx.register_platform(
        name=PLATFORM_NAME,
        label="Local iMessage (RustPush)",
        adapter_factory=lambda config: RustPushIMessageAdapter(config),
        check_fn=lambda: sys.platform == "darwin",
        validate_config=validate_config,
        required_env=[],
        install_hint="Install and start the local Hermes RustPush sidecar.",
        is_connected=is_connected,
        allowed_users_env="RUSTPUSH_ALLOWED_USERS",
        allow_all_env="RUSTPUSH_ALLOW_ALL_USERS",
        max_message_length=RustPushIMessageAdapter.MAX_MESSAGE_LENGTH,
        allow_update_command=False,
        pii_safe=True,
    )


def _bounded_float(raw: Any, default: float, low: float, high: float) -> float:
    try:
        value = float(raw) if raw is not None else default
    except (TypeError, ValueError):
        value = default
    if value != value:  # NaN
        value = default
    return min(max(value, low), high)


_PLACEHOLDER_TEXTS = {"[Attachment]", "[Attachment unavailable]"}


class _InboundBurst:
    """A pending group of inbound events from one sender in one chat."""

    def __init__(self, first: MessageEvent) -> None:
        self.event = first
        self.started = time.monotonic()
        self.last = self.started
        self._bumped = asyncio.Event()
        self.result: asyncio.Future = asyncio.get_running_loop().create_future()

    def absorb(self, incoming: MessageEvent) -> None:
        merged = self.event
        texts = [t for t in (merged.text, incoming.text) if t and t not in _PLACEHOLDER_TEXTS]
        if texts:
            merged.text = "\n\n".join(texts)
        elif incoming.text and not merged.text:
            merged.text = incoming.text
        merged.media_urls.extend(incoming.media_urls)
        merged.media_types.extend(incoming.media_types)
        if merged.media_types:
            merged.message_type = _message_type(merged.media_types)
        # Thread any reply to the newest message in the burst.
        merged.message_id = incoming.message_id
        if incoming.reply_to_message_id and not merged.reply_to_message_id:
            merged.reply_to_message_id = incoming.reply_to_message_id
        raw, extra_raw = merged.raw_message, incoming.raw_message
        if isinstance(raw, dict) and isinstance(extra_raw, dict):
            for field in ("attachment_count", "declared_attachment_count", "unavailable_attachment_count"):
                raw[field] = int(raw.get(field) or 0) + int(extra_raw.get(field) or 0)
            raw["burst_size"] = int(raw.get("burst_size") or 1) + 1
        self.last = time.monotonic()
        self._bumped.set()

    def wake(self) -> None:
        """Re-evaluate the deadline (typing state changed)."""
        self._bumped.set()

    async def wait_quiet(
        self, quiet_s: float, max_s: float, typing_until: Callable[[], float] = lambda: 0.0
    ) -> None:
        """Return once the sender went quiet: no new event for ``quiet_s`` and
        no active typing indicator, or ``max_s`` elapsed since the first event."""
        while True:
            now = time.monotonic()
            settle = max(self.last + quiet_s, typing_until())
            timeout = min(settle - now, self.started + max_s - now)
            if timeout <= 0:
                return
            self._bumped.clear()
            try:
                await asyncio.wait_for(self._bumped.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                pass
