from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

if hermes_root := os.environ.get("HERMES_AGENT_ROOT"):
    sys.path.insert(0, str(Path(hermes_root).expanduser().resolve()))

try:
    from gateway.config import Platform  # noqa: E402
except ImportError as exc:  # pragma: no cover - depends on external checkout
    raise unittest.SkipTest(
        "set HERMES_AGENT_ROOT to a Hermes Agent checkout to run adapter contract tests"
    ) from exc

from adapter import RustPushIMessageAdapter, validate_config  # noqa: E402


class AdapterContractTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        socket_path = str(Path(self.tmp.name) / "bridge.sock")
        config = SimpleNamespace(extra={"socket_path": socket_path, "burst_quiet_seconds": 0})
        self.adapter = RustPushIMessageAdapter(config, _platform=Platform.LOCAL)
        self.calls: list[tuple[str, dict]] = []

        async def request(operation: str, **fields):
            self.calls.append((operation, fields))
            if operation in {"send", "reply", "send_file", "reply_file", "edit", "undo_send"}:
                return {"native_id": f"id-{operation}"}
            return {"ok": True}

        self.adapter._request = request  # type: ignore[method-assign]

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def message(self, **changes):
        return {
            "event_id": "event-1", "kind": "message", "chat_id": "chat-1",
            "native_id": "BEBC3E9B-FAA8-4BA7-8FE2-1AE68084D033",
            "sender": "mailto:peer@example.com", "text": "lunch",
            "timestamp_ms": 1_700_000_000_000, **changes,
        }

    async def test_cancellation_after_gateway_admission_keeps_native_claim(self) -> None:
        accepted = []
        entered, release = asyncio.Event(), asyncio.Event()

        async def handle(event):
            accepted.append(event)
            event._gateway_accepted = True
            entered.set()
            await release.wait()

        self.adapter.handle_message = handle
        owner = asyncio.create_task(self.adapter._on_event(self.message()))
        await asyncio.wait_for(entered.wait(), 1)
        owner.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await owner
        release.set()
        await self.adapter._on_event(self.message(event_id="event-after-cancel"))
        self.assertEqual(len(accepted), 1)
        self.assertEqual(self.calls, [("ack", {"event_id": "event-after-cancel"})])

    async def test_concurrent_message_duplicates_wait_for_admission(self) -> None:
        accepted = []
        entered, release = asyncio.Event(), asyncio.Event()

        async def handle(event):
            accepted.append(event)
            entered.set()
            await release.wait()

        self.adapter.handle_message = handle
        first = asyncio.create_task(self.adapter._on_event(self.message()))
        await asyncio.wait_for(entered.wait(), 1)
        duplicates = [asyncio.create_task(self.adapter._on_event(event)) for event in (
            self.message(),
            self.message(event_id="event-2", timestamp_ms=1_700_000_000_100),
        )]
        await asyncio.sleep(0)
        before_release = (len(accepted), list(self.calls))
        release.set()
        await asyncio.gather(first, *duplicates)
        self.assertEqual(before_release, (1, []))
        self.assertEqual(len(accepted), 1)
        self.assertEqual([op for op, _ in self.calls].count("mark_read"), 1)
        self.assertCountEqual(
            [fields["event_id"] for op, fields in self.calls if op == "ack"],
            ["event-1", "event-1", "event-2"],
        )
        await self.adapter._on_event(self.message(event_id="event-3"))
        self.assertEqual(len(accepted), 1)
        self.assertEqual(self.calls[-1], ("ack", {"event_id": "event-3"}))

    async def test_failed_admission_releases_claim_for_waiting_duplicate(self) -> None:
        entered, release = asyncio.Event(), asyncio.Event()
        attempts = []

        async def handle(event):
            attempts.append(event)
            if len(attempts) == 1:
                entered.set()
                await release.wait()
                raise RuntimeError("admission failed")

        self.adapter.handle_message = handle
        first = asyncio.create_task(self.adapter._on_event(self.message()))
        await asyncio.wait_for(entered.wait(), 1)
        second = asyncio.create_task(self.adapter._on_event(self.message(event_id="retry")))
        await asyncio.sleep(0)
        self.assertEqual(self.calls, [])
        release.set()
        results = await asyncio.gather(first, second, return_exceptions=True)
        self.assertIsInstance(results[0], RuntimeError)
        self.assertIsNone(results[1])
        self.assertEqual(len(attempts), 2)
        self.assertEqual([fields["event_id"] for op, fields in self.calls if op == "ack"], ["retry"])
        await self.adapter._on_event(self.message())
        self.assertEqual(len(attempts), 2)
        self.assertEqual(self.adapter._inflight_events, {})

    async def test_failed_admission_can_replay_same_event_id(self) -> None:
        attempts = []

        async def handle(event):
            attempts.append(event)
            if len(attempts) == 1:
                raise RuntimeError("admission failed")

        self.adapter.handle_message = handle
        with self.assertRaises(RuntimeError):
            await self.adapter._on_event(self.message())
        self.assertEqual(self.calls, [])
        await self.adapter._on_event(self.message())
        self.assertEqual(len(attempts), 2)
        self.assertEqual(self.calls[-1][0], "ack")

    async def test_cancelled_owner_leaves_admission_retryable(self) -> None:
        entered = asyncio.Event()
        attempts = []

        async def handle(event):
            attempts.append(event)
            if len(attempts) == 1:
                entered.set()
                await asyncio.Event().wait()

        self.adapter.handle_message = handle
        first = asyncio.create_task(self.adapter._on_event(self.message()))
        await asyncio.wait_for(entered.wait(), 1)
        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first
        self.assertEqual(self.calls, [])
        await self.adapter._on_event(self.message())
        self.assertEqual(len(attempts), 2)
        self.assertEqual(self.adapter._inflight_events, {})

    async def test_cancelled_duplicate_does_not_cancel_owner_claim(self) -> None:
        entered, release = asyncio.Event(), asyncio.Event()
        accepted = []

        async def handle(event):
            accepted.append(event)
            entered.set()
            await release.wait()

        self.adapter.handle_message = handle
        first = asyncio.create_task(self.adapter._on_event(self.message()))
        await asyncio.wait_for(entered.wait(), 1)
        second = asyncio.create_task(self.adapter._on_event(self.message()))
        await asyncio.sleep(0)
        second.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await second
        release.set()
        await first
        await self.adapter._on_event(self.message(event_id="later"))
        self.assertEqual(len(accepted), 1)
        self.assertEqual(self.adapter._inflight_events, {})

    async def test_cancellation_during_read_receipt_does_not_repeat_admission(self) -> None:
        accepted = []
        reading = asyncio.Event()
        request = self.adapter._request

        async def handle(event):
            accepted.append(event)

        async def blocked_read(op, **payload):
            if op == "mark_read":
                reading.set()
                await asyncio.Event().wait()
            return await request(op, **payload)

        self.adapter.handle_message = handle
        self.adapter._request = blocked_read
        first = asyncio.create_task(self.adapter._on_event(self.message()))
        await asyncio.wait_for(reading.wait(), 1)
        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first
        self.adapter._request = request
        await self.adapter._on_event(self.message(event_id="replay"))
        self.assertEqual(len(accepted), 1)
        self.assertEqual(self.calls[-1], ("ack", {"event_id": "replay"}))

    async def test_ack_failure_does_not_repeat_admission(self) -> None:
        accepted = []
        request = self.adapter._request

        async def handle(event):
            accepted.append(event)

        async def fail_ack(op, **payload):
            if op == "ack":
                raise RuntimeError("ack failed")
            return await request(op, **payload)

        self.adapter.handle_message = handle
        self.adapter._request = fail_ack
        with self.assertRaises(RuntimeError):
            await self.adapter._on_event(self.message())
        self.adapter._request = request
        await self.adapter._on_event(self.message())
        await self.adapter._on_event(self.message(event_id="new-callback"))
        self.assertEqual(len(accepted), 1)
        self.assertEqual([op for op, _ in self.calls].count("ack"), 2)

    async def test_message_identity_is_scoped_to_chat_not_text(self) -> None:
        accepted = []

        async def handle(event):
            accepted.append(event)

        self.adapter.handle_message = handle
        for event in (self.message(), self.message(event_id="other-chat", chat_id="chat-2"),
                      self.message(event_id="other-message", native_id="native-2")):
            await self.adapter._on_event(event)
        self.assertEqual(len(accepted), 3)

    async def test_native_dedupe_preserves_edit_reaction_receipt_and_typing(self) -> None:
        accepted, forwarded = [], []

        async def handle(event):
            accepted.append(event)

        async def forward(event, source):
            forwarded.append(event)

        self.adapter.handle_message = handle
        self.adapter._platform_event_handler = forward
        await self.adapter._on_event(self.message())
        for kind in ("edit", "reaction", "unsend", "read_receipt", "delivery_receipt"):
            for index in range(2):
                await self.adapter._on_event(self.message(
                    event_id=f"{kind}-{index}", kind=kind, target_id="mine",
                    target_from_me=True, remove=bool(index), text=f"revision-{index}",
                ))
        for _ in range(2):
            await self.adapter._on_event(self.message(event_id="typing", kind="typing"))
        self.assertEqual(len(accepted), 3)  # message plus reaction add/remove
        self.assertEqual(len(forwarded), 12)
        self.assertEqual([op for op, _ in self.calls].count("ack"), 11)

    async def test_concurrent_event_id_dedupe_also_covers_edits(self) -> None:
        entered, release = asyncio.Event(), asyncio.Event()
        forwarded = []

        async def forward(event, source):
            forwarded.append(event)
            entered.set()
            await release.wait()

        self.adapter._platform_event_handler = forward
        event = self.message(kind="edit")
        first = asyncio.create_task(self.adapter._on_event(event))
        await asyncio.wait_for(entered.wait(), 1)
        second = asyncio.create_task(self.adapter._on_event(event))
        await asyncio.sleep(0)
        release.set()
        await asyncio.gather(first, second)
        self.assertEqual(len(forwarded), 1)
        self.assertEqual([op for op, _ in self.calls], ["ack", "ack"])

    async def test_completed_retention_is_bounded_without_evicting_inflight(self) -> None:
        entered, release = asyncio.Event(), asyncio.Event()
        accepted = []

        async def handle(event):
            accepted.append(event)
            if event.message_id == self.message()["native_id"]:
                entered.set()
                await release.wait()

        self.adapter.handle_message = handle
        first = asyncio.create_task(self.adapter._on_event(self.message()))
        await asyncio.wait_for(entered.wait(), 1)
        for index in range(4100):
            await self.adapter._on_event(self.message(event_id=f"event-{index+10}", native_id=f"native-{index}"))
            if index % 100 == 0:
                await asyncio.sleep(0)
        self.assertEqual(len(self.adapter._processed_events), 4096)
        self.assertEqual(len(self.adapter._processed_messages), 4096)
        duplicate = asyncio.create_task(self.adapter._on_event(self.message(event_id="duplicate")))
        await asyncio.sleep(0)
        count_before_release = len(accepted)
        release.set()
        await asyncio.gather(first, duplicate)
        self.assertEqual(count_before_release, 4101)
        self.assertEqual(len(accepted), 4101)
        self.assertEqual(self.adapter._inflight_events, {})
        self.assertEqual(len(self.adapter._processed_events), 4096)
        self.assertEqual(len(self.adapter._processed_messages), 4096)
        await self.adapter._on_event(self.message(event_id="event-10", native_id="native-0"))
        self.assertEqual(len(accepted), 4102)  # oldest completed identity was evicted

    async def test_outbound_fields_match_sidecar_protocol(self) -> None:
        sent = await self.adapter.send("mailto:peer@example.com", "hello", reply_to="orig")
        self.assertTrue(sent.success)
        self.assertEqual(
            self.calls[-1],
            (
                "reply",
                {
                    "chat_id": "mailto:peer@example.com",
                    "text": "hello",
                    "reply_to": "orig",
                },
            ),
        )

        with tempfile.NamedTemporaryFile() as attachment:
            result = await self.adapter.send_document(
                "mailto:peer@example.com",
                attachment.name,
                caption="caption",
                file_name="report.bin",
                reply_to="orig",
            )
        self.assertTrue(result.success)
        operation, fields = self.calls[-1]
        self.assertEqual(operation, "reply_file")
        self.assertEqual(fields["caption"], "caption")
        self.assertEqual(fields["filename"], "report.bin")
        self.assertEqual(fields["reply_to"], "orig")
        self.assertNotIn("body", fields)
        self.assertNotIn("message_id", fields)

        await self.adapter.send_typing("mailto:peer@example.com")
        self.assertEqual(self.calls[-1][1]["active"], True)
        await self.adapter.stop_typing("mailto:peer@example.com")
        self.assertEqual(self.calls[-1][1]["active"], False)

        self.adapter._last_inbound["chat"] = "incoming"
        self.assertTrue(await self.adapter.mark_read("chat"))
        self.assertEqual(self.calls[-1][1]["native_id"], "incoming")

        reaction = await self.adapter.add_reaction("chat", "love", "incoming")
        self.assertTrue(reaction["success"])
        self.assertEqual(self.calls[-1][1]["native_id"], "incoming")
        removed = await self.adapter.remove_reaction("chat", "incoming")
        self.assertTrue(removed["success"])
        self.assertEqual(self.calls[-1], ("unreact", {"chat_id": "chat", "native_id": "incoming"}))

        edit = await self.adapter.edit_message("chat", "mine", "revised")
        self.assertTrue(edit.success)
        self.assertEqual(self.calls[-1][1]["native_id"], "mine")
        self.assertTrue(await self.adapter.delete_message("chat", "mine"))
        self.assertEqual(self.calls[-1][1]["native_id"], "mine")

    async def test_connect_accepts_gateway_reconnect_keyword(self) -> None:
        async def ready_supervisor() -> None:
            self.adapter._ready.set()
            await self.adapter._stop.wait()

        self.adapter._connection_supervisor = ready_supervisor  # type: ignore[method-assign]
        self.assertTrue(await self.adapter.connect(is_reconnect=True))
        await self.adapter.disconnect()

    async def test_inbound_message_is_accepted_marked_read_then_acked(self) -> None:
        accepted = []

        async def handle(event):
            accepted.append(event)

        self.adapter.handle_message = handle  # type: ignore[method-assign]
        event = {
            "event_id": "event-1",
            "kind": "message",
            "chat_id": "imessage:" + "1" * 32,
            "native_id": "native-1",
            "sender": "mailto:peer@example.com",
            "participants": ["mailto:peer@example.com"],
            "timestamp_ms": 1_700_000_000_000,
            "received_monotonic_ns": 100,
            "from_me": False,
            "text": "hello",
            "attachments": [],
        }
        await self.adapter._on_event(event)
        self.assertEqual(len(accepted), 1)
        self.assertEqual(accepted[0].text, "hello")
        operations = [item[0] for item in self.calls]
        self.assertIn("mark_read", operations)
        self.assertEqual(operations[-1], "ack")

    async def test_inbound_attachment_materializes_and_removes_object_replacement_text(self) -> None:
        accepted = []
        source = Path(self.tmp.name) / "photo.jpg"
        source.write_bytes(b"jpeg")

        async def handle(event):
            accepted.append(event)

        async def request(operation: str, **fields):
            self.calls.append((operation, fields))
            if operation == "materialize_attachment":
                return {"path": str(source)}
            return {"ok": True}

        self.adapter.handle_message = handle  # type: ignore[method-assign]
        self.adapter._request = request  # type: ignore[method-assign]
        event = {
            "event_id": "event-attachment",
            "kind": "message",
            "chat_id": "imessage:" + "3" * 32,
            "native_id": "native-attachment",
            "sender": "mailto:peer@example.com",
            "participants": ["mailto:peer@example.com"],
            "timestamp_ms": 1_700_000_000_000,
            "received_monotonic_ns": 100,
            "from_me": False,
            "text": "\ufffc",
            "attachments": [
                {
                    "attachment_id": "a" * 32,
                    "filename": "photo.jpg",
                    "mime": "image/jpeg",
                    "size": 4,
                    "available": True,
                }
            ],
        }
        cached = SimpleNamespace(path="/private/cache/photo.jpg", mime_type="image/jpeg")
        with patch("adapter.cache_media_bytes", return_value=cached):
            await self.adapter._on_event(event)
        self.assertEqual(len(accepted), 1)
        self.assertEqual(accepted[0].text, "[Attachment]")
        self.assertEqual(accepted[0].media_urls, ["/private/cache/photo.jpg"])
        self.assertEqual(accepted[0].raw_message["declared_attachment_count"], 1)
        self.assertEqual(accepted[0].raw_message["unavailable_attachment_count"], 0)
        operations = [item[0] for item in self.calls]
        self.assertIn("materialize_attachment", operations)
        self.assertIn("release_attachment", operations)
        self.assertEqual(operations[-1], "ack")

    async def test_unavailable_attachment_is_explicit_not_raw_object_replacement_text(self) -> None:
        accepted = []

        async def handle(event):
            accepted.append(event)

        self.adapter.handle_message = handle  # type: ignore[method-assign]
        event = {
            "event_id": "event-unavailable",
            "kind": "message",
            "chat_id": "imessage:" + "4" * 32,
            "native_id": "native-unavailable",
            "sender": "mailto:peer@example.com",
            "participants": ["mailto:peer@example.com"],
            "timestamp_ms": 1_700_000_000_000,
            "received_monotonic_ns": 100,
            "from_me": False,
            "text": "\ufffc",
            "attachments": [
                {
                    "attachment_id": "b" * 32,
                    "filename": "photo.jpg",
                    "mime": "image/jpeg",
                    "size": 4,
                    "available": False,
                }
            ],
        }
        await self.adapter._on_event(event)
        self.assertEqual(accepted[0].text, "[Attachment unavailable]")
        self.assertEqual(accepted[0].raw_message["declared_attachment_count"], 1)
        self.assertEqual(accepted[0].raw_message["unavailable_attachment_count"], 1)

    async def test_reaction_to_prior_own_message_reaches_generic_handler(self) -> None:
        accepted = []

        async def handle(event):
            accepted.append(event)

        self.adapter.handle_message = handle  # type: ignore[method-assign]
        event = {
            "event_id": "event-2",
            "kind": "reaction",
            "chat_id": "imessage:" + "2" * 32,
            "native_id": "reaction-1",
            "sender": "tel:+15555550100",
            "participants": ["tel:+15555550100"],
            "timestamp_ms": 1_700_000_000_000,
            "target_id": "mine-before-restart",
            "target_from_me": True,
            "emoji": "❤️",
            "remove": False,
            "from_me": False,
        }
        await self.adapter._on_event(event)
        self.assertEqual(len(accepted), 1)
        self.assertEqual(accepted[0].text, "reaction:added:❤️")
        self.assertEqual(self.calls[-1][0], "ack")

    def test_registers_as_cron_delivery_target(self) -> None:
        import adapter as adapter_module

        captured = {}

        class Ctx:
            def register_platform(self, **kwargs):
                captured.update(kwargs)

        adapter_module.register(Ctx())
        self.assertEqual(captured.get("cron_deliver_env_var"), "IMESSAGE_CLI_HOME_CHANNEL")

    def test_config_rejects_network_options(self) -> None:
        self.assertEqual(
            validate_config({"extra": {"socket_path": "/tmp/a.sock"}}),
            (True, ""),
        )
        error = validate_config(
            {"extra": {"socket_path": "/tmp/a.sock", "bridge_url": "http://127.0.0.1"}}
        )
        self.assertEqual(error[0], False)



class InboundBurstTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        socket_path = str(Path(self.tmp.name) / "bridge.sock")
        config = SimpleNamespace(extra={
            "socket_path": socket_path, "burst_quiet_seconds": 0.2, "burst_max_seconds": 1.5,
            "typing_grace_seconds": 0.8,
        })
        self.adapter = RustPushIMessageAdapter(config, _platform=Platform.LOCAL)
        self.calls: list[tuple[str, dict]] = []
        self.handled: list = []

        async def request(operation: str, **fields):
            self.calls.append((operation, fields))
            if operation == "materialize_attachment":
                path = Path(self.tmp.name) / f"{fields['attachment_id']}.jpg"
                path.write_bytes(b"\xff\xd8\xff\xe0" + b"0" * 64)
                return {"path": str(path)}
            return {"ok": True}

        async def handle(event):
            self.handled.append(event)
            event._gateway_accepted = True

        self.adapter._request = request  # type: ignore[method-assign]
        self.adapter.handle_message = handle  # type: ignore[method-assign]

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def message(self, n: int, **changes):
        return {
            "event_id": f"event-{n}", "kind": "message", "chat_id": "chat-1",
            "native_id": f"native-{n}", "sender": "tel:+15555550100", "text": f"text {n}",
            "timestamp_ms": 1_700_000_000_000 + n, **changes,
        }

    def photo(self, n: int, text: str = "\ufffc"):
        return self.message(n, text=text, attachments=[{
            "attachment_id": f"att-{n}", "available": True, "mime": "image/jpeg", "filename": f"p{n}.jpg",
        }])

    def acks(self) -> list[str]:
        return [fields["event_id"] for op, fields in self.calls if op == "ack"]

    async def test_photo_then_caption_becomes_one_message(self) -> None:
        first = asyncio.create_task(self.adapter._on_event(self.photo(1)))
        await asyncio.sleep(0.05)
        second = asyncio.create_task(self.adapter._on_event(self.message(2, text="what do you think?")))
        await asyncio.gather(first, second)

        self.assertEqual(len(self.handled), 1)
        merged = self.handled[0]
        self.assertEqual(merged.text, "what do you think?")
        self.assertEqual(len(merged.media_urls), 1)
        self.assertEqual(merged.message_type.name, "PHOTO")
        self.assertEqual(merged.message_id, "native-2")
        self.assertEqual(sorted(self.acks()), ["event-1", "event-2"])

    async def test_several_photos_and_texts_merge_in_order(self) -> None:
        tasks = []
        for event in (self.message(1, text="here are both"), self.photo(2), self.photo(3), self.message(4, text="mine is second")):
            tasks.append(asyncio.create_task(self.adapter._on_event(event)))
            await asyncio.sleep(0.05)
        await asyncio.gather(*tasks)

        self.assertEqual(len(self.handled), 1)
        merged = self.handled[0]
        self.assertEqual(merged.text, "here are both\n\nmine is second")
        self.assertEqual(len(merged.media_urls), 2)
        self.assertEqual(merged.raw_message["burst_size"], 4)

    async def test_quiet_gap_starts_a_new_message(self) -> None:
        await self.adapter._on_event(self.message(1, text="first"))
        await self.adapter._on_event(self.message(2, text="second"))
        self.assertEqual([e.text for e in self.handled], ["first", "second"])

    async def test_hard_cap_flushes_a_long_burst(self) -> None:
        loop = asyncio.get_running_loop()
        started = loop.time()
        tasks = []
        for n in range(1, 14):  # 0.15s apart: never quiet for 0.2s
            tasks.append(asyncio.create_task(self.adapter._on_event(self.message(n))))
            await asyncio.sleep(0.15)
        await asyncio.gather(*tasks)
        self.assertGreaterEqual(len(self.handled), 2)
        self.assertLess(loop.time() - started, 3.0)

    async def test_other_sender_is_not_merged(self) -> None:
        a = asyncio.create_task(self.adapter._on_event(self.message(1, text="mine")))
        await asyncio.sleep(0.05)
        b = asyncio.create_task(self.adapter._on_event(self.message(2, text="theirs", sender="tel:+15555550199")))
        await asyncio.gather(a, b)
        self.assertEqual(sorted(e.text for e in self.handled), ["mine", "theirs"])

    async def test_rejected_burst_leaves_every_event_unacked(self) -> None:
        async def reject(event):
            self.handled.append(event)
            event._gateway_accepted = False
            raise RuntimeError("gateway down")

        self.adapter.handle_message = reject  # type: ignore[method-assign]
        a = asyncio.create_task(self.adapter._on_event(self.photo(1)))
        await asyncio.sleep(0.05)
        b = asyncio.create_task(self.adapter._on_event(self.message(2, text="caption")))
        results = await asyncio.gather(a, b, return_exceptions=True)
        self.assertTrue(all(isinstance(r, Exception) for r in results))
        self.assertEqual(self.acks(), [])

    def test_burst_settings_are_bounded(self) -> None:
        config = SimpleNamespace(extra={
            "socket_path": str(Path(self.tmp.name) / "x.sock"),
            "burst_quiet_seconds": "nope", "burst_max_seconds": 999,
        })
        adapter = RustPushIMessageAdapter(config, _platform=Platform.LOCAL)
        self.assertEqual(adapter._burst_quiet_s, 1.0)
        self.assertEqual(adapter._burst_max_s, 120.0)

    def typing(self, n: int, *, active: bool = True, sender: str = "tel:+15555550100"):
        if active:
            return {"event_id": f"typing-{n}", "kind": "typing", "chat_id": "chat-1",
                    "sender": sender, "typing": True}
        # RustPush surfaces Typing(false) as an empty message event.
        return self.message(n, text="", sender=sender)

    async def test_single_text_dispatches_after_short_window(self) -> None:
        loop = asyncio.get_running_loop()
        started = loop.time()
        await self.adapter._on_event(self.message(1, text="quick question"))
        self.assertEqual(len(self.handled), 1)
        self.assertLess(loop.time() - started, 0.5)

    async def test_typing_indicator_holds_the_burst_open(self) -> None:
        first = asyncio.create_task(self.adapter._on_event(self.message(1, text="one more thing")))
        await asyncio.sleep(0.05)
        await self.adapter._on_event(self.typing(2))
        await asyncio.sleep(0.5)  # past the 0.2s quiet window, still typing
        self.assertEqual(self.handled, [])
        second = asyncio.create_task(self.adapter._on_event(self.message(3, text="and this")))
        await asyncio.gather(first, second)
        self.assertEqual(len(self.handled), 1)
        self.assertEqual(self.handled[0].text, "one more thing\n\nand this")

    async def test_stopped_typing_releases_the_burst(self) -> None:
        loop = asyncio.get_running_loop()
        started = loop.time()
        first = asyncio.create_task(self.adapter._on_event(self.message(1, text="hmm")))
        await asyncio.sleep(0.05)
        await self.adapter._on_event(self.typing(2))
        await asyncio.sleep(0.1)
        await self.adapter._on_event(self.typing(3, active=False))
        await first
        self.assertEqual([e.text for e in self.handled], ["hmm"])
        self.assertLess(loop.time() - started, 0.6)  # did not wait out the 0.8s grace

    async def test_abandoned_typing_expires(self) -> None:
        loop = asyncio.get_running_loop()
        started = loop.time()
        first = asyncio.create_task(self.adapter._on_event(self.message(1, text="hello")))
        await asyncio.sleep(0.05)
        await self.adapter._on_event(self.typing(2))
        await first
        elapsed = loop.time() - started
        self.assertEqual(len(self.handled), 1)
        self.assertGreater(elapsed, 0.7)
        self.assertLess(elapsed, 1.3)

    async def test_other_senders_typing_does_not_delay(self) -> None:
        loop = asyncio.get_running_loop()
        started = loop.time()
        first = asyncio.create_task(self.adapter._on_event(self.message(1, text="mine")))
        await asyncio.sleep(0.05)
        await self.adapter._on_event(self.typing(2, sender="tel:+15555550199"))
        await first
        self.assertLess(loop.time() - started, 0.5)

if __name__ == "__main__":
    unittest.main()
