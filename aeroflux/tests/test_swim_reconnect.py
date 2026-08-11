"""swim_to_kafka.py's run_live() reconnect loop — the fix for ingest silently
dying when the Solace receiver terminates (it did, for ~2 days, while
e2e.sh health kept reporting the PID as "running").

Exercises the reconnect logic directly (no real Solace/Kafka needed):
_build_service and publish_raw_message are mocked, so this proves the loop
itself — reconnect on failure, resume publishing, respect --max-messages,
respect STOP_REQUESTED — without a live broker.
"""
from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import swim_to_kafka as stk


def _args(**overrides):
    base = dict(mock=False, max_messages=0, duration=0, no_verify_tls=False)
    base.update(overrides)
    return argparse.Namespace(**base)


class FakeMessage:
    def __init__(self, payload: str):
        self._payload = payload

    def get_payload_as_string(self):
        return self._payload

    def get_payload_as_bytes(self):
        return None

    def get_destination_name(self):
        return "swim/test"


def _fake_service(receiver: MagicMock) -> MagicMock:
    service = MagicMock(name="service")
    builder = MagicMock()
    builder.build.return_value = receiver
    service.create_persistent_message_receiver_builder.return_value = builder
    return service


def test_reconnects_after_receiver_failure_and_keeps_publishing(monkeypatch):
    """First connection yields one message then raises (simulating a
    terminated receiver) instead of a clean exit; the loop must reconnect
    and keep going, not exit the process."""
    monkeypatch.setattr(stk.time, "sleep", lambda *_: None)  # no real backoff wait

    receiver_1 = MagicMock(name="receiver_1")
    receiver_1.receive_message.side_effect = [
        FakeMessage("<msg1/>"),
        RuntimeError("receiver terminated"),
    ]
    receiver_2 = MagicMock(name="receiver_2")
    receiver_2.receive_message.side_effect = [FakeMessage("<msg2/>")]

    services = [_fake_service(receiver_1), _fake_service(receiver_2)]
    build_calls = []

    def fake_build_service(*_a, **_kw):
        build_calls.append(1)
        return services[len(build_calls) - 1]

    published_payloads = []

    def fake_publish(_producer, _topic, payload, _dest):
        published_payloads.append(payload)

    with patch.object(stk, "_build_service", side_effect=fake_build_service), \
         patch.object(stk, "build_kafka_producer", return_value=MagicMock()), \
         patch.object(stk, "publish_raw_message", side_effect=fake_publish), \
         patch.object(stk, "required_env", side_effect=lambda name: f"dummy-{name}"):
        stk.STOP_REQUESTED = False
        # Stop after 2 published messages so the test terminates deterministically.
        stk.run_live(_args(max_messages=2))

    assert len(build_calls) == 2, "expected exactly one reconnect (two connection attempts)"
    assert published_payloads == ["<msg1/>", "<msg2/>"]
    # The failed first receiver was still terminated on the way out, and the
    # second (live) one was too — cleanup must run on both paths, not just
    # the reconnect path.
    receiver_1.terminate.assert_called_once()
    receiver_2.terminate.assert_called_once()
    assert services[0].disconnect.call_count == 1
    assert services[1].disconnect.call_count == 1


def test_stop_requested_halts_reconnect_loop(monkeypatch):
    """A SIGINT/SIGTERM during a reconnect backoff must stop the loop, not
    force it through another connection attempt."""
    monkeypatch.setattr(stk.time, "sleep", lambda *_: None)

    receiver = MagicMock(name="receiver")
    receiver.receive_message.side_effect = RuntimeError("connection dropped")
    service = _fake_service(receiver)

    calls = {"n": 0}

    def fake_build_service(*_a, **_kw):
        calls["n"] += 1
        if calls["n"] == 1:
            return service
        # If the loop reconnected a second time, the STOP_REQUESTED check
        # failed to stop it — fail loudly rather than looping forever.
        raise AssertionError("reconnected after STOP_REQUESTED was set")

    def fake_publish(*_a, **_kw):
        pass

    def set_stop_after_failure(*_a, **_kw):
        stk.STOP_REQUESTED = True

    with patch.object(stk, "_build_service", side_effect=fake_build_service), \
         patch.object(stk, "build_kafka_producer", return_value=MagicMock()), \
         patch.object(stk, "publish_raw_message", side_effect=fake_publish), \
         patch.object(stk, "required_env", side_effect=lambda name: f"dummy-{name}"), \
         patch.object(stk.time, "sleep", side_effect=set_stop_after_failure):
        stk.STOP_REQUESTED = False
        stk.run_live(_args())

    assert calls["n"] == 1
    stk.STOP_REQUESTED = False  # reset module global for other tests
