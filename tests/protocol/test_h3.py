from __future__ import annotations

import importlib
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from hypercorn.asyncio.worker_context import WorkerContext
from hypercorn.config import Config
from hypercorn.typing import ConnectionState


async def _protocol_with_requests(
    stream_ids: list[int],
) -> tuple[Any, dict[int, AsyncMock]]:
    pytest.importorskip("aioquic")
    h3_events = importlib.import_module("aioquic.h3.events")
    h3_protocol = importlib.import_module("hypercorn.protocol.h3")
    config = Config()
    config._log = AsyncMock()
    app_receivers = {stream_id: AsyncMock() for stream_id in stream_ids}
    task_group = Mock()
    task_group.spawn_app = AsyncMock(side_effect=list(app_receivers.values()))
    protocol = h3_protocol.H3Protocol(
        Mock(),
        config,
        WorkerContext(None),
        task_group,
        ConnectionState({}),
        None,
        None,
        Mock(),
        AsyncMock(),
    )
    for stream_id in stream_ids:
        await protocol._create_stream(
            h3_events.HeadersReceived(
                stream_id=stream_id,
                stream_ended=False,
                headers=[
                    (b":method", b"POST"),
                    (b":path", f"/upload/{stream_id}".encode()),
                    (b":scheme", b"https"),
                    (b":authority", b"hypercorn"),
                ],
            )
        )

    return protocol, app_receivers


@pytest.mark.asyncio
async def test_stream_reset_reaches_asgi_as_http_disconnect() -> None:
    protocol, app_receivers = await _protocol_with_requests([0])
    quic_events = importlib.import_module("aioquic.quic.events")

    await protocol.handle(quic_events.StreamReset(error_code=0, stream_id=0))

    app_receivers[0].assert_awaited_once_with({"type": "http.disconnect"})
    assert protocol.streams == {}


@pytest.mark.asyncio
async def test_connection_termination_disconnects_all_active_requests() -> None:
    protocol, app_receivers = await _protocol_with_requests([0, 4])
    quic_events = importlib.import_module("aioquic.quic.events")

    await protocol.handle(
        quic_events.ConnectionTerminated(
            error_code=0, frame_type=None, reason_phrase="client closed"
        )
    )

    for receiver in app_receivers.values():
        receiver.assert_awaited_once_with({"type": "http.disconnect"})
    assert protocol.streams == {}
