from __future__ import annotations

import json
from collections import defaultdict, deque
from typing import Any

from mb_commerce_scraper.transports import (
    CommerceTransport,
    RotationReason,
    TransportRequest,
    TransportResponse,
)


class FakeTransport(CommerceTransport):
    def __init__(self) -> None:
        self._responses: dict[str, deque[TransportResponse]] = defaultdict(deque)
        self.requests: list[TransportRequest] = []
        self.rotations: list[RotationReason] = []

    def add(self, url: str, *, status: int = 200, json_body: Any | None = None, body: str | bytes | None = None, headers: dict[str, str] | None = None) -> None:
        if json_body is not None:
            content = json.dumps(json_body).encode()
        elif isinstance(body, str):
            content = body.encode()
        else:
            content = body or b""
        self._responses[url].append(TransportResponse(status=status, headers=headers or {}, content=content, final_url=url))

    async def request(self, request: TransportRequest) -> TransportResponse:
        self.requests.append(request)
        try:
            return self._responses[request.url].popleft()
        except IndexError:
            raise RuntimeError(f"no fake response registered for {request.url}") from None

    async def rotate_identity(self, reason: RotationReason) -> None:
        self.rotations.append(reason)

