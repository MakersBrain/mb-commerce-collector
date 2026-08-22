from __future__ import annotations

import json
from collections import defaultdict, deque
from typing import Any

from mb_commerce_scraper.transports import (
    CommerceTransport,
    RotationReason,
    TransportRequest,
    TransportResponse,
    sanitize_url,
)

from .limits import (
    DEFAULT_FIXTURE_LIMITS,
    retain_fixture_content,
    retain_fixture_error,
)


class FakeTransport(CommerceTransport):
    def __init__(
        self,
        *,
        maximum_response_bytes: int = DEFAULT_FIXTURE_LIMITS.maximum_response_bytes,
        maximum_error_characters: int = DEFAULT_FIXTURE_LIMITS.maximum_error_characters,
    ) -> None:
        if maximum_response_bytes < 1:
            raise ValueError("maximum_response_bytes must be positive")
        if maximum_error_characters < 1:
            raise ValueError("maximum_error_characters must be positive")
        self.maximum_response_bytes = maximum_response_bytes
        self.maximum_error_characters = maximum_error_characters
        self._responses: dict[str, deque[TransportResponse | Exception]] = defaultdict(deque)
        self.requests: list[TransportRequest] = []
        self.rotations: list[RotationReason] = []

    def add(self, url: str, *, status: int = 200, json_body: Any | None = None, body: str | bytes | None = None, headers: dict[str, str] | None = None, error: Exception | None = None) -> None:
        if error is not None:
            if json_body is not None or body is not None or headers is not None or status != 200:
                raise ValueError("a fake error cannot be combined with response fields")
            self._responses[url].append(
                retain_fixture_error(
                    error,
                    maximum_characters=self.maximum_error_characters,
                )
            )
            return
        if json_body is not None:
            content = json.dumps(json_body).encode()
        elif isinstance(body, str):
            content = body.encode()
        else:
            content = body or b""
        retained = retain_fixture_content(
            content,
            maximum_bytes=self.maximum_response_bytes,
        )
        self._responses[url].append(
            TransportResponse(
                status=status,
                headers=headers or {},
                content=retained,
                final_url=url,
            )
        )

    async def request(self, request: TransportRequest) -> TransportResponse:
        self.requests.append(request)
        try:
            response = self._responses[request.url].popleft()
        except IndexError:
            raise RuntimeError(
                f"no fake response registered for {sanitize_url(request.url)}"
            ) from None
        if isinstance(response, Exception):
            raise response
        return response

    async def rotate_identity(self, reason: RotationReason) -> None:
        self.rotations.append(reason)
