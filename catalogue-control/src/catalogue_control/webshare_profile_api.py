"""Credential-safe HTTP boundary for operator-managed Webshare gateways."""

from __future__ import annotations

import json
import unicodedata
from dataclasses import dataclass
from typing import Any

from mb_ceramics_catalogue.observability import logging as obs
from mb_ceramics_catalogue.ops import events
from mb_ceramics_catalogue.proxy import ProxyDenied
from mb_ceramics_catalogue.webshare_gateway_secrets import (
    WebshareGatewaySecret,
    WebshareGatewaySecretStore,
    validate_webshare_gateway_secret,
)
from psycopg.types.json import Jsonb
from pydantic import SecretStr
from starlette.requests import Request
from starlette.responses import Response

from catalogue_control.auth import Actor
from catalogue_control.proxy_api import actor_for, payload, problem
from catalogue_control.proxy_control import Mutation, append_audit, finish_mutation
from catalogue_control.webshare_profile_import import (
    MUTATION_ACTION,
    WebshareProfileImportError,
    WebshareProfileImportResult,
    install_webshare_profile,
)

_MAX_BODY_BYTES = 32_768
_ROOT_FIELDS = frozenset(
    {"profile", "expected_generation", "display_name", "allocated_bytes", "confirmation"}
)
_PROFILE_FIELDS = frozenset(
    {"provider", "logical_name", "generation", "gateway", "credentials", "capabilities"}
)
_GATEWAY_FIELDS = frozenset({"endpoint_id", "protocol", "host", "port"})
_CREDENTIAL_FIELDS = frozenset({"username", "password"})
_CAPABILITY_FIELDS = frozenset({"countries", "sticky_session_ttl_seconds"})


class _InvalidRequest(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class _ImportRequest:
    secret: WebshareGatewaySecret
    expected_generation: int | None
    display_name: str | None
    allocated_bytes: int | None

    @property
    def safe_metadata(self) -> dict[str, Any]:
        return {
            "provider": self.secret.provider,
            "logical_name": self.secret.logical_name,
            "expected_generation": self.expected_generation,
            "target_generation": self.secret.generation,
            "display_name": self.display_name,
            "allocated_bytes": self.allocated_bytes,
        }


def _without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    obs.register_secrets(
        {
            value
            for key, value in pairs
            if key in {"username", "password"} and isinstance(value, str) and value
        }
    )
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _InvalidRequest
        result[key] = value
    return result


def _exact(value: Any, fields: frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise _InvalidRequest
    return value


def _credential_values(raw: Any) -> set[str]:
    if not isinstance(raw, dict) or not isinstance((profile := raw.get("profile")), dict):
        return set()
    credentials = profile.get("credentials")
    if not isinstance(credentials, dict):
        return set()
    return {
        value
        for key in ("username", "password")
        if isinstance((value := credentials.get(key)), str) and value
    }


def _optional_integer(value: Any) -> int | None:
    if value is None:
        return None
    if type(value) is not int:
        raise _InvalidRequest
    return value


def _parse_request(contents: bytes) -> _ImportRequest:
    if not contents or len(contents) > _MAX_BODY_BYTES:
        raise _InvalidRequest
    try:
        raw = json.loads(contents.decode("utf-8"), object_pairs_hook=_without_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError, _InvalidRequest):
        raise _InvalidRequest from None
    # Register immediately after decoding and before validation can fail.
    obs.register_secrets(_credential_values(raw))
    root = _exact(raw, _ROOT_FIELDS)
    profile = _exact(root["profile"], _PROFILE_FIELDS)
    gateway = _exact(profile["gateway"], _GATEWAY_FIELDS)
    credentials = _exact(profile["credentials"], _CREDENTIAL_FIELDS)
    capabilities = _exact(profile["capabilities"], _CAPABILITY_FIELDS)
    countries = capabilities["countries"]
    if (
        not isinstance(countries, list)
        or any(
            not isinstance(country, str)
            or len(country) != 2
            or not country.isascii()
            or not country.isalpha()
            or country != country.upper()
            for country in countries
        )
        or len(countries) != len(set(countries))
    ):
        raise _InvalidRequest
    try:
        secret = validate_webshare_gateway_secret(
            WebshareGatewaySecret(
                provider=profile["provider"],
                logical_name=profile["logical_name"],
                generation=profile["generation"],
                endpoint_id=gateway["endpoint_id"],
                protocol=gateway["protocol"],
                host=gateway["host"],
                port=gateway["port"],
                username=SecretStr(credentials["username"]),
                password=SecretStr(credentials["password"]),
                countries=frozenset(countries),
                sticky_session_ttl_seconds=capabilities["sticky_session_ttl_seconds"],
            )
        )
    except (KeyError, TypeError, ValueError, ProxyDenied):
        raise _InvalidRequest from None
    expected = _optional_integer(root["expected_generation"])
    display_name = root["display_name"]
    allocated_bytes = _optional_integer(root["allocated_bytes"])
    if expected is None:
        if (
            secret.generation != 1
            or not isinstance(display_name, str)
            or not display_name
            or len(display_name) > 200
            or any(unicodedata.category(character) == "Cc" for character in display_name)
            or allocated_bytes is None
            or allocated_bytes <= 0
            or allocated_bytes > 2_400_000_000
        ):
            raise _InvalidRequest
    elif (
        expected < 1
        or expected >= 2_147_483_647
        or secret.generation != expected + 1
        or display_name is not None
        or allocated_bytes is not None
    ):
        raise _InvalidRequest
    confirmation = f"IMPORT webshare/{secret.logical_name} GENERATION {secret.generation}"
    if root["confirmation"] != confirmation:
        raise _InvalidRequest
    return _ImportRequest(secret, expected, display_name, allocated_bytes)


async def _bounded_body(request: Request) -> bytes:
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > _MAX_BODY_BYTES:
            raise _InvalidRequest
        chunks.append(chunk)
    return b"".join(chunks)


async def _begin_or_resume(
    connection: Any,
    actor: Actor,
    idempotency_key: str | None,
    metadata: dict[str, Any],
) -> Mutation:
    """Resume only this action and only when its safe request tuple matches."""
    if not idempotency_key or len(idempotency_key) > 200:
        raise WebshareProfileImportError("idempotency_key_required")
    async with connection.transaction():
        cursor = await connection.execute(
            """insert into catalogue.proxy_mutation_requests
                      (actor, action, idempotency_key, response_data)
               values (%(actor)s, %(action)s, %(key)s, %(metadata)s)
               on conflict (actor, action, idempotency_key) do nothing
               returning operation_id""",
            {
                "actor": actor.id,
                "action": MUTATION_ACTION,
                "key": idempotency_key,
                "metadata": Jsonb(metadata),
            },
        )
        inserted = await cursor.fetchone()
        if inserted is not None:
            mutation = Mutation(inserted["operation_id"])
            await append_audit(
                connection,
                actor,
                mutation.operation_id,
                MUTATION_ACTION,
                "request",
                None,
                "started",
                idempotency_key=idempotency_key,
            )
            return mutation
        previous = await connection.execute(
            """select operation_id, state, response_status, response_data
                 from catalogue.proxy_mutation_requests
                where actor = %(actor)s and action = %(action)s
                  and idempotency_key = %(key)s""",
            {"actor": actor.id, "action": MUTATION_ACTION, "key": idempotency_key},
        )
        row = await previous.fetchone()
        if row is None:
            raise WebshareProfileImportError("idempotency_conflict")
        if row["state"] == "started":
            if row["response_data"] != metadata:
                raise WebshareProfileImportError("idempotency_request_mismatch")
            return Mutation(row["operation_id"])
        return Mutation(
            row["operation_id"], row["response_status"], row["response_data"] or {}
        )


def _safe_result(result: WebshareProfileImportResult) -> dict[str, Any]:
    data: dict[str, Any] = {
        "operation_id": str(result.operation_id),
        "profile_id": str(result.profile_id),
        "provider": result.provider,
        "logical_name": result.logical_name,
        "generation": result.generation,
        "state": result.state,
    }
    if result.error_code is not None:
        data["error_code" if result.state == "failed" else "remediation"] = result.error_code
    return data


async def _finish(
    connection: Any,
    mutation: Mutation,
    actor: Actor,
    result: WebshareProfileImportResult,
    *,
    status: int,
) -> Response:
    data = _safe_result(result)
    failed = result.state == "failed"
    async with connection.transaction():
        await finish_mutation(
            connection,
            mutation,
            actor,
            MUTATION_ACTION,
            status=status,
            data=data,
            state="failed" if failed else "succeeded",
            resource_type="profile",
            resource_id=str(result.profile_id),
            error_code=result.error_code if failed else None,
        )
        await events.emit(
            connection,
            events.Topic.PROXY,
            "proxy.profile_changed",
            payload={"id": str(result.profile_id), "provider": result.provider},
        )
    return payload(data, status=status)


async def import_webshare_profile(request: Request) -> Response:
    actor = await actor_for(request, admin=True, recent=True)
    if isinstance(actor, Response):
        return actor
    if request.query_params.getlist("provider") != ["webshare"] or len(request.query_params) != 1:
        return problem(422, "Invalid provider", "provider must equal webshare exactly once")
    settings = request.app.state.settings
    if not settings.proxy_mutations_enabled:
        return problem(409, "Proxy mutations disabled", "enable the mutation gate first")
    secret_path = settings.proxy_webshare_gateway_secret_file
    if secret_path is None:
        return problem(503, "Secret store unavailable", "Webshare gateway store is not configured")
    if request.headers.get("content-type", "").split(";", 1)[0].strip() != "application/json":
        return problem(422, "Invalid import", "application/json is required")
    try:
        parsed = _parse_request(await _bounded_body(request))
    except _InvalidRequest:
        return problem(422, "Invalid import", "request does not match the strict import contract")

    async with request.app.state.pool.connection() as connection:
        try:
            mutation = await _begin_or_resume(
                connection,
                actor,
                request.headers.get("idempotency-key"),
                parsed.safe_metadata,
            )
        except WebshareProfileImportError as error:
            return problem(409, "Idempotency conflict", error.code)
        if mutation.replay_status is not None:
            return payload(mutation.replay_data or {}, status=mutation.replay_status)
        try:
            result = await install_webshare_profile(
                connection,
                WebshareGatewaySecretStore(secret_path),
                operation_id=mutation.operation_id,
                actor_id=actor.id,
                secret=parsed.secret,
                expected_generation=parsed.expected_generation,
                display_name=parsed.display_name,
                allocated_bytes=parsed.allocated_bytes,
            )
        except WebshareProfileImportError as error:
            if error.code == "operation_busy":
                return problem(409, "Operation busy", "operation_busy")
            data = {
                "operation_id": str(mutation.operation_id),
                "provider": parsed.secret.provider,
                "logical_name": parsed.secret.logical_name,
                "generation": parsed.secret.generation,
                "state": "failed",
                "error_code": error.code,
            }
            async with connection.transaction():
                await finish_mutation(
                    connection,
                    mutation,
                    actor,
                    MUTATION_ACTION,
                    status=409,
                    data=data,
                    state="failed",
                    error_code=error.code,
                )
            return payload(data, status=409)

        if result.state == "installed":
            # The operator adds a current-cycle allocation, then retries this
            # exact idempotency key so recovery can finalize the installed file.
            return payload(_safe_result(result), status=202)
        if result.state == "draining":
            return await _finish(connection, mutation, actor, result, status=202)
        if result.state == "failed":
            return await _finish(connection, mutation, actor, result, status=409)
        status = 201 if result.created_profile else 200
        return await _finish(connection, mutation, actor, result, status=status)
