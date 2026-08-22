"""Strict contract and local CAS store for operator-issued Webshare secrets.

This module validates and atomically stores credentials.  It deliberately does
not expose an API, mutate provider state, or make a profile eligible for paid
traffic.
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import secrets
import stat
import unicodedata
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import SecretStr

from mb_ceramics_catalogue.proxy import ProxyDenied

SCHEMA_VERSION = 2
MAX_SECRET_FILE_BYTES = 1_048_576
MAX_PROFILES = 256
MAX_USERNAME_CHARACTERS = 512
MAX_PASSWORD_CHARACTERS = 1_024
MAX_GENERATION = 2_147_483_647

_PROVIDER = "webshare"
_ENDPOINT_ID = "webshare-residential-backbone"
_HOST = "p.webshare.io"
_PROTOCOL = "http"
_LOGICAL_NAME = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")

_ROOT_FIELDS = frozenset({"schema_version", "profiles"})
_PROFILE_FIELDS = frozenset(
    {"provider", "logical_name", "generation", "gateway", "credentials", "capabilities"}
)
_GATEWAY_FIELDS = frozenset({"endpoint_id", "protocol", "host", "port"})
_CREDENTIAL_FIELDS = frozenset({"username", "password"})
_CAPABILITY_FIELDS = frozenset({"countries", "sticky_session_ttl_seconds"})

ProfileKey = tuple[str, str]


@dataclass(frozen=True, slots=True)
class WebshareGatewaySecret:
    """One validated provider-bound gateway generation."""

    provider: str
    logical_name: str
    generation: int
    endpoint_id: str
    protocol: str
    host: str
    port: int
    username: SecretStr
    password: SecretStr
    countries: frozenset[str]
    sticky_session_ttl_seconds: int

    @property
    def key(self) -> ProfileKey:
        return self.provider, self.logical_name


class _DuplicateKey(ValueError):
    pass


def _object_without_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateKey
        value[key] = item
    return value


def _read_private_file(path: Path) -> bytes:
    """Read one bounded regular file descriptor without following a final symlink."""
    try:
        path_metadata = path.lstat()
        if stat.S_ISLNK(path_metadata.st_mode):
            raise ProxyDenied("Webshare gateway secret must not be a symbolic link")
        if not stat.S_ISREG(path_metadata.st_mode):
            raise ProxyDenied("Webshare gateway secret must be a regular file")
        if stat.S_IMODE(path_metadata.st_mode) not in {
            0o400,
            0o600,
        }:
            raise ProxyDenied("Webshare gateway secret permissions must be 0400 or 0600")
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
    except ProxyDenied:
        raise
    except OSError:
        raise ProxyDenied("Webshare gateway secret cannot be opened safely") from None

    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ProxyDenied("Webshare gateway secret must be a regular file")
        if stat.S_IMODE(metadata.st_mode) not in {0o400, 0o600}:
            raise ProxyDenied("Webshare gateway secret permissions must be 0400 or 0600")
        if metadata.st_size > MAX_SECRET_FILE_BYTES:
            raise ProxyDenied("Webshare gateway secret exceeds the size limit")
        chunks: list[bytes] = []
        remaining = MAX_SECRET_FILE_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(65_536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        contents = b"".join(chunks)
        if len(contents) > MAX_SECRET_FILE_BYTES:
            raise ProxyDenied("Webshare gateway secret exceeds the size limit")
        return contents
    finally:
        os.close(descriptor)


def _exact_object(value: Any, fields: frozenset[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise ProxyDenied(f"Webshare gateway secret {label} has invalid fields")
    return value


def _strict_integer(value: Any, label: str, *, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ProxyDenied(f"Webshare gateway secret {label} is invalid")
    return value


def _secret_text(value: Any, label: str, *, maximum: int) -> SecretStr:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ProxyDenied(f"Webshare gateway secret {label} is invalid")
    if any(unicodedata.category(character) == "Cc" for character in value):
        raise ProxyDenied(f"Webshare gateway secret {label} contains control characters")
    return SecretStr(value)


def _countries(value: Any) -> frozenset[str]:
    if not isinstance(value, list):
        raise ProxyDenied("Webshare gateway secret countries must be an array")
    countries: list[str] = []
    for country in value:
        if (
            not isinstance(country, str)
            or len(country) != 2
            or not country.isascii()
            or not country.isalpha()
            or country != country.upper()
        ):
            raise ProxyDenied("Webshare gateway secret contains an invalid country")
        countries.append(country)
    if len(countries) != len(set(countries)):
        raise ProxyDenied("Webshare gateway secret countries must be unique")
    return frozenset(countries)


def _profile(profile_key: str, value: Any) -> WebshareGatewaySecret:
    record = _exact_object(value, _PROFILE_FIELDS, f"profile {profile_key!r}")
    provider = record["provider"]
    logical_name = record["logical_name"]
    if provider != _PROVIDER or not isinstance(logical_name, str):
        raise ProxyDenied("Webshare gateway secret profile identity is invalid")
    if _LOGICAL_NAME.fullmatch(logical_name) is None:
        raise ProxyDenied("Webshare gateway secret logical name is invalid")
    if profile_key != f"{provider}/{logical_name}":
        raise ProxyDenied("Webshare gateway secret profile key does not match its identity")

    gateway = _exact_object(record["gateway"], _GATEWAY_FIELDS, "gateway")
    if (
        gateway["endpoint_id"] != _ENDPOINT_ID
        or gateway["protocol"] != _PROTOCOL
        or gateway["host"] != _HOST
    ):
        raise ProxyDenied("Webshare gateway secret endpoint is not verified")
    port = _strict_integer(gateway["port"], "port", minimum=1, maximum=65_535)
    if port not in {80, 1_080, 3_128} and not 9_999 <= port <= 19_999:
        raise ProxyDenied("Webshare gateway secret port is not verified")

    credentials = _exact_object(record["credentials"], _CREDENTIAL_FIELDS, "credentials")
    capabilities = _exact_object(record["capabilities"], _CAPABILITY_FIELDS, "capabilities")
    return WebshareGatewaySecret(
        provider=provider,
        logical_name=logical_name,
        generation=_strict_integer(
            record["generation"], "generation", minimum=1, maximum=MAX_GENERATION
        ),
        endpoint_id=_ENDPOINT_ID,
        protocol=_PROTOCOL,
        host=_HOST,
        port=port,
        username=_secret_text(
            credentials["username"], "username", maximum=MAX_USERNAME_CHARACTERS
        ),
        password=_secret_text(
            credentials["password"], "password", maximum=MAX_PASSWORD_CHARACTERS
        ),
        countries=_countries(capabilities["countries"]),
        sticky_session_ttl_seconds=_strict_integer(
            capabilities["sticky_session_ttl_seconds"],
            "sticky session duration",
            minimum=60,
            maximum=86_400,
        ),
    )


def _parse_webshare_gateway_secrets(
    contents: bytes,
) -> dict[ProfileKey, WebshareGatewaySecret]:
    try:
        raw = json.loads(
            contents.decode("utf-8"),
            object_pairs_hook=_object_without_duplicate_keys,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, _DuplicateKey):
        raise ProxyDenied("Webshare gateway secret is not valid strict JSON") from None
    root = _exact_object(raw, _ROOT_FIELDS, "root")
    if type(root["schema_version"]) is not int or root["schema_version"] != SCHEMA_VERSION:
        raise ProxyDenied("Webshare gateway secret schema version is unsupported")
    profiles = root["profiles"]
    if not isinstance(profiles, dict):
        raise ProxyDenied("Webshare gateway secret profiles must be an object")
    if len(profiles) > MAX_PROFILES:
        raise ProxyDenied("Webshare gateway secret contains too many profiles")
    loaded: dict[ProfileKey, WebshareGatewaySecret] = {}
    for profile_key, value in profiles.items():
        profile = _profile(profile_key, value)
        if profile.key in loaded:
            raise ProxyDenied("Webshare gateway secret contains a duplicate profile identity")
        loaded[profile.key] = profile
    return loaded


def load_webshare_gateway_secrets(path: Path) -> dict[ProfileKey, WebshareGatewaySecret]:
    """Load strict v2 records indexed by ``(provider, logical_name)``."""
    return _parse_webshare_gateway_secrets(_read_private_file(path))


def secret_values(profiles: dict[ProfileKey, WebshareGatewaySecret]) -> set[str]:
    """Return exact credential values for immediate structured-log redaction."""
    return {
        value
        for profile in profiles.values()
        for value in (
            profile.username.get_secret_value(),
            profile.password.get_secret_value(),
        )
    }


def _profile_record(profile: WebshareGatewaySecret) -> dict[str, Any]:
    return {
        "provider": profile.provider,
        "logical_name": profile.logical_name,
        "generation": profile.generation,
        "gateway": {
            "endpoint_id": profile.endpoint_id,
            "protocol": profile.protocol,
            "host": profile.host,
            "port": profile.port,
        },
        "credentials": {
            "username": profile.username.get_secret_value(),
            "password": profile.password.get_secret_value(),
        },
        "capabilities": {
            "countries": sorted(profile.countries),
            "sticky_session_ttl_seconds": profile.sticky_session_ttl_seconds,
        },
    }


def _validated_profile(profile: WebshareGatewaySecret) -> WebshareGatewaySecret:
    if not isinstance(profile, WebshareGatewaySecret):
        raise ProxyDenied("Webshare gateway secret record is invalid")
    try:
        return _profile(f"{profile.provider}/{profile.logical_name}", _profile_record(profile))
    except ProxyDenied:
        raise
    except (AttributeError, TypeError, ValueError):
        raise ProxyDenied("Webshare gateway secret record is invalid") from None


def _validated_expected_generation(expected_generation: int) -> int:
    return _strict_integer(
        expected_generation,
        "expected generation",
        minimum=1,
        maximum=MAX_GENERATION,
    )


class WebshareGatewaySecretStore:
    """Whole-file compare-and-swap storage for validated v2 records."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock_path = path.with_suffix(path.suffix + ".lock")

    @contextmanager
    def _lock(self) -> Iterator[None]:
        try:
            self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            parent = self.path.parent.lstat()
            if not stat.S_ISDIR(parent.st_mode) or parent.st_mode & 0o077:
                raise ProxyDenied("Webshare gateway secret directory must be private")
            flags = (
                os.O_CREAT
                | os.O_RDWR
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            )
            descriptor = os.open(self.lock_path, flags, 0o600)
        except ProxyDenied:
            raise
        except OSError:
            raise ProxyDenied("Webshare gateway secret lock cannot be opened safely") from None
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise ProxyDenied("Webshare gateway secret lock must be a regular file")
            os.fchmod(descriptor, 0o600)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX)
            except OSError:
                raise ProxyDenied("Webshare gateway secret lock cannot be acquired") from None
            yield
        finally:
            os.close(descriptor)

    def _read_current(self) -> dict[ProfileKey, WebshareGatewaySecret]:
        try:
            self.path.lstat()
        except FileNotFoundError:
            return {}
        return _parse_webshare_gateway_secrets(_read_private_file(self.path))

    def install(
        self,
        profile: WebshareGatewaySecret,
        *,
        expected_generation: int | None,
    ) -> int:
        """Create generation one or replace exactly one expected generation."""
        candidate = _validated_profile(profile)
        expected = (
            None
            if expected_generation is None
            else _validated_expected_generation(expected_generation)
        )
        with self._lock():
            profiles = self._read_current()
            current = profiles.get(candidate.key)
            if expected is None:
                if current is not None or candidate.generation != 1:
                    raise ProxyDenied("Webshare gateway secret create generation conflict")
            else:
                if (
                    current is None
                    or current.generation != expected
                    or expected == MAX_GENERATION
                    or candidate.generation != expected + 1
                ):
                    raise ProxyDenied("Webshare gateway secret update generation conflict")
            if current is None and len(profiles) >= MAX_PROFILES:
                raise ProxyDenied("Webshare gateway secret contains too many profiles")
            profiles[candidate.key] = candidate
            self._replace(profiles)
        return candidate.generation

    def remove(
        self,
        provider: str,
        logical_name: str,
        *,
        expected_generation: int,
    ) -> None:
        """Remove only the provider/name record at the expected generation."""
        expected = _validated_expected_generation(expected_generation)
        if provider != _PROVIDER or _LOGICAL_NAME.fullmatch(logical_name) is None:
            raise ProxyDenied("Webshare gateway secret profile identity is invalid")
        key = provider, logical_name
        with self._lock():
            profiles = self._read_current()
            current = profiles.get(key)
            if current is None or current.generation != expected:
                raise ProxyDenied("Webshare gateway secret remove generation conflict")
            del profiles[key]
            self._replace(profiles)

    def _replace(self, profiles: dict[ProfileKey, WebshareGatewaySecret]) -> None:
        raw = {
            "schema_version": SCHEMA_VERSION,
            "profiles": {
                f"{provider}/{logical_name}": _profile_record(profile)
                for (provider, logical_name), profile in profiles.items()
            },
        }
        contents = (json.dumps(raw, sort_keys=True, separators=(",", ":")) + "\n").encode()
        if len(contents) > MAX_SECRET_FILE_BYTES:
            raise ProxyDenied("Webshare gateway secret exceeds the size limit")
        # Validate the exact generation that will be installed before touching
        # the destination. This also keeps serialization changes fail closed.
        _parse_webshare_gateway_secrets(contents)

        temporary = self.path.with_name(f".{self.path.name}.{secrets.token_hex(8)}.tmp")
        flags = (
            os.O_CREAT
            | os.O_EXCL
            | os.O_WRONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptor: int | None = None
        try:
            descriptor = os.open(temporary, flags, 0o400)
            view = memoryview(contents)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise OSError("short write")
                view = view[written:]
            os.fchmod(descriptor, 0o400)
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = None
            os.replace(temporary, self.path)
            directory = os.open(
                self.path.parent,
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except OSError:
            raise ProxyDenied("Webshare gateway secret atomic replacement failed") from None
        finally:
            if descriptor is not None:
                os.close(descriptor)
            with suppress(FileNotFoundError):
                temporary.unlink()
