import json
import os

import pytest

from mb_ceramics_catalogue.proxy import ProxyDenied
from mb_ceramics_catalogue.webshare_gateway_secrets import (
    MAX_GENERATION,
    MAX_SECRET_FILE_BYTES,
    load_webshare_gateway_secrets,
    secret_values,
)


def record(**changes: object) -> dict[str, object]:
    value: dict[str, object] = {
        "provider": "webshare",
        "logical_name": "primary",
        "generation": 1,
        "gateway": {
            "endpoint_id": "webshare-residential-backbone",
            "protocol": "http",
            "host": "p.webshare.io",
            "port": 80,
        },
        "credentials": {"username": "issued-user", "password": "issued:@password"},
        "capabilities": {
            "countries": ["FR", "US"],
            "sticky_session_ttl_seconds": 1_800,
        },
    }
    value.update(changes)
    return value


def write_secret(path, profiles=None, **root_changes):
    root = {
        "schema_version": 2,
        "profiles": profiles if profiles is not None else {"webshare/primary": record()},
    }
    root.update(root_changes)
    path.write_text(json.dumps(root), encoding="utf-8")
    os.chmod(path, 0o400)


def test_loads_provider_keyed_secret_and_exposes_only_explicit_redaction_values(tmp_path):
    path = tmp_path / "webshare.json"
    write_secret(path)

    profiles = load_webshare_gateway_secrets(path)

    profile = profiles[("webshare", "primary")]
    assert profile.generation == 1
    assert profile.host == "p.webshare.io"
    assert profile.countries == frozenset({"FR", "US"})
    assert "issued-user" not in repr(profile)
    assert "issued:@password" not in repr(profile)
    assert secret_values(profiles) == {"issued-user", "issued:@password"}


@pytest.mark.parametrize(
    ("root_changes", "match"),
    [
        ({"unexpected": True}, "root.*invalid fields"),
        ({"schema_version": True}, "schema version"),
        ({"schema_version": 1}, "schema version"),
        ({"profiles": []}, "profiles must be an object"),
    ],
)
def test_rejects_non_exact_root(root_changes, match, tmp_path):
    path = tmp_path / "webshare.json"
    write_secret(path, **root_changes)
    with pytest.raises(ProxyDenied, match=match):
        load_webshare_gateway_secrets(path)


def test_rejects_duplicate_json_keys(tmp_path):
    path = tmp_path / "webshare.json"
    path.write_text('{"schema_version":2,"schema_version":2,"profiles":{}}')
    os.chmod(path, 0o400)
    with pytest.raises(ProxyDenied, match="strict JSON"):
        load_webshare_gateway_secrets(path)


@pytest.mark.parametrize(
    "profiles",
    [
        {"webshare/other": record()},
        {"decodo/primary": record(provider="decodo")},
        {"webshare/primary": record(extra="forbidden")},
        {"webshare/primary": record(logical_name="Invalid Name")},
    ],
)
def test_rejects_unbound_or_non_exact_profile_records(profiles, tmp_path):
    path = tmp_path / "webshare.json"
    write_secret(path, profiles)
    with pytest.raises(ProxyDenied):
        load_webshare_gateway_secrets(path)


@pytest.mark.parametrize("generation", [True, 0, -1, MAX_GENERATION + 1, 1.0, "1"])
def test_generation_is_a_strict_bounded_integer(generation, tmp_path):
    path = tmp_path / "webshare.json"
    write_secret(path, {"webshare/primary": record(generation=generation)})
    with pytest.raises(ProxyDenied, match="generation"):
        load_webshare_gateway_secrets(path)


@pytest.mark.parametrize(
    ("credentials", "match"),
    [
        ({"username": "", "password": "valid"}, "username"),
        ({"username": "valid", "password": ""}, "password"),
        ({"username": "bad\nuser", "password": "valid"}, "control characters"),
        ({"username": "valid", "password": "bad\x00password"}, "control characters"),
        ({"username": 123, "password": "valid"}, "username"),
        ({"username": "valid", "password": "x" * 1_025}, "password"),
        ({"username": "valid", "password": "valid", "token": "forbidden"}, "invalid fields"),
    ],
)
def test_credentials_are_exact_bounded_text(credentials, match, tmp_path):
    path = tmp_path / "webshare.json"
    write_secret(path, {"webshare/primary": record(credentials=credentials)})
    with pytest.raises(ProxyDenied, match=match):
        load_webshare_gateway_secrets(path)


@pytest.mark.parametrize(
    "gateway",
    [
        {
            "endpoint_id": "other",
            "protocol": "http",
            "host": "p.webshare.io",
            "port": 80,
        },
        {
            "endpoint_id": "webshare-residential-backbone",
            "protocol": "https",
            "host": "p.webshare.io",
            "port": 80,
        },
        {
            "endpoint_id": "webshare-residential-backbone",
            "protocol": "http",
            "host": "attacker.example",
            "port": 80,
        },
        {
            "endpoint_id": "webshare-residential-backbone",
            "protocol": "http",
            "host": "p.webshare.io",
            "port": 443,
        },
        {
            "endpoint_id": "webshare-residential-backbone",
            "protocol": "http",
            "host": "p.webshare.io",
            "port": True,
        },
    ],
)
def test_accepts_only_the_verified_webshare_http_gateway(gateway, tmp_path):
    path = tmp_path / "webshare.json"
    write_secret(path, {"webshare/primary": record(gateway=gateway)})
    with pytest.raises(ProxyDenied):
        load_webshare_gateway_secrets(path)


@pytest.mark.parametrize("port", [80, 1_080, 3_128, 9_999, 19_999])
def test_accepts_every_verified_webshare_http_gateway_port(port, tmp_path):
    path = tmp_path / "webshare.json"
    gateway = record()["gateway"]
    assert isinstance(gateway, dict)
    gateway["port"] = port
    write_secret(path, {"webshare/primary": record(gateway=gateway)})

    assert load_webshare_gateway_secrets(path)[("webshare", "primary")].port == port


@pytest.mark.parametrize(
    "capabilities",
    [
        {"countries": ["fr"], "sticky_session_ttl_seconds": 1_800},
        {"countries": ["FR", "FR"], "sticky_session_ttl_seconds": 1_800},
        {"countries": ["FRA"], "sticky_session_ttl_seconds": 1_800},
        {"countries": "FR", "sticky_session_ttl_seconds": 1_800},
        {"countries": ["FR"], "sticky_session_ttl_seconds": True},
        {"countries": ["FR"], "sticky_session_ttl_seconds": 59},
        {"countries": ["FR"], "sticky_session_ttl_seconds": 86_401},
        {"countries": ["FR"], "sticky_session_ttl_seconds": 1_800, "extra": True},
    ],
)
def test_capabilities_require_unique_uppercase_countries_and_explicit_bounded_ttl(
    capabilities, tmp_path
):
    path = tmp_path / "webshare.json"
    write_secret(path, {"webshare/primary": record(capabilities=capabilities)})
    with pytest.raises(ProxyDenied):
        load_webshare_gateway_secrets(path)


@pytest.mark.parametrize("mode", [0o000, 0o200, 0o440, 0o600 | 0o004, 0o644])
def test_file_permissions_are_exactly_private(mode, tmp_path):
    path = tmp_path / "webshare.json"
    write_secret(path)
    os.chmod(path, mode)
    with pytest.raises(ProxyDenied, match="permissions"):
        load_webshare_gateway_secrets(path)


def test_rejects_symlinks_non_regular_files_and_oversized_files(tmp_path):
    target = tmp_path / "target.json"
    write_secret(target)
    link = tmp_path / "link.json"
    link.symlink_to(target)
    with pytest.raises(ProxyDenied, match="symbolic link"):
        load_webshare_gateway_secrets(link)

    directory = tmp_path / "directory"
    directory.mkdir(mode=0o700)
    with pytest.raises(ProxyDenied, match="regular file"):
        load_webshare_gateway_secrets(directory)

    oversized = tmp_path / "oversized.json"
    oversized.write_bytes(b"x" * (MAX_SECRET_FILE_BYTES + 1))
    os.chmod(oversized, 0o400)
    with pytest.raises(ProxyDenied, match="size limit"):
        load_webshare_gateway_secrets(oversized)
