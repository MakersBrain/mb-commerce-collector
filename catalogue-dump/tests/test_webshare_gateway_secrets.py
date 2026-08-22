import json
import os
from concurrent.futures import ThreadPoolExecutor

import pytest

import mb_ceramics_catalogue.webshare_gateway_secrets as gateway_secrets
from mb_ceramics_catalogue.proxy import ProxyDenied
from mb_ceramics_catalogue.webshare_gateway_secrets import (
    MAX_GENERATION,
    MAX_SECRET_FILE_BYTES,
    WebshareGatewaySecretStore,
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
    if path.exists():
        os.chmod(path, 0o600)
    path.write_text(json.dumps(root), encoding="utf-8")
    os.chmod(path, 0o400)


def validated_profile(
    tmp_path,
    *,
    logical_name="primary",
    generation=1,
    username="issued-user",
    password="issued-password",
):
    path = tmp_path / "candidate.json"
    key = f"webshare/{logical_name}"
    write_secret(
        path,
        {
            key: record(
                logical_name=logical_name,
                generation=generation,
                credentials={"username": username, "password": password},
            )
        },
    )
    return load_webshare_gateway_secrets(path)[("webshare", logical_name)]


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


def test_store_creates_generation_one_as_a_private_canonical_file(tmp_path):
    target = tmp_path / "store" / "webshare.json"
    candidate = validated_profile(tmp_path)

    generation = WebshareGatewaySecretStore(target).install(
        candidate, expected_generation=None
    )

    assert generation == 1
    assert os.stat(target).st_mode & 0o777 == 0o400
    installed = load_webshare_gateway_secrets(target)[("webshare", "primary")]
    assert installed.username.get_secret_value() == "issued-user"
    assert json.loads(target.read_text())["schema_version"] == 2


def test_store_rejects_create_conflicts_without_exposing_or_replacing_credentials(tmp_path):
    target = tmp_path / "webshare.json"
    store = WebshareGatewaySecretStore(target)
    store.install(validated_profile(tmp_path, password="first-secret"), expected_generation=None)

    with pytest.raises(ProxyDenied, match="create generation conflict") as raised:
        store.install(
            validated_profile(tmp_path, password="second-secret"), expected_generation=None
        )

    assert "first-secret" not in str(raised.value)
    assert "second-secret" not in str(raised.value)
    installed = load_webshare_gateway_secrets(target)[("webshare", "primary")]
    assert installed.password.get_secret_value() == "first-secret"


@pytest.mark.parametrize("expected_generation", [True, 0, -1, MAX_GENERATION + 1])
def test_store_requires_a_strict_expected_generation_for_update(expected_generation, tmp_path):
    target = tmp_path / "webshare.json"
    store = WebshareGatewaySecretStore(target)
    store.install(validated_profile(tmp_path), expected_generation=None)
    replacement = validated_profile(tmp_path, generation=2, password="new-credential-value")

    with pytest.raises(ProxyDenied, match="expected generation"):
        store.install(replacement, expected_generation=expected_generation)


def test_store_compare_and_swap_rotates_one_record_and_preserves_others(tmp_path):
    target = tmp_path / "webshare.json"
    store = WebshareGatewaySecretStore(target)
    store.install(validated_profile(tmp_path, password="primary-one"), expected_generation=None)
    store.install(
        validated_profile(
            tmp_path,
            logical_name="secondary",
            username="second-user",
            password="secondary-one",
        ),
        expected_generation=None,
    )

    assert store.install(
        validated_profile(tmp_path, generation=2, password="primary-two"),
        expected_generation=1,
    ) == 2

    installed = load_webshare_gateway_secrets(target)
    assert installed[("webshare", "primary")].generation == 2
    assert installed[("webshare", "primary")].password.get_secret_value() == "primary-two"
    assert installed[("webshare", "secondary")].generation == 1
    assert installed[("webshare", "secondary")].password.get_secret_value() == "secondary-one"


@pytest.mark.parametrize(
    ("expected_generation", "candidate_generation"),
    [(2, 2), (1, 1), (1, 3)],
)
def test_store_rejects_stale_or_non_sequential_updates_without_replacement(
    expected_generation, candidate_generation, tmp_path
):
    target = tmp_path / "webshare.json"
    store = WebshareGatewaySecretStore(target)
    store.install(validated_profile(tmp_path, password="original"), expected_generation=None)

    with pytest.raises(ProxyDenied, match="update generation conflict"):
        store.install(
            validated_profile(
                tmp_path, generation=candidate_generation, password="must-not-install"
            ),
            expected_generation=expected_generation,
        )

    installed = load_webshare_gateway_secrets(target)[("webshare", "primary")]
    assert installed.generation == 1
    assert installed.password.get_secret_value() == "original"


def test_store_rejects_update_when_profile_does_not_exist(tmp_path):
    target = tmp_path / "webshare.json"
    with pytest.raises(ProxyDenied, match="update generation conflict"):
        WebshareGatewaySecretStore(target).install(
            validated_profile(tmp_path, generation=2), expected_generation=1
        )
    assert not target.exists()


def test_store_remove_is_provider_keyed_generation_guarded_and_preserves_other_records(tmp_path):
    target = tmp_path / "webshare.json"
    store = WebshareGatewaySecretStore(target)
    store.install(validated_profile(tmp_path), expected_generation=None)
    store.install(
        validated_profile(tmp_path, logical_name="secondary", password="keep-me"),
        expected_generation=None,
    )

    with pytest.raises(ProxyDenied, match="profile identity"):
        store.remove("decodo", "primary", expected_generation=1)
    with pytest.raises(ProxyDenied, match="remove generation conflict"):
        store.remove("webshare", "primary", expected_generation=2)
    store.remove("webshare", "primary", expected_generation=1)

    installed = load_webshare_gateway_secrets(target)
    assert set(installed) == {("webshare", "secondary")}
    assert installed[("webshare", "secondary")].password.get_secret_value() == "keep-me"


def test_store_reads_and_parses_the_current_file_once_inside_the_cas(monkeypatch, tmp_path):
    target = tmp_path / "webshare.json"
    store = WebshareGatewaySecretStore(target)
    store.install(validated_profile(tmp_path), expected_generation=None)
    replacement = validated_profile(tmp_path, generation=2)
    original_read = gateway_secrets._read_private_file
    reads = []

    def counted_read(path):
        reads.append(path)
        return original_read(path)

    monkeypatch.setattr(gateway_secrets, "_read_private_file", counted_read)
    store.install(replacement, expected_generation=1)

    assert reads == [target]


def test_store_atomic_replace_failure_leaves_current_generation_and_removes_temp(
    monkeypatch, tmp_path
):
    target = tmp_path / "webshare.json"
    store = WebshareGatewaySecretStore(target)
    store.install(validated_profile(tmp_path, password="original"), expected_generation=None)
    replacement = validated_profile(
        tmp_path, generation=2, password="new-credential-value"
    )

    def fail_replace(source, destination):
        raise OSError("simulated replacement failure")

    monkeypatch.setattr(gateway_secrets.os, "replace", fail_replace)
    with pytest.raises(ProxyDenied, match="atomic replacement failed") as raised:
        store.install(replacement, expected_generation=1)

    assert "original" not in str(raised.value)
    assert "new-credential-value" not in str(raised.value)
    installed = load_webshare_gateway_secrets(target)[("webshare", "primary")]
    assert installed.generation == 1
    assert installed.password.get_secret_value() == "original"
    assert list(target.parent.glob(f".{target.name}.*.tmp")) == []


def test_store_fsyncs_the_private_file_and_containing_directory(monkeypatch, tmp_path):
    target = tmp_path / "webshare.json"
    candidate = validated_profile(tmp_path)
    original_fsync = gateway_secrets.os.fsync
    synced_directory_flags = []

    def traced_fsync(descriptor):
        synced_directory_flags.append(
            gateway_secrets.stat.S_ISDIR(os.fstat(descriptor).st_mode)
        )
        original_fsync(descriptor)

    monkeypatch.setattr(gateway_secrets.os, "fsync", traced_fsync)
    WebshareGatewaySecretStore(target).install(candidate, expected_generation=None)

    assert synced_directory_flags == [False, True]


def test_store_serializes_concurrent_compare_and_swap_updates(tmp_path):
    target = tmp_path / "webshare.json"
    store = WebshareGatewaySecretStore(target)
    store.install(validated_profile(tmp_path), expected_generation=None)
    first = validated_profile(tmp_path, generation=2, password="first-candidate")
    second = validated_profile(tmp_path, generation=2, password="second-candidate")

    def rotate(candidate):
        try:
            return store.install(candidate, expected_generation=1)
        except ProxyDenied:
            return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(rotate, (first, second)))

    assert sorted(result for result in results if result is not None) == [2]
    installed = load_webshare_gateway_secrets(target)[("webshare", "primary")]
    assert installed.generation == 2
    assert installed.password.get_secret_value() in {"first-candidate", "second-candidate"}


def test_store_refuses_a_symlink_lock_without_touching_its_target(tmp_path):
    target = tmp_path / "webshare.json"
    victim = tmp_path / "victim"
    victim.write_text("do-not-touch")
    lock = target.with_suffix(target.suffix + ".lock")
    lock.symlink_to(victim)

    with pytest.raises(ProxyDenied, match="lock cannot be opened safely"):
        WebshareGatewaySecretStore(target).install(
            validated_profile(tmp_path), expected_generation=None
        )

    assert victim.read_text() == "do-not-touch"
    assert not target.exists()
