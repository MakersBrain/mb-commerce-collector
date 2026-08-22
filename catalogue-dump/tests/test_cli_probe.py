from mb_ceramics_catalogue.cli.probe import build_parser


def test_probe_pipeline_selection_is_explicit_and_legacy_by_default() -> None:
    parser = build_parser()

    assert parser.parse_args(["shop"]).pipeline == "legacy"
    assert (
        parser.parse_args(["shop", "--pipeline", "connector_canary"]).pipeline
        == "connector_canary"
    )
