from unittest.mock import MagicMock, patch

import pytest
import requests
from datadog_checks.base import AgentCheck, ConfigurationError

from planetscale import PlanetScaleCheck


def make_instance(**overrides):
    instance = {
        "planetscale_organization": "test-org",
        "ps_service_token_id": "id",
        "ps_service_token_secret": "secret",
        "namespace": "planetscale",
        "metrics": [".*"],
    }
    instance.update(overrides)
    return instance


def make_target(
    database_name="my-db",
    branch_name="main",
    metrics_path="/metrics/branch/abc",
    host="metrics.planetscale.com",
    extra_labels=None,
):
    labels = {
        "__metrics_path__": metrics_path,
        "__param_sig": "sig-value",
        "__param_exp": "12345",
        "__scheme__": "https",
        "planetscale_database_name": database_name,
        "planetscale_branch_name": branch_name,
        "planetscale_organization_name": "test-org",
    }
    if extra_labels:
        labels.update(extra_labels)
    return {"targets": [host], "labels": labels}


@pytest.fixture
def check():
    return PlanetScaleCheck("planetscale", {}, [make_instance()])


@pytest.fixture
def capture_scraper_configs(check):
    def run(instance, targets):
        captured = []

        def side_effect(config):
            captured.append(config)
            return MagicMock()

        with patch.object(check, "create_scraper", side_effect=side_effect):
            check.scrape_planetscale_targets(instance, targets)
        return captured

    return run


def _find_config_for_db(configs, db_name):
    return next(
        c for c in configs if f"planetscale_database_name:{db_name}" in c["tags"]
    )


class TestDatabaseTags:
    def test_applies_tags_for_matching_database(self, capture_scraper_configs):
        instance = make_instance(database_tags={"my-db": ["env:prod"]})
        configs = capture_scraper_configs(instance, [make_target(database_name="my-db")])
        assert "env:prod" in configs[0]["tags"]

    def test_skips_non_matching_database(self, capture_scraper_configs):
        instance = make_instance(database_tags={"other-db": ["env:prod"]})
        configs = capture_scraper_configs(instance, [make_target(database_name="my-db")])
        assert "env:prod" not in configs[0]["tags"]

    def test_additive_with_instance_level_tags(self, capture_scraper_configs):
        instance = make_instance(
            tags=["team:infra"],
            database_tags={"my-db": ["env:prod"]},
        )
        configs = capture_scraper_configs(instance, [make_target(database_name="my-db")])
        assert "team:infra" in configs[0]["tags"]
        assert "env:prod" in configs[0]["tags"]

    def test_supports_multiple_tags_per_database(self, capture_scraper_configs):
        instance = make_instance(
            database_tags={"my-db": ["env:prod", "team:billing", "tier:critical"]},
        )
        configs = capture_scraper_configs(instance, [make_target(database_name="my-db")])
        for tag in ("env:prod", "team:billing", "tier:critical"):
            assert tag in configs[0]["tags"]

    def test_applies_only_to_matching_target_in_mixed_set(self, capture_scraper_configs):
        instance = make_instance(database_tags={"prod-db": ["env:prod"]})
        targets = [
            make_target(database_name="prod-db"),
            make_target(database_name="staging-db"),
        ]
        configs = capture_scraper_configs(instance, targets)
        assert "env:prod" in _find_config_for_db(configs, "prod-db")["tags"]
        assert "env:prod" not in _find_config_for_db(configs, "staging-db")["tags"]

    def test_absent_config_is_noop(self, capture_scraper_configs):
        configs = capture_scraper_configs(make_instance(), [make_target()])
        assert not any(t.startswith("env:") for t in configs[0]["tags"])

    def test_null_config_is_noop(self, capture_scraper_configs):
        configs = capture_scraper_configs(
            make_instance(database_tags=None), [make_target()]
        )
        assert not any(t.startswith("env:") for t in configs[0]["tags"])

    def test_normalizes_single_tag_string(self, capture_scraper_configs):
        configs = capture_scraper_configs(
            make_instance(database_tags={"my-db": "env:prod"}),
            [make_target(database_name="my-db")],
        )
        assert "env:prod" in configs[0]["tags"]
        assert not any(len(tag) == 1 for tag in configs[0]["tags"])

    def test_invalid_tag_value_raises(self, capture_scraper_configs):
        with pytest.raises(ConfigurationError, match="database_tags\\[my-db\\]"):
            capture_scraper_configs(
                make_instance(database_tags={"my-db": None}),
                [make_target(database_name="my-db")],
            )

    def test_invalid_top_level_shape_raises(self, capture_scraper_configs):
        with pytest.raises(ConfigurationError, match="database_tags"):
            capture_scraper_configs(
                make_instance(database_tags=["env:prod"]),
                [make_target(database_name="my-db")],
            )


class TestTargetConfig:
    def test_discovered_labels_become_tags(self, capture_scraper_configs):
        configs = capture_scraper_configs(
            make_instance(), [make_target(database_name="my-db", branch_name="main")]
        )
        tags = configs[0]["tags"]
        assert "planetscale_database_name:my-db" in tags
        assert "planetscale_branch_name:main" in tags
        assert "planetscale_organization_name:test-org" in tags

    def test_meta_labels_are_not_promoted_to_tags(self, capture_scraper_configs):
        configs = capture_scraper_configs(make_instance(), [make_target()])
        tags = configs[0]["tags"]
        assert not any(t.startswith("__") for t in tags)

    def test_builds_url_from_host_path_and_params(self, capture_scraper_configs):
        configs = capture_scraper_configs(
            make_instance(),
            [make_target(metrics_path="/metrics/branch/xyz", host="api.example.com")],
        )
        url = configs[0]["openmetrics_endpoint"]
        assert url.startswith("https://api.example.com/metrics/branch/xyz")
        assert "sig=sig-value" in url
        assert "exp=12345" in url

    def test_adds_scheme_when_host_missing_scheme(self, capture_scraper_configs):
        configs = capture_scraper_configs(
            make_instance(), [make_target(host="api.example.com")]
        )
        assert configs[0]["openmetrics_endpoint"].startswith("https://")

    def test_skips_target_with_no_targets_field(self, capture_scraper_configs):
        configs = capture_scraper_configs(
            make_instance(), [{"labels": {"planetscale_database_name": "x"}}]
        )
        assert configs == []


class TestCheckValidation:
    def test_missing_organization_raises(self, check):
        instance = make_instance()
        del instance["planetscale_organization"]
        with pytest.raises(ConfigurationError, match="planetscale_organization"):
            check.check(instance)

    def test_missing_token_id_raises(self, check):
        instance = make_instance()
        del instance["ps_service_token_id"]
        with pytest.raises(ConfigurationError, match="ps_service_token_id"):
            check.check(instance)

    def test_missing_token_secret_raises(self, check):
        instance = make_instance()
        del instance["ps_service_token_secret"]
        with pytest.raises(ConfigurationError, match="ps_service_token_secret"):
            check.check(instance)


class TestApiConnectivity:
    def test_api_failure_emits_critical_service_check(self, check):
        with patch("planetscale.requests.get") as mock_get, patch.object(
            check, "service_check"
        ) as mock_sc:
            mock_get.side_effect = requests.exceptions.ConnectionError("nope")
            check.check(make_instance())
            names_and_statuses = [(c.args[0], c.args[1]) for c in mock_sc.call_args_list]
            assert ("planetscale.api.can_connect", AgentCheck.CRITICAL) in names_and_statuses

    def test_api_success_emits_ok_service_check(self, check):
        response = MagicMock()
        response.json.return_value = []
        response.raise_for_status.return_value = None
        with patch("planetscale.requests.get", return_value=response), patch.object(
            check, "service_check"
        ) as mock_sc:
            check.check(make_instance())
            names_and_statuses = [(c.args[0], c.args[1]) for c in mock_sc.call_args_list]
            assert ("planetscale.api.can_connect", AgentCheck.OK) in names_and_statuses
