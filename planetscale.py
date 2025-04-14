import requests
from copy import deepcopy

from datadog_checks.base import AgentCheck, ConfigurationError
from datadog_checks.base.checks.openmetrics import OpenMetricsBaseCheckV2


class PlanetScaleCheck(OpenMetricsBaseCheckV2):
    DEFAULT_METRIC_LIMIT = (
        0  # By default, collect all metrics from discovered endpoints
    )

    def __init__(self, name, init_config, instances):
        # Set the default namespace
        if "namespace" not in instances[0]:
            instances[0]["namespace"] = "planetscale"

        # Create a modified instance config for initialization to satisfy the base class
        # We add a dummy prometheus_url which will be overridden later in check()
        init_instances = []
        for inst in instances:
            init_inst = deepcopy(inst)
            init_inst.setdefault(
                "openmetrics_endpoint", "http://localhost:1/dummy"
            )  # Dummy URL for V2
            init_instances.append(init_inst)

        # Initialize OpenMetricsBaseCheckV2
        # Pass the modified instances list for initialization
        super(PlanetScaleCheck, self).__init__(
            name,
            init_config,
            init_instances,  # Use instances with dummy URL for init
            default_metric_limit=self.DEFAULT_METRIC_LIMIT,
        )
        # Store original instances without the dummy URL for use in check()
        self.original_instances = instances

    def check(self, instance):
        # NOTE: The 'instance' passed here is from the original configuration (self.original_instances),
        #       NOT the one with the dummy URL used for initialization.
        #       The base class handles iterating through self.instances internally,
        #       but we need access to the original config values (like credentials).
        #       Let's retrieve the *original* instance based on some unique key if multiple instances are possible,
        #       or assume a single instance for simplicity if that matches the use case.
        #       For now, assuming the 'instance' passed IS the original one we need.
        #       If multiple instances are defined in planetscale.yaml, this needs refinement.

        # Get required configuration
        org_id = instance.get("planetscale_organization")
        token_id = instance.get("ps_service_token_id")
        token_secret = instance.get("ps_service_token_secret")

        if not org_id:
            raise ConfigurationError(
                "Missing 'planetscale_organization' in instance configuration."
            )
        if not token_id:
            raise ConfigurationError(
                "Missing 'ps_service_token_id' in instance configuration."
            )
        if not token_secret:
            raise ConfigurationError(
                "Missing 'ps_service_token_secret' in instance configuration."
            )

        # Get optional configuration
        req_timeout = instance.get(
            "timeout", self.init_config.get("timeout", 10)
        )  # Use instance timeout, then init_config, then default 10
        ssl_verify = instance.get("ssl_verify", True)

        # Construct API URL
        api_url = f"https://api.planetscale.com/v1/organizations/{org_id}/metrics"
        headers = {
            "Accept": "application/json",
            "Authorization": f"{token_id}:{token_secret}",
        }

        try:
            self.log.debug(f"Querying PlanetScale API: {api_url}")
            response = requests.get(
                api_url, headers=headers, timeout=req_timeout, verify=ssl_verify
            )
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            targets = response.json()
            self.log.debug(f"Received {len(targets)} targets from PlanetScale API.")

        except requests.exceptions.Timeout as e:
            self.service_check(
                "planetscale.api.can_connect",
                AgentCheck.CRITICAL,
                message=f"Timeout connecting to PlanetScale API endpoint {api_url}: {e}",
                tags=[f"planetscale_org:{org_id}"],
            )
            self.log.error(f"Timeout connecting to PlanetScale API: {e}")
            return
        except requests.exceptions.RequestException as e:
            self.service_check(
                "planetscale.api.can_connect",
                AgentCheck.CRITICAL,
                message=f"Error connecting to PlanetScale API endpoint {api_url}: {e}",
                tags=[f"planetscale_org:{org_id}"],
            )
            self.log.error(f"Error querying PlanetScale API: {e}")
            return
        except Exception as e:
            self.service_check(
                "planetscale.api.can_connect",
                AgentCheck.CRITICAL,
                message=f"Unexpected error querying PlanetScale API endpoint {api_url}: {e}",
                tags=[f"planetscale_org:{org_id}"],
            )
            self.log.error(f"Unexpected error querying PlanetScale API: {e}")
            return

        # If successful, report API connectivity
        self.service_check(
            "planetscale.api.can_connect",
            AgentCheck.OK,
            tags=[f"planetscale_org:{org_id}"],
        )

        # Process each discovered target using the OpenMetricsBaseCheck logic
        for target_config in targets:
            if not target_config.get("targets"):
                self.log.warning(
                    f"Skipping target due to missing 'targets' field: {target_config}"
                )
                continue

            # Create a dynamic instance configuration for this specific target
            dynamic_instance = deepcopy(
                instance
            )  # Start with a copy of the original instance config

            # --- Construct the full Prometheus URL ---
            base_target = target_config["targets"][
                0
            ]  # e.g., "hostname:port" or "http://hostname:port"
            labels = target_config.get("labels", {})

            # Ensure base_target includes scheme
            if not base_target.startswith(("http://", "https://")):
                # Default to http if not specified, adjust if needed
                base_target = f"https://{base_target}"

            # Get metrics path, default to /metrics
            metrics_path = labels.get("__metrics_path__", "/metrics")
            if not metrics_path.startswith("/"):
                metrics_path = f"/{metrics_path}"

            # Extract URL parameters (__param_*)
            url_params = {
                key.replace("__param_", ""): value
                for key, value in labels.items()
                if key.startswith("__param_")
            }
            query_string = requests.compat.urlencode(
                url_params
            )  # Use requests' helper for encoding

            # Combine parts
            final_url = f"{base_target.rstrip('/')}{metrics_path}"
            if query_string:
                final_url += f"?{query_string}"

            # In V2, we use openmetrics_endpoint instead of prometheus_url
            dynamic_instance["openmetrics_endpoint"] = final_url
            self.log.debug(
                f"Constructed scrape URL: {final_url} from target: {target_config}"
            )
            # --- End URL Construction ---

            # Merge labels from the target config into tags, excluding special __* labels
            discovered_labels = target_config.get("labels", {})
            dynamic_tags = dynamic_instance.get("tags", [])  # Get existing tags
            for key, value in discovered_labels.items():
                if not key.startswith(
                    "__"
                ):  # Exclude special labels like __metrics_path__, __param_*
                    # Prefix discovered labels to avoid conflicts and add clarity
                    dynamic_tags.append(f"ps_{key}:{value}")
            dynamic_instance["tags"] = list(set(dynamic_tags))  # Ensure uniqueness

            # Remove PlanetScale specific config keys as they are not needed by the base class processor
            dynamic_instance.pop("planetscale_organization", None)
            dynamic_instance.pop("ps_service_token_id", None)
            dynamic_instance.pop("ps_service_token_secret", None)

            try:
                # In V2, we use submit_openmetric_values instead of process
                scraper = self.get_scraper(dynamic_instance)
                if scraper:
                    self.submit_openmetric_values(scraper, dynamic_instance)
                else:
                    self.log.error(f"Failed to get scraper for endpoint: {final_url}")
                    raise Exception("Failed to initialize scraper")
            except Exception as e:
                self.log.error(
                    f"Error processing dynamic instance {dynamic_instance.get('openmetrics_endpoint')}: {e}"
                )
                # Add a service check for the specific target failure
                target_tags = dynamic_instance.get("tags", [])
                target_tags.append(
                    f"openmetrics_endpoint:{dynamic_instance.get('openmetrics_endpoint')}"
                )
                self.service_check(
                    "planetscale.target.can_scrape",
                    AgentCheck.CRITICAL,
                    message=str(e),
                    tags=target_tags,
                )
