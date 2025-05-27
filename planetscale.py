import requests
from copy import deepcopy
import concurrent.futures
from datadog_checks.base import AgentCheck, ConfigurationError
from datadog_checks.base.checks.openmetrics.v2.base import OpenMetricsBaseCheckV2


class PlanetScaleCheck(OpenMetricsBaseCheckV2):
    DEFAULT_METRIC_LIMIT = (
        0  # By default, collect all metrics from discovered endpoints
    )

    def __init__(self, name, init_config, instances):
        # Ensure the namespace is set for all instances
        init_instances = []
        for inst in instances:
            init_inst = deepcopy(inst)
            # Explicitly set namespace, defaulting to "planetscale" if not provided
            init_inst["namespace"] = init_inst.get("namespace", "planetscale")
            # Add a dummy openmetrics_endpoint to satisfy initial configuration
            # This will be overridden later when we discover real endpoints
            init_inst.setdefault("openmetrics_endpoint", "http://localhost:1/dummy")
            init_instances.append(init_inst)

        # Initialize with parent class
        super(PlanetScaleCheck, self).__init__(name, init_config, init_instances)

    def check(self, instance):
        # Get required configuration directly from the instance
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
        req_timeout = instance.get("timeout", self.init_config.get("timeout", 10))
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
            response.raise_for_status()
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

        # Configure scrapers for each target
        self.scrape_planetscale_targets(instance, targets)

    def scrape_planetscale_targets(self, instance, targets):
        # Set max workers - adjust based on your needs
        max_workers = instance.get("max_concurrent_requests", 5)
        
        # Create a list to store target configurations for parallel processing
        scrape_configs = []
        
        # Process each discovered target
        for target_config in targets:
            if not target_config.get("targets"):
                self.log.warning(
                    f"Skipping target due to missing 'targets' field: {target_config}"
                )
                continue
                
            # Create dynamic instance configuration (existing code)
            dynamic_instance = {
                # OpenMetrics V2 configuration keys
                "namespace": instance.get("namespace", "planetscale"),
                "metrics": instance.get("metrics", []),
                "exclude_metrics": instance.get("exclude_metrics", []),
                "metadata_metrics": instance.get("metadata_metrics", []),
                "metadata_label_map": instance.get("metadata_label_map", {}),
                "prometheus_metrics_prefix": instance.get(
                    "prometheus_metrics_prefix", ""
                ),
                "label_joins": instance.get("label_joins", {}),
                "labels_mapper": instance.get("labels_mapper", {}),
                "type_overrides": instance.get("type_overrides", {}),
                "histogram_buckets_as_distributions": instance.get(
                    "histogram_buckets_as_distributions", True
                ),
                "non_cumulative_histogram_buckets": instance.get(
                    "non_cumulative_histogram_buckets", False
                ),
                "raw_metric_prefix": instance.get("raw_metric_prefix", ""),
                "cache_metric_wildcards": instance.get("cache_metric_wildcards", True),
                "monotonic_counter": instance.get("monotonic_counter", True),
                "telemetry": instance.get("telemetry", True),
                "ignore_tags": instance.get("ignore_tags", []),
                "remap_metric_names": instance.get("remap_metric_names", True),
                # Other useful configuration
                "tags": list(instance.get("tags", [])),
                "ssl_verify": instance.get("ssl_verify", True),
                "ssl_cert": instance.get("ssl_cert", None),
                "ssl_private_key": instance.get("ssl_private_key", None),
                "ssl_ca_cert": instance.get("ssl_ca_cert", None),
                "timeout": instance.get("timeout", 10),
            }

            # --- Construct the full endpoint URL ---
            base_target = target_config["targets"][0]
            labels = target_config.get("labels", {})

            # Ensure base_target includes scheme
            if not base_target.startswith(("http://", "https://")):
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
            query_string = requests.compat.urlencode(url_params)

            # Combine parts
            final_url = f"{base_target.rstrip('/')}{metrics_path}"
            if query_string:
                final_url += f"?{query_string}"

            # Set the openmetrics_endpoint for the scraper config
            dynamic_instance["openmetrics_endpoint"] = final_url

            # Disable tagging metrics with the endpoint
            dynamic_instance["tag_by_endpoint"] = False

            # Log configuration details for debugging
            self.log.debug(
                f"Constructed scrape URL: {final_url} from target: {target_config}"
            )
            self.log.debug(f"Using namespace: {dynamic_instance['namespace']}")
            self.log.debug(f"Configured metrics: {dynamic_instance['metrics']}")

            # Merge labels from the target config into tags, excluding special __* labels
            discovered_labels = target_config.get("labels", {})
            dynamic_tags = dynamic_instance.get("tags", []) or []
            for key, value in discovered_labels.items():
                if not key.startswith("__"):
                    dynamic_tags.append(f"{key}:{value}")
            dynamic_instance["tags"] = list(set(dynamic_tags))

            # Add to scrape configs instead of scraping immediately
            scrape_configs.append(dynamic_instance)
        
        # Execute scrapes in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self._scrape_single_target, config) for config in scrape_configs]
            # Wait for all to complete
            concurrent.futures.wait(futures)
            
    def _scrape_single_target(self, dynamic_instance):
        try:
            # Set the namespace for this scraper
            self.__NAMESPACE__ = dynamic_instance["namespace"]
            
            # Create the scraper using the base class method
            scraper = self.create_scraper(dynamic_instance)
            
            # Log the scraper's namespace for debugging
            self.log.debug(f"Scraper namespace: {scraper.namespace}")
            self.log.debug(f"Check namespace: {self.__NAMESPACE__}")
            
            # Perform the actual scraping
            endpoint = dynamic_instance.get("openmetrics_endpoint", "unknown")
            self.log.debug(f"Scraping metrics from {endpoint}")
            scraper.scrape()
            
        except Exception as e:
            endpoint = dynamic_instance.get("openmetrics_endpoint", "unknown")
            self.log.error(f"Error scraping metrics from {endpoint}: {e}")
            target_tags = dynamic_instance.get("tags", [])
            self.service_check(
                "planetscale.target.can_scrape",
                AgentCheck.CRITICAL,
                message=str(e),
                tags=target_tags,
            )
