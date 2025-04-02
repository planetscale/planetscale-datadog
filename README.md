# Datadog PlanetScale Custom Check

## Overview

This Datadog Agent check integrates with the PlanetScale API to dynamically discover Prometheus scrape endpoints associated with database branches within a specified organization. It then utilizes the Agent's built-in OpenMetrics capabilities to collect metrics from these discovered endpoints.

This allows you to monitor metrics exposed by your PlanetScale database branches directly within Datadog.

## Prerequisites

*   **Datadog Agent:** Version 6 or 7 installed and running.
*   **PlanetScale Account:** Access to the PlanetScale organization you wish to monitor.
*   **PlanetScale Service Token:** A Service Token (ID and Secret) with `read_metrics_endpoints` permissions. Create one in your PlanetScale organization settings.

## Installation

1.  **Copy Check Files:**
    *   Place `planetscale.py` into your Datadog Agent's `checks.d` directory.
        *   Linux: `/etc/datadog-agent/checks.d/`
        *   macOS: `/opt/datadog-agent/etc/checks.d/`
        *   Windows: `C:\ProgramData\Datadog\checks.d\`
2.  **Copy Configuration File:**
    *   Copy the `conf.d/planetscale.yaml.example` file to your Agent's `conf.d` directory (e.g., `/etc/datadog-agent/conf.d/`) and rename it to `planetscale.yaml`.
3.  **Install Dependencies:**
    * Place the `requirements.txt` file somewhere accessible by your datadog-agent, such as /etc/datadog-agent/planetscale.txt
    *   Install the required Python packages into the Datadog Agent's embedded Python environment. Open a terminal with appropriate permissions and run:
        ```bash
        # Linux/macOS (adjust path if necessary)
        sudo -u dd-agent /opt/datadog-agent/embedded/bin/pip install -r /etc/datadog-agent/planetscale.txt

        # Windows (adjust path if necessary, run as Administrator)
        # "C:\Program Files\Datadog\Datadog Agent\embedded\python.exe" -m pip install -r "C:\ProgramData\Datadog\planetscale.txt"
        ```
4.  **Restart Datadog Agent:** Restart the Agent service to load the new check and configuration.

## Configuration

Edit the `conf.d/planetscale.yaml` file to configure the check:

```yaml
instances:
  - # Required: Your PlanetScale organization ID
    planetscale_organization: '<YOUR_PLANETSCALE_ORG_NAME>'

    # Required: Your PlanetScale Service Token ID
    ps_service_token_id: '<YOUR_PLANETSCALE_SERVICE_TOKEN_ID>'

    # Required: Your PlanetScale Service Token Secret
    # Consider using Datadog secrets management for production:
    # https://docs.datadoghq.com/agent/guide/secrets-management/
    ps_service_token_secret: '<YOUR_PLANETSCALE_SERVICE_TOKEN_SECRET>'

    # Required: Namespace for the metrics (prepended to metric names)
    namespace: 'planetscale'

    # Required: List of metrics to collect from discovered endpoints.
    # Use simple names or mappings for renaming/type overrides.
    metrics:
      - planetscale_vtgate_queries_duration: vtgate_query_duration # Example: Rename metric

      # Add other metrics exposed by the PlanetScale endpoints here
      # Example with type override:
      # - some_metric:
      #     name: my_metric_gauge
      #     type: gauge

    # Optional: Set the collection interval (in seconds)
    # Default is 15s. Set to 60s to match ~1 min metric updates from PlanetScale.
    min_collection_interval: 60
    send_distribution_buckets: true
    collect_counters_with_distributions: true

    # Optional OpenMetricsBaseCheck settings (applied to discovered endpoints)
    # tags:
    #   - 'static_tag:value' # Additional static tags added to all metrics
    # timeout: 5 # Override default timeout (10s) for scraping individual endpoints
    # ssl_verify: true # Set to false to disable SSL verification (not recommended)
    # prometheus_metrics_prefix: 'planetscale' # Optional prefix to remove from metric names
```

**Key Configuration Options:**

*   `planetscale_organization`: The ID of the PlanetScale organization to query.
*   `ps_service_token_id`, `ps_service_token_secret`: Credentials for authenticating with the PlanetScale API.
*   `namespace`: Prefix added to all collected metrics (e.g., `planetscale.cluster_size`).
*   `metrics`: A list defining which metrics to collect from the discovered Prometheus endpoints. You can rename metrics or override their types here.
*   `tags`: Optional static tags to add to all metrics collected by this instance. Discovered labels (like database and branch name) are automatically added as tags prefixed with `ps_`.

## Validation

1.  **Check Agent Status:** Run the Datadog Agent status command (e.g., `sudo -u dd-agent -- datadog-agent check planetscale`) and look for configuration errors.
2.  **Look for Metrics:** Search for metrics starting with `planetscale.` in your Datadog Metrics Explorer. Allow a few minutes for data to appear after restarting the Agent.
3.  **Check Service Checks:** Look for the following service checks:
    *   `planetscale.api.can_connect`: Reports the status of the connection to the PlanetScale API. Tags: `planetscale_org:<org_id>`.
    *   `planetscale.target.can_scrape`: Reports the status of scraping individual discovered endpoints. Tags include `prometheus_url` and discovered labels.

## Troubleshooting

*   **Configuration Errors:** Double-check `planetscale.yaml` for correct formatting, valid credentials, and organization ID. Check the Agent status report for specific errors.
*   **API Connection Issues:** Verify the Service Token ID and Secret are correct and have the necessary permissions in PlanetScale. Check the `planetscale.api.can_connect` service check status. Ensure the Datadog Agent host has network connectivity to `api.planetscale.com`.
*   **Scraping Errors:** Check the `planetscale.target.can_scrape` service check. Verify the discovered endpoints are accessible from the Agent host and are serving valid OpenMetrics/Prometheus data. Check Agent logs (`agent.log`, `collector.log`) for more detailed errors related to scraping.
*   **Missing Metrics:** Ensure the metric names in your `planetscale.yaml` `metrics` list exactly match the names exposed by the PlanetScale endpoints (after any potential `prometheus_metrics_prefix` removal). Verify the `namespace` is correct.
