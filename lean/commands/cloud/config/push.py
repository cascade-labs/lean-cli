# QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
# Lean CLI v1.0. Copyright 2021 QuantConnect Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json

from click import command, option
from lean.click import LeanCommand
from lean.container import container


@command(cls=LeanCommand, requires_lean_config=True)
@option("--name", type=str, default="default",
        help="Config name (default: 'default')")
@option("--project-id", type=str, default=None,
        help="Project UUID for project-specific config (global if not specified)")
@option("--description", type=str, default="",
        help="Optional description for the config")
@option("--dry-run", is_flag=True, default=False,
        help="Show what would be pushed without actually pushing")
def push(name: str, project_id: str, description: str, dry_run: bool) -> None:
    """Push local lean.json configuration to the data server.

    This uploads all configuration keys from the local lean.json file,
    including data provider settings, API keys, and algorithm settings.

    The only value that should be overridden locally is LEAN_DATA_PATH
    via environment variable (for local data caching).
    """
    logger = container.logger
    lean_config_manager = container.lean_config_manager
    cli_config_manager = container.cli_config_manager

    # Get the local lean config
    lean_config_path = lean_config_manager.get_lean_config_path()
    logger.info(f"Reading lean config from: {lean_config_path}")

    with open(lean_config_path, "r") as f:
        config_data = json.load(f)

    # Merge credentials from CLI credentials storage into the config.
    # These are stored separately (via `lean config set`) and won't be in lean.json.
    cli_credentials = {
        "job-user-id": cli_config_manager.user_id.get_value(),
        "api-access-token": cli_config_manager.api_token.get_value(),
        "thetadata-url": cli_config_manager.thetadata_url.get_value(),
        "thetadata-api-key": cli_config_manager.thetadata_api_key.get_value(),
        "kalshi-api-key": cli_config_manager.kalshi_api_key.get_value(),
        "kalshi-private-key": cli_config_manager.kalshi_private_key.get_value(),
        "s3-access-key": cli_config_manager.s3_access_key.get_value(),
        "s3-secret-key": cli_config_manager.s3_secret_key.get_value(),
        "s3-endpoint": cli_config_manager.s3_endpoint.get_value(),
        "tradealert-s3-bucket": cli_config_manager.tradealert_s3_bucket.get_value(),
        "s3-region": cli_config_manager.s3_region.get_value(),
        "polygon-api-key": cli_config_manager.polygon_api_key.get_value(),
        "hyperliquid-aws-access-key-id": cli_config_manager.hyperliquid_aws_access_key_id.get_value(),
        "hyperliquid-aws-secret-access-key": cli_config_manager.hyperliquid_aws_secret_access_key.get_value(),
        "container-registry": cli_config_manager.container_registry.get_value(),
        "container-registry-namespace": cli_config_manager.container_registry_namespace.get_value(),
        "container-registry-username": cli_config_manager.container_registry_username.get_value(),
        "container-registry-token": cli_config_manager.container_registry_token.get_value(),
        "security-data-feeds": cli_config_manager.security_data_feeds.get_value(),
    }

    credentials_merged = 0
    for key, value in cli_credentials.items():
        if value and config_data.get(key, "") == "":
            config_data[key] = value
            credentials_merged += 1

    if credentials_merged > 0:
        logger.info(f"Merged {credentials_merged} credential(s) from CLI credentials storage")

    # Identify sensitive keys for user awareness
    sensitive_keys = [
        "job-user-id", "api-access-token",
        "thetadata-api-key", "thetadata-auth-token",
        "kalshi-api-key", "kalshi-private-key",
        "s3-access-key", "s3-secret-key",
        "polygon-api-key",
        "hyperliquid-aws-access-key-id", "hyperliquid-aws-secret-access-key",
        "container-registry-token",
        "alpaca-access-token", "ib-password", "oanda-access-token",
        "tradier-access-token", "tiingo-auth-token", "nasdaq-auth-token",
        "fxcm-password", "eze-password", "samco-client-password",
        "tastytrade-password", "zerodha-access-token",
        "trade-station-refresh-token", "tt-session-password",
        "us-energy-information-auth-token",
    ]
    found_sensitive = [k for k in sensitive_keys if k in config_data and config_data.get(k)]

    if dry_run:
        logger.info(f"[DRY RUN] Would push config '{name}' with {len(config_data)} keys")
        if project_id:
            logger.info(f"[DRY RUN] Project ID: {project_id}")
        if found_sensitive:
            logger.info(f"[DRY RUN] Config includes sensitive keys: {', '.join(found_sensitive)}")

        logger.info("[DRY RUN] Keys to push:")
        for key in sorted(config_data.keys()):
            value = config_data[key]
            if key in sensitive_keys:
                display_value = "***" if value else "(empty)"
            elif isinstance(value, dict):
                display_value = "{...}"
            elif isinstance(value, list):
                display_value = f"[{len(value)} items]"
            elif isinstance(value, str) and len(value) > 50:
                display_value = f"{value[:47]}..."
            else:
                display_value = str(value)
            logger.info(f"  {key}: {display_value}")
        return

    # Get data server client
    data_server_client = container.data_server_client

    if found_sensitive:
        logger.info(f"Config includes sensitive keys with values: {', '.join(found_sensitive)}")

    # Warn about empty credential keys so the user knows what's missing
    empty_sensitive = [k for k in sensitive_keys if k in config_data and not config_data.get(k)]
    if empty_sensitive:
        logger.info(f"Warning: {len(empty_sensitive)} credential key(s) are empty: {', '.join(empty_sensitive)}")
        logger.info("Set these in lean.json or via `lean config set <key> <value>` before pushing")

    # Push config
    logger.info(f"Pushing config '{name}' to data server...")
    result = data_server_client.push_config(
        config=config_data,
        name=name,
        project_id=project_id,
        description=description
    )

    logger.info(f"Successfully pushed config '{name}' (id: {result['id']})")
    logger.info(f"Total keys: {len(config_data)}")
