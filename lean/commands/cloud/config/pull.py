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
        help="Config name to pull (default: 'default')")
@option("--project-id", type=str, default=None,
        help="Project UUID (falls back to global if not found)")
@option("--overwrite", is_flag=True, default=False,
        help="Overwrite local lean.json with server config")
@option("--merge", is_flag=True, default=False,
        help="Merge server config into local lean.json (server wins conflicts)")
@option("--dry-run", is_flag=True, default=False,
        help="Show what would change without actually changing")
def pull(name: str, project_id: str, overwrite: bool, merge: bool, dry_run: bool) -> None:
    """Pull lean configuration from the data server to local lean.json.

    By default, this shows the diff without making changes.
    Use --overwrite to replace local config entirely.
    Use --merge to merge server config into local (server wins on conflicts).
    """
    logger = container.logger
    lean_config_manager = container.lean_config_manager

    if overwrite and merge:
        raise RuntimeError("Cannot use both --overwrite and --merge")

    # Get the local lean config
    lean_config_path = lean_config_manager.get_lean_config_path()
    logger.info(f"Local lean config: {lean_config_path}")

    with open(lean_config_path, "r") as f:
        local_config = json.load(f)

    # Pull config from server
    data_server_client = container.data_server_client
    logger.info(f"Pulling config '{name}' from data server...")

    result = data_server_client.pull_config(name=name, project_id=project_id)
    server_config = result.get("config", {})

    logger.info(f"Retrieved config '{name}' (id: {result['id']})")
    if result.get("project_id"):
        logger.info(f"Project-specific config (project_id: {result['project_id']})")
    else:
        logger.info("Global config")

    # Compute diff
    local_keys = set(local_config.keys())
    server_keys = set(server_config.keys())

    added_keys = server_keys - local_keys
    removed_keys = local_keys - server_keys
    common_keys = local_keys & server_keys

    modified_keys = []
    for key in common_keys:
        if local_config[key] != server_config[key]:
            modified_keys.append(key)

    # Sensitive keys for display masking
    sensitive_keys = [
        "job-user-id", "api-access-token",
        "thetadata-api-key", "thetadata-auth-token",
        "kalshi-api-key", "kalshi-private-key",
        "tradealert-s3-access-key", "tradealert-s3-secret-key",
        "polygon-api-key",
        "hyperliquid-aws-access-key-id", "hyperliquid-aws-secret-access-key",
        "alpaca-access-token", "ib-password", "oanda-access-token",
        "tradier-access-token", "tiingo-auth-token", "nasdaq-auth-token",
        "fxcm-password", "eze-password", "samco-client-password",
        "tastytrade-password", "zerodha-access-token",
        "trade-station-refresh-token", "tt-session-password",
        "us-energy-information-auth-token",
    ]

    def format_value(key, value):
        if key in sensitive_keys:
            return "***" if value else "(empty)"
        elif isinstance(value, dict):
            return "{...}"
        elif isinstance(value, list):
            return f"[{len(value)} items]"
        elif isinstance(value, str) and len(value) > 50:
            return f"{value[:47]}..."
        else:
            return str(value)

    # Show diff
    if added_keys or removed_keys or modified_keys:
        logger.info("\nChanges from server config:")

        if added_keys:
            logger.info(f"\n  Keys to add ({len(added_keys)}):")
            for key in sorted(added_keys):
                logger.info(f"    + {key}: {format_value(key, server_config[key])}")

        if modified_keys:
            logger.info(f"\n  Keys to modify ({len(modified_keys)}):")
            for key in sorted(modified_keys):
                logger.info(f"    ~ {key}:")
                logger.info(f"        local:  {format_value(key, local_config[key])}")
                logger.info(f"        server: {format_value(key, server_config[key])}")

        if removed_keys and overwrite:
            logger.info(f"\n  Keys to remove ({len(removed_keys)}) [overwrite only]:")
            for key in sorted(removed_keys):
                logger.info(f"    - {key}: {format_value(key, local_config[key])}")
    else:
        logger.info("\nNo changes - local config matches server config")
        return

    if dry_run:
        logger.info("\n[DRY RUN] No changes made")
        return

    if not overwrite and not merge:
        logger.info("\nUse --overwrite or --merge to apply changes")
        return

    # Apply changes
    if overwrite:
        new_config = server_config
        logger.info("\nOverwriting local config with server config...")
    else:  # merge
        new_config = local_config.copy()
        new_config.update(server_config)
        logger.info("\nMerging server config into local (server wins conflicts)...")

    # Write updated config
    with open(lean_config_path, "w") as f:
        json.dump(new_config, f, indent=4)

    logger.info(f"Updated {lean_config_path}")
    logger.info(f"Total keys: {len(new_config)}")
