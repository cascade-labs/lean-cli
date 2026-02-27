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

from click import argument, command, option

from lean.click import LeanCommand
from lean.container import container


@command(cls=LeanCommand)
@argument("name", type=str)
@option("--url", type=str, default=None, help="Data server URL for this profile")
@option("--bearer", type=str, default=None, help="Data server Bearer API key for this profile")
@option("--config-name", type=str, default=None, help="Cloud config name to use for push/pull in this profile")
@option("--activate/--no-activate", default=True, help="Set this profile as active")
def set(name: str, url: str, bearer: str, config_name: str, activate: bool) -> None:
    """Create/update a data server config profile and optionally set it active."""
    logger = container.logger
    cli_config_manager = container.cli_config_manager

    existing = cli_config_manager.list_data_server_profiles().get(name)
    is_new = existing is None
    if is_new and (url is None or bearer is None):
        raise RuntimeError("New profiles require both --url and --bearer")

    profile = cli_config_manager.upsert_data_server_profile(
        name=name,
        data_server_url=url,
        data_server_api_key=bearer,
        config_name=config_name
    )

    if activate:
        cli_config_manager.set_active_data_server_profile(name)
        container.reset_data_server_clients()
        logger.info(f"Active profile: {name}")

    logger.info(
        f"Saved profile '{name}' (url={profile.get('data-server-url', '(unset)')}, "
        f"cloud-config={profile.get('config-name', 'default')})"
    )
