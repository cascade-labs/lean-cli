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

from typing import Optional

from click import command, option, prompt

from lean.click import LeanCommand
from lean.container import container


@command(cls=LeanCommand)
@option("--url", "-u", type=str, help="Data server URL")
@option("--api-key", "-k", type=str, help="API key for data server")
@option("--show-secrets", is_flag=True, show_default=True, default=False, help="Show secrets as they are input")
def login(url: Optional[str],
          api_key: Optional[str],
          show_secrets: bool) -> None:
    """Log in to the data server.

    If URL or API key is not provided an interactive prompt will show.

    Credentials are stored in ~/.lean/credentials and are removed upon running `lean logout`.

    After logging in, use `lean cloud config pull` to pull additional configuration from the server.
    """
    logger = container.logger
    cli_config_manager = container.cli_config_manager

    # Set default user-id if not already set
    # Note: user-id must be numeric as LEAN expects an integer for job-user-id
    current_user_id = cli_config_manager.user_id.get_value()
    if not current_user_id or not current_user_id.isdigit():
        cli_config_manager.user_id.set_value("0")

    # Set a placeholder API token if not already set (needed to avoid validation errors)
    current_api_token = cli_config_manager.api_token.get_value()
    if current_api_token is None:
        cli_config_manager.api_token.set_value("placeholder")

    current_url = cli_config_manager.data_server_url.get_value()
    current_api_key = cli_config_manager.data_server_api_key.get_value()

    if url is None:
        url = prompt("Data server URL", default=current_url or "http://0.0.0.0:5067")

    if api_key is None:
        api_key = logger.prompt_password("API key", current_api_key, hide_input=not show_secrets)

    cli_config_manager.data_server_url.set_value(url)
    cli_config_manager.data_server_api_key.set_value(api_key)

    logger.info(f"Successfully configured data server: {url}")
    logger.info("Run `lean cloud config pull` to pull additional configuration from the server.")
