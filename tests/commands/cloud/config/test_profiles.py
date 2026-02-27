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

from click.testing import CliRunner

from lean.commands import lean
from lean.container import container
from tests.conftest import initialize_container


def test_cloud_config_set_creates_and_activates_profile() -> None:
    initialize_container()

    result = CliRunner().invoke(
        lean,
        [
            "cloud", "config", "set", "dev",
            "--url", "http://0.0.0.0:5067",
            "--bearer", "TEST",
            "--config-name", "dev"
        ]
    )

    assert result.exit_code == 0
    assert container.cli_config_manager.get_active_data_server_profile_name() == "dev"
    assert container.cli_config_manager.data_server_url.get_value() == "http://0.0.0.0:5067"
    assert container.cli_config_manager.data_server_api_key.get_value() == "TEST"
    assert container.cli_config_manager.get_active_cloud_config_name() == "dev"


def test_cloud_config_list_shows_active_marker() -> None:
    initialize_container()
    container.cli_config_manager.upsert_data_server_profile(
        "dev",
        data_server_url="http://0.0.0.0:5067",
        data_server_api_key="TEST",
        config_name="dev"
    )
    container.cli_config_manager.set_active_data_server_profile("dev")

    result = CliRunner().invoke(lean, ["cloud", "config", "list"])

    assert result.exit_code == 0
    assert "* dev: url=http://0.0.0.0:5067, cloud-config=dev" in result.output
