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


def test_login_with_options() -> None:
    initialize_container()

    result = CliRunner().invoke(lean, ["login", "--url", "http://localhost:5067", "--api-key", "test-key"])

    assert result.exit_code == 0
    assert container.cli_config_manager.data_server_url.get_value() == "http://localhost:5067"
    assert container.cli_config_manager.data_server_api_key.get_value() == "test-key"


def test_login_prompts_for_url_when_not_given() -> None:
    initialize_container()

    result = CliRunner().invoke(lean, ["login", "--api-key", "test-key"], input="\n")

    assert result.exit_code == 0
    assert "Data server URL" in result.output
    assert container.cli_config_manager.data_server_url.get_value() == "http://0.0.0.0:5067"
    assert container.cli_config_manager.data_server_api_key.get_value() == "test-key"


def test_login_prompts_for_api_key_when_not_given() -> None:
    initialize_container()

    result = CliRunner().invoke(lean, ["login", "--url", "http://localhost:5067"], input="my-api-key\n")

    assert result.exit_code == 0
    assert "API key" in result.output
    assert container.cli_config_manager.data_server_url.get_value() == "http://localhost:5067"
    assert container.cli_config_manager.data_server_api_key.get_value() == "my-api-key"


def test_login_sets_default_user_id() -> None:
    initialize_container()

    result = CliRunner().invoke(lean, ["login", "--url", "http://localhost:5067", "--api-key", "test-key"])

    assert result.exit_code == 0
    assert container.cli_config_manager.user_id.get_value() == "0"


def test_login_sets_placeholder_api_token() -> None:
    initialize_container()

    result = CliRunner().invoke(lean, ["login", "--url", "http://localhost:5067", "--api-key", "test-key"])

    assert result.exit_code == 0
    assert container.cli_config_manager.api_token.get_value() == "placeholder"
