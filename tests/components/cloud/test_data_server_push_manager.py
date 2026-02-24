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

from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from lean.components.cloud.data_server_push_manager import DataServerPushManager
from lean.models.errors import RequestFailedError


def _create_request_failed_error(status_code: int, text: str) -> RequestFailedError:
    response = mock.Mock()
    response.status_code = status_code
    response.text = text
    response.request = mock.Mock(method="PUT", url="https://example.com/api/v1/lean/projects/stale-id")
    return RequestFailedError(response)


def _create_manager(existing_data_server_id: str = "stale-id") -> tuple:
    logger = mock.Mock()
    data_server_client = mock.Mock()
    project_manager = mock.Mock()
    project_config_manager = mock.Mock()
    project_config = mock.Mock()

    project_config.get.side_effect = lambda key, default=None: {
        "data-server-id": existing_data_server_id,
        "description": "",
        "algorithm-language": "Python",
        "parameters": {}
    }.get(key, default)
    project_config_manager.get_project_config.return_value = project_config

    manager = DataServerPushManager(logger, data_server_client, project_manager, project_config_manager)
    return manager, data_server_client, project_manager, project_config


def test_push_project_creates_when_update_returns_500_and_project_cannot_be_retrieved() -> None:
    project_path = Path.cwd() / "My Project"
    project_path.mkdir(parents=True, exist_ok=True)
    source_file = project_path / "main.py"
    source_file.write_text("print('hello')", encoding="utf-8")

    manager, data_server_client, project_manager, project_config = _create_manager()
    project_manager.get_source_files.return_value = [source_file]

    data_server_client.update_project.side_effect = _create_request_failed_error(
        500, "No project with the given name or id"
    )
    data_server_client.get_project.side_effect = _create_request_failed_error(
        500, "No project with the given name or id"
    )
    created_project = mock.Mock(id="new-id")
    data_server_client.create_project.return_value = created_project

    manager.push_project(project_path)

    data_server_client.create_project.assert_called_once()
    project_config.set.assert_called_once_with("data-server-id", "new-id")


def test_push_project_re_raises_non_missing_update_failures() -> None:
    project_path = Path.cwd() / "My Project"
    project_path.mkdir(parents=True, exist_ok=True)
    source_file = project_path / "main.py"
    source_file.write_text("print('hello')", encoding="utf-8")

    manager, data_server_client, project_manager, _ = _create_manager()
    project_manager.get_source_files.return_value = [source_file]

    update_error = _create_request_failed_error(500, "Internal server error")
    data_server_client.update_project.side_effect = update_error
    data_server_client.get_project.return_value = mock.Mock(id="stale-id")

    with pytest.raises(RequestFailedError):
        manager._push_project(project_path)

    data_server_client.create_project.assert_not_called()


def test_push_project_recovers_when_create_returns_500_but_project_exists_by_name() -> None:
    project_path = Path.cwd() / "My Project"
    project_path.mkdir(parents=True, exist_ok=True)
    source_file = project_path / "main.py"
    source_file.write_text("print('hello')", encoding="utf-8")

    manager, data_server_client, project_manager, project_config = _create_manager(existing_data_server_id=None)
    project_manager.get_source_files.return_value = [source_file]

    data_server_client.create_project.side_effect = _create_request_failed_error(500, "Internal server error")
    data_server_client.get_project_by_name.return_value = mock.Mock(id="existing-id")
    data_server_client.update_project.return_value = mock.Mock(id="existing-id")

    manager.push_project(project_path)

    data_server_client.get_project_by_name.assert_called_once_with("My Project")
    data_server_client.update_project.assert_called_once()
    project_config.set.assert_called_once_with("data-server-id", "existing-id")


def test_push_project_recovers_when_by_name_fails_but_list_contains_project() -> None:
    project_path = Path.cwd() / "My Project"
    project_path.mkdir(parents=True, exist_ok=True)
    source_file = project_path / "main.py"
    source_file.write_text("print('hello')", encoding="utf-8")

    manager, data_server_client, project_manager, project_config = _create_manager(existing_data_server_id=None)
    project_manager.get_source_files.return_value = [source_file]

    data_server_client.create_project.side_effect = _create_request_failed_error(500, "Internal server error")
    data_server_client.get_project_by_name.side_effect = _create_request_failed_error(500, "Internal server error")
    data_server_client.list_projects.return_value = [SimpleNamespace(id="existing-id", name="My Project")]
    data_server_client.update_project.return_value = mock.Mock(id="existing-id")

    manager.push_project(project_path)

    data_server_client.list_projects.assert_called_once()
    data_server_client.update_project.assert_called_once()
    project_config.set.assert_called_once_with("data-server-id", "existing-id")


def test_push_project_raises_when_single_project_push_fails() -> None:
    project_path = Path.cwd() / "My Project"
    project_path.mkdir(parents=True, exist_ok=True)
    source_file = project_path / "main.py"
    source_file.write_text("print('hello')", encoding="utf-8")

    manager, data_server_client, project_manager, _ = _create_manager()
    project_manager.get_source_files.return_value = [source_file]
    data_server_client.update_project.side_effect = _create_request_failed_error(500, "Internal server error")
    data_server_client.get_project.return_value = mock.Mock(id="stale-id")

    with pytest.raises(RequestFailedError):
        manager.push_project(project_path)


def test_push_project_retries_create_on_transient_schema_cache_error() -> None:
    project_path = Path.cwd() / "My Project"
    project_path.mkdir(parents=True, exist_ok=True)
    source_file = project_path / "main.py"
    source_file.write_text("print('hello')", encoding="utf-8")

    manager, data_server_client, project_manager, project_config = _create_manager(existing_data_server_id=None)
    project_manager.get_source_files.return_value = [source_file]

    transient_error = _create_request_failed_error(
        500, "Failed to create project: {'message':'Could not query the database for the schema cache. Retrying.','code':'PGRST002'}"
    )
    data_server_client.create_project.side_effect = [transient_error, transient_error, mock.Mock(id="new-id")]

    manager.push_project(project_path)

    assert data_server_client.create_project.call_count == 3
    project_config.set.assert_called_once_with("data-server-id", "new-id")
